package integration

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// pki is a throwaway CA and the certificates it signed, as PEM files.
type pki struct {
	dir    string
	ca     *x509.Certificate
	caKey  *ecdsa.PrivateKey
	caFile string
	pool   *x509.CertPool
}

func newPKI(t *testing.T, name string) *pki {
	t.Helper()
	p := &pki{dir: t.TempDir()}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	p.ca, _ = x509.ParseCertificate(der)
	p.caKey = key
	p.caFile = p.write(t, name+"-ca.pem", "CERTIFICATE", der)
	p.pool = x509.NewCertPool()
	p.pool.AddCert(p.ca)
	return p
}

func (p *pki) write(t *testing.T, name, kind string, der []byte) string {
	t.Helper()
	file := filepath.Join(p.dir, name)
	if err := os.WriteFile(file, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return file
}

// leaf issues a certificate for 127.0.0.1 and localhost, usable as server
// and as client; it returns the PEM files and the loaded pair.
func (p *pki) leaf(t *testing.T, name string) (certFile, keyFile string, pair tls.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, DNSNames: []string{"localhost"},
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.ca, &key.PublicKey, p.caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile = p.write(t, name+".pem", "CERTIFICATE", der), p.write(t, name+"-key.pem", "PRIVATE KEY", keyDER)
	pair, err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile, pair
}

// usersFile runs `samsa user add` for each user=password.
func usersFile(t *testing.T, users ...string) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "users")
	for _, u := range users {
		name, pw, _ := strings.Cut(u, "=")
		cmd := exec.Command(samsaBin(), "user", "add", "-file", file, "-name", name)
		cmd.Stdin = strings.NewReader(pw + "\n")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("samsa user add: %v\n%s", err, out)
		}
	}
	return file
}

func samsaBin() string {
	if bin := os.Getenv("SAMSA_BIN"); bin != "" {
		return bin
	}
	return "../bin/samsa"
}

// roundTrip produces one record and reads it back, as proof the session
// works end to end.
func roundTrip(t *testing.T, cl *kgo.Client, topic string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: []byte("secret payload")}).FirstErr(); err != nil {
		return err
	}
	cl.AddConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}})
	fs := cl.PollFetches(ctx)
	if err := fs.Err0(); err != nil {
		return err
	}
	if fs.NumRecords() == 0 {
		return errors.New("no record read back")
	}
	return nil
}

// TestTLSAndSASL: production plan item 7 with franz-go. Over TLS, SCRAM
// (both hashes) and PLAIN log in and work; a wrong password, an unknown
// user, plaintext to a TLS listener and a TLS client that does not trust
// the broker's CA are all refused; a node with SASL but no login refuses
// every request but ApiVersions.
func TestTLSAndSASL(t *testing.T) {
	p := newPKI(t, "test")
	cert, key, _ := p.leaf(t, "broker")
	users := usersFile(t, "alice=alice-secret", "bob=bob-secret")
	b := startBroker(t, "-tls-cert", cert, "-tls-key", key,
		"-sasl-mechanisms", "PLAIN,SCRAM-SHA-256,SCRAM-SHA-512", "-sasl-users", users)
	trust := &tls.Config{RootCAs: p.pool, MinVersion: tls.VersionTLS12}
	client := func(m sasl.Mechanism, extra ...kgo.Opt) *kgo.Client {
		opts := append([]kgo.Opt{kgo.DialTLSConfig(trust.Clone()), kgo.RequestRetries(0), kgo.RetryTimeout(3 * time.Second)}, extra...)
		if m != nil {
			opts = append(opts, kgo.SASL(m))
		}
		return franz(t, b, opts...)
	}
	createTopic(t, client(scram.Auth{User: "alice", Pass: "alice-secret"}.AsSha512Mechanism()), "secure", 1)

	for name, m := range map[string]sasl.Mechanism{
		"SCRAM-SHA-256": scram.Auth{User: "alice", Pass: "alice-secret"}.AsSha256Mechanism(),
		"SCRAM-SHA-512": scram.Auth{User: "bob", Pass: "bob-secret"}.AsSha512Mechanism(),
		"PLAIN":         plain.Auth{User: "alice", Pass: "alice-secret"}.AsMechanism(),
	} {
		t.Run("login "+name, func(t *testing.T) {
			if err := roundTrip(t, client(m), "secure"); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		})
	}
	for name, m := range map[string]sasl.Mechanism{
		"wrong password, SCRAM": scram.Auth{User: "alice", Pass: "nope"}.AsSha256Mechanism(),
		"unknown user, SCRAM":   scram.Auth{User: "mallory", Pass: "alice-secret"}.AsSha512Mechanism(),
		"wrong password, PLAIN": plain.Auth{User: "bob", Pass: "alice-secret"}.AsMechanism(),
	} {
		t.Run("refused: "+name, func(t *testing.T) {
			// One request, not a produce: a producer retries until its
			// deadline and reports that instead of the cause.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, client(m))
			if !errors.Is(err, kerr.SaslAuthenticationFailed) {
				t.Fatalf("want SASL_AUTHENTICATION_FAILED, got %v", err)
			}
		})
	}
	t.Run("refused: no SASL", func(t *testing.T) {
		if err := roundTrip(t, client(nil), "secure"); err == nil {
			t.Fatal("a client without SASL produced")
		}
	})
	t.Run("refused: plaintext", func(t *testing.T) {
		cl := franz(t, b, kgo.SASL(scram.Auth{User: "alice", Pass: "alice-secret"}.AsSha256Mechanism()), kgo.RequestRetries(0), kgo.RetryTimeout(3*time.Second))
		if err := roundTrip(t, cl, "secure"); err == nil {
			t.Fatal("a plaintext client produced to a TLS listener")
		}
	})
	t.Run("refused: untrusted broker", func(t *testing.T) {
		other := newPKI(t, "other")
		conn, err := tls.Dial("tcp", b.addr, &tls.Config{RootCAs: other.pool, MinVersion: tls.VersionTLS12})
		if err == nil {
			conn.Close()
			t.Fatal("a client that trusts another CA completed a handshake")
		}
	})
	if strings.Contains(b.log(), "alice-secret") || strings.Contains(b.log(), "bob-secret") {
		t.Fatal("a password appears in the broker log")
	}
}

// TestClientCertificates: with -tls-client-ca, a client without a
// certificate from that CA cannot connect.
func TestClientCertificates(t *testing.T) {
	p := newPKI(t, "test")
	cert, key, _ := p.leaf(t, "broker")
	_, _, clientPair := p.leaf(t, "client")
	b := startBroker(t, "-tls-cert", cert, "-tls-key", key, "-tls-client-ca", p.caFile)
	with := franz(t, b, kgo.DialTLSConfig(&tls.Config{RootCAs: p.pool, Certificates: []tls.Certificate{clientPair}, MinVersion: tls.VersionTLS12}))
	createTopic(t, with, "mtls", 1)
	if err := roundTrip(t, with, "mtls"); err != nil {
		t.Fatalf("client with a certificate: %v", err)
	}
	without := franz(t, b, kgo.DialTLSConfig(&tls.Config{RootCAs: p.pool, MinVersion: tls.VersionTLS12}), kgo.RequestRetries(0), kgo.RetryTimeout(3*time.Second))
	if err := roundTrip(t, without, "mtls"); err == nil {
		t.Fatal("a client without a certificate produced")
	}
}

// TestPeerTLS: a three-node cluster whose nodes talk mutual TLS elects a
// leader and replicates; a stranger with a certificate from another CA
// cannot complete a handshake on a peer port.
func TestPeerTLS(t *testing.T) {
	p := newPKI(t, "cluster")
	c := &cluster{t: t, nodes: map[int32]*broker{}}
	var peers, peerAddrs []string
	for id := int32(1); id <= 3; id++ {
		c.nodes[id] = &broker{t: t, addr: freeAddr(t), dir: t.TempDir()}
		peerAddr := freeAddr(t)
		peerAddrs = append(peerAddrs, peerAddr)
		peers = append(peers, fmt.Sprintf("%d=%s/%s", id, c.nodes[id].addr, peerAddr))
	}
	for id, b := range c.nodes {
		cert, key, _ := p.leaf(t, fmt.Sprintf("node-%d", id))
		b.args = []string{"-node-id", fmt.Sprint(id), "-peers", strings.Join(peers, ","),
			"-peer-tls-cert", cert, "-peer-tls-key", key, "-peer-tls-ca", p.caFile}
		b.start()
	}
	t.Cleanup(func() {
		for _, b := range c.nodes {
			b.kill()
		}
	})
	cl := c.client()
	createTopic(t, cl, "replicated", 3) // c.produce spreads over three partitions
	if err := c.produce(cl, "replicated", "r", 50, 30*time.Second); err != nil {
		t.Fatalf("produce over a TLS cluster: %v", err)
	}
	other := newPKI(t, "stranger")
	_, _, strangerPair := other.leaf(t, "stranger")
	conn, err := tls.Dial("tcp", peerAddrs[0], &tls.Config{RootCAs: p.pool, Certificates: []tls.Certificate{strangerPair}, MinVersion: tls.VersionTLS12})
	if err == nil {
		// TLS 1.3 reports a refused client certificate on the first read.
		conn.SetReadDeadline(time.Now().Add(2 * time.Second)) //nolint:errcheck // the read reports it
		_, err = conn.Read(make([]byte, 1))
		conn.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("a stranger's certificate was not refused on the peer port: %v", err)
	}
}

// TestTLSAndSASLOtherClients: the Java client (the Kafka 4.4 console tools)
// and kcat (librdkafka) produce and consume over TLS with SCRAM, and a
// wrong password is refused.
func TestTLSAndSASLOtherClients(t *testing.T) {
	p := newPKI(t, "test")
	cert, key, _ := p.leaf(t, "broker")
	users := usersFile(t, "alice=alice-secret")
	b := startBroker(t, "-tls-cert", cert, "-tls-key", key, "-sasl-mechanisms", "SCRAM-SHA-256,SCRAM-SHA-512", "-sasl-users", users)
	admin := franz(t, b, kgo.DialTLSConfig(&tls.Config{RootCAs: p.pool, MinVersion: tls.VersionTLS12}),
		kgo.SASL(scram.Auth{User: "alice", Pass: "alice-secret"}.AsSha256Mechanism()))
	createTopic(t, admin, "java", 1)
	createTopic(t, admin, "kcat", 1)

	t.Run("java", func(t *testing.T) {
		bin, javaHome := kafkaTools(t)
		props := func(password string) string {
			f := filepath.Join(t.TempDir(), "client.properties")
			text := "security.protocol=SASL_SSL\nsasl.mechanism=SCRAM-SHA-512\n" +
				`sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="alice" password="` + password + "\";\n" +
				"ssl.truststore.type=PEM\nssl.truststore.location=" + p.caFile + "\n"
			if err := os.WriteFile(f, []byte(text), 0o600); err != nil {
				t.Fatal(err)
			}
			return f
		}
		run := func(stdin string, tool string, args ...string) (string, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, filepath.Join(bin, tool), append(args, "--bootstrap-server", b.addr)...)
			cmd.Env = append(os.Environ(), "JAVA_HOME="+javaHome)
			cmd.Stdin = strings.NewReader(stdin)
			out, err := cmd.CombinedOutput()
			return string(out), err
		}
		good := props("alice-secret")
		if out, err := run("one\ntwo\nthree\n", "kafka-console-producer.sh", "--topic", "java", "--command-config", good); err != nil {
			t.Fatalf("console producer: %v\n%s", err, out)
		}
		out, err := run("", "kafka-console-consumer.sh", "--topic", "java", "--from-beginning", "--max-messages", "3", "--timeout-ms", "30000", "--command-config", good)
		if err != nil || !strings.Contains(out, "one\ntwo\nthree") {
			t.Fatalf("console consumer: %v\n%s", err, out)
		}
		out, _ = run("", "kafka-console-consumer.sh", "--topic", "java", "--from-beginning", "--max-messages", "1", "--timeout-ms", "15000", "--command-config", props("wrong"))
		if !strings.Contains(out, "SaslAuthenticationException") {
			t.Fatalf("a wrong password was not refused with a SASL error:\n%s", out)
		}
	})

	t.Run("kcat", func(t *testing.T) {
		kcat, err := exec.LookPath("kcat")
		if err != nil {
			missing(t, "kcat")
		}
		args := func(password string, more ...string) []string {
			return append([]string{"-b", b.addr, "-X", "security.protocol=SASL_SSL", "-X", "sasl.mechanisms=SCRAM-SHA-256",
				"-X", "sasl.username=alice", "-X", "sasl.password=" + password, "-X", "ssl.ca.location=" + p.caFile}, more...)
		}
		produce := exec.Command(kcat, args("alice-secret", "-P", "-t", "kcat", "-p", "0")...)
		produce.Stdin = strings.NewReader("a\nb\n")
		if out, err := produce.CombinedOutput(); err != nil {
			t.Fatalf("kcat produce: %v\n%s", err, out)
		}
		out, err := exec.Command(kcat, args("alice-secret", "-C", "-t", "kcat", "-p", "0", "-o", "beginning", "-e", "-c", "2")...).CombinedOutput()
		if err != nil || string(out) != "a\nb\n" {
			t.Fatalf("kcat consume: %v\n%q", err, out)
		}
		out, err = exec.Command(kcat, args("wrong", "-L", "-m", "5")...).CombinedOutput()
		if err == nil || !strings.Contains(strings.ToLower(string(out)), "authentication") {
			t.Fatalf("kcat with a wrong password: %v\n%s", err, out)
		}
	})
}
