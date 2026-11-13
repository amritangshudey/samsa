package main

import (
	"bufio"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/amritangshudey/samsa/internal/broker"
	"github.com/amritangshudey/samsa/internal/scram"
)

// serverTLS is the TLS config of a listener: this node's certificate and,
// if clientCA is set, required client certificates signed by it.
func serverTLS(certFile, keyFile, clientCA string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load certificate: %w", err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if clientCA != "" {
		pool, err := loadPool(clientCA)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs, cfg.ClientAuth = pool, tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

// peerTLS returns the listener and dialer configs of mutual TLS between
// nodes: both sides show a certificate the cluster CA signed.
func peerTLS(certFile, keyFile, caFile string) (listen, dial *tls.Config, err error) {
	if listen, err = serverTLS(certFile, keyFile, caFile); err != nil {
		return nil, nil, err
	}
	pool, err := loadPool(caFile)
	if err != nil {
		return nil, nil, err
	}
	return listen, &tls.Config{Certificates: listen.Certificates, RootCAs: pool, MinVersion: tls.VersionTLS12}, nil
}

func loadPool(file string) (*x509.CertPool, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		return nil, fmt.Errorf("%s: no PEM certificate", file)
	}
	return pool, nil
}

// loadSASL reads a users file (written by `samsa user add`): one line per
// user and mechanism, "user mechanism iterations salt storedKey serverKey".
func loadSASL(mechanisms, file string) (*broker.SASL, error) {
	s := &broker.SASL{Users: map[string][]scram.Credential{}}
	for _, m := range strings.Split(mechanisms, ",") {
		m = strings.TrimSpace(m)
		if !slices.Contains(broker.SASLMechanisms, m) {
			return nil, fmt.Errorf("-sasl-mechanisms: unknown mechanism %q (known: %s)", m, strings.Join(broker.SASLMechanisms, ", "))
		}
		if !slices.Contains(s.Mechanisms, m) {
			s.Mechanisms = append(s.Mechanisms, m)
		}
	}
	slices.Sort(s.Mechanisms)
	if file == "" {
		return nil, errors.New("-sasl-users is required with -sasl-mechanisms")
	}
	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("-sasl-users: %w", err)
	}
	defer f.Close() //nolint:errcheck // read only
	sc := bufio.NewScanner(f)
	for line := 1; sc.Scan(); line++ {
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		user, rest, _ := strings.Cut(text, " ")
		c, err := scram.ParseCredential(rest)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: %w", file, line, err)
		}
		for _, old := range s.Users[user] {
			if old.Mechanism == c.Mechanism {
				return nil, fmt.Errorf("%s:%d: user %q has two %s credentials", file, line, user, c.Mechanism)
			}
		}
		s.Users[user] = append(s.Users[user], c)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("-sasl-users: %w", err)
	}
	if len(s.Users) == 0 {
		return nil, fmt.Errorf("%s: no users", file)
	}
	return s, nil
}

// runUser is `samsa user add -file F -name U`: it reads the password from
// stdin (one line) and writes the user's SCRAM credentials to F, replacing
// any the user had. The password itself is never written.
func runUser(args []string, stdin io.Reader, out io.Writer) (bool, error) {
	if len(args) == 0 || args[0] != "add" {
		return false, errors.New("usage: samsa user add -file FILE -name USER [-iterations N] < password")
	}
	fl := flag.NewFlagSet("user add", flag.ContinueOnError)
	file := fl.String("file", "", "users file to update (created if missing; mode 0600)")
	name := fl.String("name", "", "user name")
	iterations := fl.Int("iterations", scram.MinIterations, "PBKDF2 iterations")
	if err := fl.Parse(args[1:]); err != nil {
		return false, err
	}
	if *file == "" || *name == "" || strings.ContainsAny(*name, " \t\n") {
		return false, errors.New("-file and -name (without spaces) are required")
	}
	password, err := bufio.NewReader(stdin).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	password = strings.TrimRight(password, "\r\n")
	if password == "" {
		return false, errors.New("empty password on stdin")
	}
	var lines []string
	if b, err := os.ReadFile(*file); err == nil {
		for _, l := range strings.Split(strings.TrimRight(string(b), "\n"), "\n") {
			if user, _, _ := strings.Cut(strings.TrimSpace(l), " "); user != *name && l != "" {
				lines = append(lines, l)
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	for _, m := range []string{scram.SHA256, scram.SHA512} {
		salt := make([]byte, 16)
		if _, err := rand.Read(salt); err != nil {
			return false, err
		}
		c, err := scram.NewCredential(m, password, salt, *iterations)
		if err != nil {
			return false, err
		}
		lines = append(lines, *name+" "+c.String())
	}
	if err := os.WriteFile(*file, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		return false, err
	}
	fmt.Fprintf(out, "%s: credentials for %q (SCRAM-SHA-256, SCRAM-SHA-512; PLAIN checks against them)\n", *file, *name) //nolint:errcheck // nowhere to report it
	return true, nil
}

// loadAuthorization is the ACL configuration of -acl, -super-users and
// -allow-if-no-acl, or nil without -acl.
func loadAuthorization(on bool, superUsers string, allowNoACL bool) (*broker.Authorization, error) {
	if !on {
		if superUsers != "" || allowNoACL {
			return nil, errors.New("-super-users and -allow-if-no-acl need -acl")
		}
		return nil, nil
	}
	a := &broker.Authorization{AllowIfNoACL: allowNoACL}
	for _, p := range strings.Split(superUsers, ",") {
		if p = strings.TrimSpace(p); p == "" {
			continue
		}
		if !strings.Contains(p, ":") {
			return nil, fmt.Errorf("-super-users: %q is not a principal (User:name)", p)
		}
		a.SuperUsers = append(a.SuperUsers, p)
	}
	return a, nil
}
