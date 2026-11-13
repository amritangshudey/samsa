// Package scram is the server side of SCRAM-SHA-256 and SCRAM-SHA-512
// (RFC 5802, RFC 7677) as Kafka uses them, without channel binding, and the
// stored credentials it checks against (ADR 0019). It is pure: the caller
// supplies the server nonce and the key for unknown users.
package scram

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"strconv"
	"strings"
)

// The mechanisms, by their SASL names.
const (
	SHA256 = "SCRAM-SHA-256"
	SHA512 = "SCRAM-SHA-512"
)

// MinIterations and MaxIterations bound a credential's PBKDF2 iterations,
// as Kafka's ScramMechanism does (4096 to 16384).
const (
	MinIterations = 4096
	MaxIterations = 16384
)

// Credential is what the server stores for one user and mechanism: never
// the password, only what RFC 5802 section 3 derives from it.
type Credential struct {
	Mechanism            string
	Iterations           int
	Salt                 []byte
	StoredKey, ServerKey []byte
}

func hashFor(mechanism string) (func() hash.Hash, error) {
	switch mechanism {
	case SHA256:
		return sha256.New, nil
	case SHA512:
		return sha512.New, nil
	}
	return nil, fmt.Errorf("unknown SCRAM mechanism %q", mechanism)
}

func mac(h func() hash.Hash, key []byte, parts ...string) []byte {
	m := hmac.New(h, key)
	for _, p := range parts {
		m.Write([]byte(p))
	}
	return m.Sum(nil)
}

// NewCredential derives the stored credential for password.
func NewCredential(mechanism, password string, salt []byte, iterations int) (Credential, error) {
	h, err := hashFor(mechanism)
	if err != nil {
		return Credential{}, err
	}
	if iterations < MinIterations || iterations > MaxIterations {
		return Credential{}, fmt.Errorf("iterations %d outside [%d, %d]", iterations, MinIterations, MaxIterations)
	}
	salted, err := pbkdf2Key(h, password, salt, iterations)
	if err != nil {
		return Credential{}, err
	}
	clientKey := mac(h, salted, "Client Key")
	stored := h()
	stored.Write(clientKey)
	return Credential{Mechanism: mechanism, Iterations: iterations, Salt: salt,
		StoredKey: stored.Sum(nil), ServerKey: mac(h, salted, "Server Key")}, nil
}

func pbkdf2Key(h func() hash.Hash, password string, salt []byte, iterations int) ([]byte, error) {
	return pbkdf2.Key(h, password, salt, iterations, h().Size())
}

// CheckPassword reports whether password is the one c was derived from.
// It costs a full PBKDF2 derivation.
func (c Credential) CheckPassword(password string) bool {
	d, err := NewCredential(c.Mechanism, password, c.Salt, c.Iterations)
	return err == nil && subtle.ConstantTimeCompare(d.StoredKey, c.StoredKey) == 1
}

// String encodes c as one line of a credentials file:
// "mechanism iterations salt storedKey serverKey", the byte fields in
// standard base64.
func (c Credential) String() string {
	b := base64.StdEncoding.EncodeToString
	return fmt.Sprintf("%s %d %s %s %s", c.Mechanism, c.Iterations, b(c.Salt), b(c.StoredKey), b(c.ServerKey))
}

// ParseCredential reads what String writes.
func ParseCredential(s string) (Credential, error) {
	f := strings.Fields(s)
	if len(f) != 5 {
		return Credential{}, errors.New("want: mechanism iterations salt storedKey serverKey")
	}
	h, err := hashFor(f[0])
	if err != nil {
		return Credential{}, err
	}
	c := Credential{Mechanism: f[0]}
	if c.Iterations, err = strconv.Atoi(f[1]); err != nil || c.Iterations < MinIterations || c.Iterations > MaxIterations {
		return Credential{}, fmt.Errorf("iterations %q outside [%d, %d]", f[1], MinIterations, MaxIterations)
	}
	for i, dst := range []*[]byte{&c.Salt, &c.StoredKey, &c.ServerKey} {
		if *dst, err = base64.StdEncoding.DecodeString(f[2+i]); err != nil {
			return Credential{}, fmt.Errorf("field %d: %w", 3+i, err)
		}
	}
	if len(c.Salt) == 0 || len(c.StoredKey) != h().Size() || len(c.ServerKey) != h().Size() {
		return Credential{}, errors.New("salt empty or key of the wrong length")
	}
	return c, nil
}

// ErrAuth is every authentication failure. Its text names no user and no
// reason, so that a client learns nothing about which users exist.
var ErrAuth = errors.New("authentication failed: invalid credentials")

// Server is one SCRAM exchange: two client messages, two answers.
type Server struct {
	mechanism   string
	h           func() hash.Hash
	cred        Credential
	unknown     bool
	user        string
	gs2         string // the GS2 header, as sent
	clientBare  string
	serverFirst string
	nonce       string
}

// NewServer starts an exchange for mechanism.
func NewServer(mechanism string) (*Server, error) {
	h, err := hashFor(mechanism)
	if err != nil {
		return nil, err
	}
	return &Server{mechanism: mechanism, h: h}, nil
}

// First takes the client-first message. lookup finds the user's credential
// for this mechanism; for a user it does not know, a credential derived
// from unknownKey stands in, so the exchange fails only at the proof, as
// for a wrong password. serverNonce must be printable, without commas.
// It returns the server-first message.
func (s *Server) First(msg []byte, lookup func(user string) (Credential, bool), unknownKey []byte, serverNonce string) ([]byte, error) {
	m := string(msg)
	// gs2-header: "n," or "y," then an optional "a=authzid", then ",".
	flag, rest, ok := strings.Cut(m, ",")
	if !ok || (flag != "n" && flag != "y") { // "p=..." asks for channel binding, which Kafka does not do
		return nil, ErrAuth
	}
	authz, bare, ok := strings.Cut(rest, ",")
	if !ok || (authz != "" && !strings.HasPrefix(authz, "a=")) {
		return nil, ErrAuth
	}
	s.gs2, s.clientBare = m[:len(m)-len(bare)], bare
	attrs := strings.Split(bare, ",")
	if len(attrs) < 2 || !strings.HasPrefix(attrs[0], "n=") || !strings.HasPrefix(attrs[1], "r=") || len(attrs[1]) < 3 {
		return nil, ErrAuth // a leading "m=" (mandatory extension) is refused too
	}
	user, err := unescape(attrs[0][2:])
	if err != nil || user == "" {
		return nil, ErrAuth
	}
	if authz != "" { // Kafka: the authorization id, if any, must be the user
		if a, err := unescape(authz[2:]); err != nil || a != user {
			return nil, ErrAuth
		}
	}
	for _, c := range serverNonce {
		if c == ',' || c < 0x21 || c > 0x7e {
			return nil, errors.New("server nonce must be printable without commas")
		}
	}
	// Later attributes are extensions (Kafka's "tokenauth"); ignored, as Kafka does.
	s.user = user
	s.nonce = attrs[1][2:] + serverNonce
	cred, found := lookup(user)
	if !found || cred.Mechanism != s.mechanism {
		s.unknown = true
		cred = s.fake(user, unknownKey)
	}
	s.cred = cred
	s.serverFirst = fmt.Sprintf("r=%s,s=%s,i=%d", s.nonce, base64.StdEncoding.EncodeToString(cred.Salt), cred.Iterations)
	return []byte(s.serverFirst), nil
}

// fake is the credential of a user that does not exist: stable per user,
// so a client cannot tell from the salt either.
func (s *Server) fake(user string, key []byte) Credential {
	salt := mac(s.h, key, "salt", user)
	return Credential{Mechanism: s.mechanism, Iterations: MinIterations, Salt: salt[:16],
		StoredKey: mac(s.h, key, "stored", user), ServerKey: mac(s.h, key, "server", user)}
}

// Final takes the client-final message. On success it returns the
// server-final message and the authenticated user.
func (s *Server) Final(msg []byte) ([]byte, string, error) {
	m := string(msg)
	i := strings.LastIndex(m, ",p=")
	if s.serverFirst == "" || i < 0 {
		return nil, "", ErrAuth
	}
	withoutProof, proofText := m[:i], m[i+3:]
	attrs := strings.Split(withoutProof, ",")
	if len(attrs) < 2 || attrs[0] != "c="+base64.StdEncoding.EncodeToString([]byte(s.gs2)) || attrs[1] != "r="+s.nonce {
		return nil, "", ErrAuth
	}
	proof, err := base64.StdEncoding.DecodeString(proofText)
	if err != nil || len(proof) != s.h().Size() {
		return nil, "", ErrAuth
	}
	authMessage := s.clientBare + "," + s.serverFirst + "," + withoutProof
	signature := mac(s.h, s.cred.StoredKey, authMessage)
	clientKey := make([]byte, len(proof))
	for i := range proof {
		clientKey[i] = proof[i] ^ signature[i]
	}
	stored := s.h()
	stored.Write(clientKey)
	if subtle.ConstantTimeCompare(stored.Sum(nil), s.cred.StoredKey) != 1 || s.unknown {
		return nil, "", ErrAuth
	}
	serverSig := mac(s.h, s.cred.ServerKey, authMessage)
	return []byte("v=" + base64.StdEncoding.EncodeToString(serverSig)), s.user, nil
}

// unescape decodes a saslname: "=2C" is ",", "=3D" is "="; any other "="
// is invalid (RFC 5802, section 5.1).
func unescape(s string) (string, error) {
	if !strings.Contains(s, "=") {
		return s, nil
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '=' {
			b.WriteByte(s[i])
			continue
		}
		switch {
		case strings.HasPrefix(s[i:], "=2C"):
			b.WriteByte(',')
		case strings.HasPrefix(s[i:], "=3D"):
			b.WriteByte('=')
		default:
			return "", ErrAuth
		}
		i += 2
	}
	return b.String(), nil
}

// Client is the client side of one exchange. The broker never uses it;
// tests and tools that talk to a broker do.
type Client struct {
	mechanism, user, password, nonce string
	first, serverFirst               string
	salted                           []byte
}

// NewClient starts an exchange; nonce must be printable without commas.
func NewClient(mechanism, user, password, nonce string) *Client {
	return &Client{mechanism: mechanism, user: user, password: password, nonce: nonce}
}

// First returns the client-first message.
func (c *Client) First() []byte {
	esc := strings.NewReplacer("=", "=3D", ",", "=2C").Replace
	c.first = "n=" + esc(c.user) + ",r=" + c.nonce
	return []byte("n,," + c.first)
}

// Final takes the server-first message and returns the client-final one.
func (c *Client) Final(serverFirst []byte) ([]byte, error) {
	h, err := hashFor(c.mechanism)
	if err != nil {
		return nil, err
	}
	var nonce, saltText string
	iter := 0
	for _, a := range strings.Split(string(serverFirst), ",") {
		k, v, _ := strings.Cut(a, "=")
		switch k {
		case "r":
			nonce = v
		case "s":
			saltText = v
		case "i":
			iter, _ = strconv.Atoi(v)
		}
	}
	salt, err := base64.StdEncoding.DecodeString(saltText)
	if err != nil || !strings.HasPrefix(nonce, c.nonce) || iter < MinIterations || iter > MaxIterations {
		return nil, errors.New("bad server-first message")
	}
	if c.salted, err = pbkdf2Key(h, c.password, salt, iter); err != nil {
		return nil, err
	}
	c.serverFirst = string(serverFirst)
	without := "c=biws,r=" + nonce
	clientKey := mac(h, c.salted, "Client Key")
	stored := h()
	stored.Write(clientKey)
	sig := mac(h, stored.Sum(nil), c.first+","+c.serverFirst+","+without)
	for i := range sig {
		sig[i] ^= clientKey[i]
	}
	c.first += "," + c.serverFirst + "," + without // now the auth message
	return []byte(without + ",p=" + base64.StdEncoding.EncodeToString(sig)), nil
}

// Verify checks the server-final message: the server knew the credential.
func (c *Client) Verify(serverFinal []byte) error {
	h, _ := hashFor(c.mechanism)
	want := "v=" + base64.StdEncoding.EncodeToString(mac(h, mac(h, c.salted, "Server Key"), c.first))
	if subtle.ConstantTimeCompare([]byte(want), serverFinal) != 1 {
		return errors.New("server signature does not match")
	}
	return nil
}
