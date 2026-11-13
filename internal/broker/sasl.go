package broker

import (
	"bytes"
	"encoding/base64"
	"slices"

	"github.com/amritangshudey/samsa/internal/scram"
	"github.com/amritangshudey/samsa/internal/wire"
)

// SASL makes every client connection authenticate before any request but
// ApiVersions (ADR 0019).
type SASL struct {
	// Mechanisms are the enabled mechanisms, from SASLMechanisms.
	Mechanisms []string
	// Users maps each user to its SCRAM credentials. PLAIN checks the
	// password against them too, so no password is ever stored.
	Users map[string][]scram.Credential
}

// MechanismPlain is SASL PLAIN (RFC 4616). Use it only over TLS: the
// password crosses the connection as it is.
const MechanismPlain = "PLAIN"

// SASLMechanisms is every mechanism the broker implements, sorted.
var SASLMechanisms = []string{MechanismPlain, scram.SHA256, scram.SHA512}

// authState is one connection's progress through authentication. R5: it
// lives and dies with the connection.
type authState struct {
	mechanism string        // chosen by SaslHandshake; "" before
	scram     *scram.Server // a SCRAM exchange after its first message
	user      string        // set once authenticated
}

// authenticated reports whether c may send any request.
func (n *Node) authenticated(c *conn) bool { return n.cfg.SASL == nil || c.auth.user != "" }

// failAuth answers, then closes the connection: Kafka sends the error and
// then drops a connection whose authentication failed. The SASL handlers
// run inside pump, so nothing else from c is read in between.
func (n *Node) failAuth(c *conn, h wire.RequestHeader, resp wire.Message, why string) {
	n.log.Warn("authentication failed", "conn", uint64(c.id), "mechanism", c.auth.mechanism, "reason", why)
	n.respond(c, h, resp)
	if !c.closed {
		c.closed = true
		n.net.Close(c.id)
	}
}

func (n *Node) saslHandshake(c *conn, h wire.RequestHeader, req *wire.SaslHandshakeRequest) {
	if n.cfg.SASL == nil {
		n.failAuth(c, h, &wire.SaslHandshakeResponse{ErrorCode: wire.ErrUnsupportedSaslMechanism}, "SASL is not enabled")
		return
	}
	mechs := n.cfg.SASL.Mechanisms
	switch {
	case c.auth.mechanism != "" || c.auth.user != "":
		// A second handshake would be re-authentication (KIP-368), which a
		// session lifetime of 0 tells clients is never needed.
		n.failAuth(c, h, &wire.SaslHandshakeResponse{ErrorCode: wire.ErrIllegalSaslState, Mechanisms: mechs}, "second handshake")
	case !slices.Contains(mechs, req.Mechanism):
		n.failAuth(c, h, &wire.SaslHandshakeResponse{ErrorCode: wire.ErrUnsupportedSaslMechanism, Mechanisms: mechs}, "unsupported mechanism "+req.Mechanism)
	default:
		c.auth.mechanism = req.Mechanism
		n.respond(c, h, &wire.SaslHandshakeResponse{Mechanisms: mechs})
	}
}

func (n *Node) saslAuthenticate(c *conn, h wire.RequestHeader, req *wire.SaslAuthenticateRequest) {
	mech := c.auth.mechanism
	if mech == "" || c.auth.user != "" {
		n.failAuth(c, h, &wire.SaslAuthenticateResponse{ErrorCode: wire.ErrIllegalSaslState,
			ErrorMessage: "SaslAuthenticate is valid only after SaslHandshake, once"}, "authenticate out of order")
		return
	}
	// Kafka's text: it names neither the user nor what was wrong.
	failed := &wire.SaslAuthenticateResponse{ErrorCode: wire.ErrSaslAuthenticationFailed,
		ErrorMessage: "Authentication failed during authentication due to invalid credentials with SASL mechanism " + mech}
	if mech == MechanismPlain {
		user, ok := n.checkPlain(req.AuthBytes)
		if !ok {
			n.failAuth(c, h, failed, "wrong PLAIN credentials")
			return
		}
		n.authenticatedAs(c, user)
		n.respond(c, h, &wire.SaslAuthenticateResponse{})
		return
	}
	if c.auth.scram == nil {
		s, err := scram.NewServer(mech)
		if err != nil { // unreachable: the handshake accepted only known mechanisms
			n.failAuth(c, h, failed, err.Error())
			return
		}
		first, err := s.First(req.AuthBytes, n.lookupCredential(mech), n.unknownKey, n.nonce())
		if err != nil {
			n.failAuth(c, h, failed, "malformed SCRAM client-first message")
			return
		}
		c.auth.scram = s
		n.respond(c, h, &wire.SaslAuthenticateResponse{AuthBytes: first})
		return
	}
	final, user, err := c.auth.scram.Final(req.AuthBytes)
	c.auth.scram = nil
	if err != nil {
		n.failAuth(c, h, failed, "wrong SCRAM proof")
		return
	}
	n.authenticatedAs(c, user)
	n.respond(c, h, &wire.SaslAuthenticateResponse{AuthBytes: final})
}

func (n *Node) authenticatedAs(c *conn, user string) {
	c.auth.user = user
	n.log.Debug("authenticated", "conn", uint64(c.id), "user", user, "mechanism", c.auth.mechanism)
}

func (n *Node) lookupCredential(mech string) func(string) (scram.Credential, bool) {
	return func(user string) (scram.Credential, bool) {
		for _, c := range n.cfg.SASL.Users[user] {
			if c.Mechanism == mech {
				return c, true
			}
		}
		return scram.Credential{}, false
	}
}

// checkPlain verifies a PLAIN message: [authzid] NUL authcid NUL password
// (RFC 4616). As in Kafka, an authorization id must be the user's own. An
// unknown user costs the same derivation as a known one.
func (n *Node) checkPlain(msg []byte) (string, bool) {
	parts := bytes.Split(msg, []byte{0})
	if len(parts) != 3 || len(parts[1]) == 0 || (len(parts[0]) > 0 && !bytes.Equal(parts[0], parts[1])) {
		return "", false
	}
	user, password := string(parts[1]), string(parts[2])
	creds := n.cfg.SASL.Users[user]
	if len(creds) == 0 {
		scram.NewCredential(scram.SHA256, password, n.unknownKey, scram.MinIterations) //nolint:errcheck // only the cost matters: a real check costs one derivation too
		return "", false
	}
	for _, c := range creds {
		if c.CheckPassword(password) {
			return user, true
		}
	}
	return "", false
}

// nonce returns a fresh server nonce: 18 random bytes, base64url, which
// has no comma.
func (n *Node) nonce() string {
	b := make([]byte, 0, 24)
	for range 3 {
		v := n.rand.Uint64()
		for range 6 {
			b = append(b, byte(v))
			v >>= 8
		}
	}
	return base64.RawURLEncoding.EncodeToString(b)
}
