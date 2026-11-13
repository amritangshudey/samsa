package broker

import (
	"slices"
	"testing"

	"github.com/amritangshudey/samsa/internal/scram"
	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

func saslHarness(t *testing.T, mechs ...string) *harness {
	t.Helper()
	var creds []scram.Credential
	for _, m := range []string{scram.SHA256, scram.SHA512} {
		c, err := scram.NewCredential(m, "alice-secret", []byte("salt-"+m), 4096)
		if err != nil {
			t.Fatal(err)
		}
		creds = append(creds, c)
	}
	return newHarness(t, Config{SASL: &SASL{Mechanisms: mechs, Users: map[string][]scram.Credential{"alice": creds}}})
}

func (h *harness) handshake(c sys.ConnID, mech string) wire.SaslHandshakeResponse {
	h.t.Helper()
	var resp wire.SaslHandshakeResponse
	h.call(c, wire.KeySaslHandshake, 1, &wire.SaslHandshakeRequest{Mechanism: mech}, &resp)
	return resp
}

func (h *harness) authenticate(c sys.ConnID, version int16, b []byte) wire.SaslAuthenticateResponse {
	h.t.Helper()
	var resp wire.SaslAuthenticateResponse
	h.call(c, wire.KeySaslAuthenticate, version, &wire.SaslAuthenticateRequest{AuthBytes: b}, &resp)
	return resp
}

func (h *harness) wasClosed(c sys.ConnID) bool {
	h.t.Helper()
	return h.node.conns[c] == nil
}

// scramLogin runs a whole SCRAM exchange on c and reports whether it got in.
func (h *harness) scramLogin(c sys.ConnID, mech, user, password string) bool {
	h.t.Helper()
	if r := h.handshake(c, mech); r.ErrorCode != wire.ErrNone {
		h.t.Fatalf("handshake %s: %d", mech, r.ErrorCode)
	}
	cl := scram.NewClient(mech, user, password, "clientnonce")
	r := h.authenticate(c, 2, cl.First())
	if r.ErrorCode != wire.ErrNone {
		return false
	}
	final, err := cl.Final(r.AuthBytes)
	if err != nil {
		h.t.Fatal(err)
	}
	r = h.authenticate(c, 2, final)
	if r.ErrorCode != wire.ErrNone {
		if r.ErrorCode != wire.ErrSaslAuthenticationFailed || !h.wasClosed(c) {
			h.t.Fatalf("failed login: code %d, closed %v", r.ErrorCode, h.wasClosed(c))
		}
		return false
	}
	if err := cl.Verify(r.AuthBytes); err != nil {
		h.t.Fatal(err)
	}
	return true
}

func TestSASLScram(t *testing.T) {
	for _, mech := range []string{scram.SHA256, scram.SHA512} {
		t.Run(mech, func(t *testing.T) {
			h := saslHarness(t, scram.SHA256, scram.SHA512)
			h.open(1)
			if !h.scramLogin(1, mech, "alice", "alice-secret") {
				t.Fatal("the right password was refused")
			}
			if code := h.createTopic(1, "t", 1); code != wire.ErrNone {
				t.Fatalf("after login: create topic %d", code)
			}
			h.open(2)
			if h.scramLogin(2, mech, "alice", "wrong") {
				t.Fatal("a wrong password got in")
			}
			h.open(3)
			if h.scramLogin(3, mech, "mallory", "alice-secret") {
				t.Fatal("an unknown user got in")
			}
		})
	}
}

func TestSASLPlain(t *testing.T) {
	h := saslHarness(t, MechanismPlain)
	for i, tc := range []struct {
		msg string
		ok  bool
	}{
		{"\x00alice\x00alice-secret", true},
		{"alice\x00alice\x00alice-secret", true},
		{"bob\x00alice\x00alice-secret", false}, // authorization id of someone else
		{"\x00alice\x00wrong", false},
		{"\x00mallory\x00alice-secret", false},
		{"\x00alice", false},
		{"", false},
	} {
		c := sys.ConnID(10 + i)
		h.open(c)
		if r := h.handshake(c, MechanismPlain); r.ErrorCode != wire.ErrNone || !slices.Equal(r.Mechanisms, []string{MechanismPlain}) {
			t.Fatalf("handshake: %+v", r)
		}
		r := h.authenticate(c, 1, []byte(tc.msg))
		if ok := r.ErrorCode == wire.ErrNone; ok != tc.ok || h.wasClosed(c) == tc.ok {
			t.Errorf("%q: code %d (%s), closed %v; want ok %v", tc.msg, r.ErrorCode, r.ErrorMessage, h.wasClosed(c), tc.ok)
		}
		if !tc.ok && r.ErrorMessage != "Authentication failed during authentication due to invalid credentials with SASL mechanism PLAIN" {
			t.Errorf("%q: message %q names more than it should", tc.msg, r.ErrorMessage)
		}
	}
}

// Before authentication only ApiVersions and the SASL requests are
// answered; anything else closes the connection unanswered. Every protocol
// error answers, then closes.
func TestSASLRefusals(t *testing.T) {
	h := saslHarness(t, scram.SHA256)
	h.open(1)
	var av wire.APIVersionsResponse
	h.call(1, wire.KeyAPIVersions, 3, &wire.APIVersionsRequest{}, &av)
	if av.ErrorCode != 0 || h.wasClosed(1) {
		t.Fatalf("ApiVersions before login: %d", av.ErrorCode)
	}
	h.send(1, wire.KeyMetadata, 8, &wire.MetadataRequest{})
	h.run()
	if !h.wasClosed(1) || len(h.net.sent[1]) != 0 {
		t.Fatal("Metadata before login was answered or left the connection open")
	}

	for i, tc := range []struct {
		name string
		do   func(c sys.ConnID) int16
		want int16
	}{
		{"unknown mechanism", func(c sys.ConnID) int16 { return h.handshake(c, "GSSAPI").ErrorCode }, wire.ErrUnsupportedSaslMechanism},
		{"mechanism not enabled", func(c sys.ConnID) int16 { return h.handshake(c, MechanismPlain).ErrorCode }, wire.ErrUnsupportedSaslMechanism},
		{"authenticate first", func(c sys.ConnID) int16 { return h.authenticate(c, 0, []byte("x")).ErrorCode }, wire.ErrIllegalSaslState},
		{"second handshake", func(c sys.ConnID) int16 {
			h.handshake(c, scram.SHA256)
			return h.handshake(c, scram.SHA256).ErrorCode
		}, wire.ErrIllegalSaslState},
		{"garbage client-first", func(c sys.ConnID) int16 {
			h.handshake(c, scram.SHA256)
			return h.authenticate(c, 0, []byte("p=tls-unique,,n=alice,r=x")).ErrorCode
		}, wire.ErrSaslAuthenticationFailed},
		{"re-authentication", func(c sys.ConnID) int16 {
			h.scramLogin(c, scram.SHA256, "alice", "alice-secret")
			return h.handshake(c, scram.SHA256).ErrorCode
		}, wire.ErrIllegalSaslState},
	} {
		c := sys.ConnID(20 + i)
		h.open(c)
		if got := tc.do(c); got != tc.want || !h.wasClosed(c) {
			t.Errorf("%s: code %d, closed %v; want %d and closed", tc.name, got, h.wasClosed(c), tc.want)
		}
	}
}

// Without SASL, a client that tries it is told no mechanism is enabled;
// everything else works as before, and the node draws no randomness for it.
func TestSASLOff(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	if r := h.handshake(1, scram.SHA256); r.ErrorCode != wire.ErrUnsupportedSaslMechanism || len(r.Mechanisms) != 0 || !h.wasClosed(1) {
		t.Fatalf("handshake without SASL: %+v", r)
	}
	if h.node.unknownKey != nil {
		t.Fatal("a node without SASL drew a key")
	}
	h.open(2)
	if code := h.createTopic(2, "t", 1); code != wire.ErrNone {
		t.Fatalf("create topic: %d", code)
	}
}
