package scram

import (
	"encoding/base64"
	"errors"
	"testing"
)

// The example exchange of RFC 7677, section 3: user "user", password
// "pencil". A server that reproduces its server messages from its client
// messages computes SCRAM-SHA-256 correctly.
func TestRFC7677Example(t *testing.T) {
	salt, _ := base64.StdEncoding.DecodeString("W22ZaJ0SNY7soEsUEjb6gQ==")
	cred, err := NewCredential(SHA256, "pencil", salt, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if !cred.CheckPassword("pencil") || cred.CheckPassword("pencil2") {
		t.Fatal("CheckPassword")
	}
	s, _ := NewServer(SHA256)
	first, err := s.First([]byte("n,,n=user,r=rOprNGfwEbeRWgbNEkqO"), func(u string) (Credential, bool) { return cred, u == "user" }, []byte("k"), "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0")
	if want := "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"; err != nil || string(first) != want {
		t.Fatalf("server-first %q, %v\nwant %q", first, err, want)
	}
	final, user, err := s.Final([]byte("c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="))
	if want := "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="; err != nil || string(final) != want || user != "user" {
		t.Fatalf("server-final %q, %q, %v\nwant %q", final, user, err, want)
	}
}

// client plays the client side, to test the server against itself for
// SHA-512 and for the failure cases.
func client(t *testing.T, mech, user, password string, s *Server, lookup func(string) (Credential, bool)) (string, error) {
	t.Helper()
	gs2 := "n,,"
	first, err := s.First([]byte(gs2+"n="+user+",r=clientnonce"), lookup, []byte("unknown-key"), "servernonce")
	if err != nil {
		return "", err
	}
	// Parse r, s, i from the server-first message.
	var nonce, saltText string
	var iter int
	for _, a := range splitComma(string(first)) {
		switch a[:2] {
		case "r=":
			nonce = a[2:]
		case "s=":
			saltText = a[2:]
		case "i=":
			for _, c := range a[2:] {
				iter = iter*10 + int(c-'0')
			}
		}
	}
	salt, _ := base64.StdEncoding.DecodeString(saltText)
	h, _ := hashFor(mech)
	derived, err := NewCredential(mech, password, salt, iter)
	if err != nil {
		t.Fatal(err)
	}
	salted := derivedSalted(t, mech, password, salt, iter)
	clientKey := mac(h, salted, "Client Key")
	without := "c=" + base64.StdEncoding.EncodeToString([]byte(gs2)) + ",r=" + nonce
	sig := mac(h, derived.StoredKey, "n="+user+",r=clientnonce,"+string(first)+","+without)
	proof := make([]byte, len(sig))
	for i := range sig {
		proof[i] = clientKey[i] ^ sig[i]
	}
	_, got, err := s.Final([]byte(without + ",p=" + base64.StdEncoding.EncodeToString(proof)))
	return got, err
}

func derivedSalted(t *testing.T, mech, password string, salt []byte, iter int) []byte {
	t.Helper()
	h, _ := hashFor(mech)
	k, err := pbkdf2Key(h, password, salt, iter)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func splitComma(s string) []string {
	var out []string
	for len(s) > 0 {
		i := 0
		for i < len(s) && s[i] != ',' {
			i++
		}
		out = append(out, s[:i])
		if i == len(s) {
			break
		}
		s = s[i+1:]
	}
	return out
}

func TestExchange(t *testing.T) {
	for _, mech := range []string{SHA256, SHA512} {
		cred, err := NewCredential(mech, "secret", []byte("0123456789abcdef"), 4096)
		if err != nil {
			t.Fatal(err)
		}
		lookup := func(u string) (Credential, bool) { return cred, u == "alice" }
		s, _ := NewServer(mech)
		if user, err := client(t, mech, "alice", "secret", s, lookup); err != nil || user != "alice" {
			t.Errorf("%s good password: %q, %v", mech, user, err)
		}
		s, _ = NewServer(mech)
		if _, err := client(t, mech, "alice", "wrong", s, lookup); !errors.Is(err, ErrAuth) {
			t.Errorf("%s wrong password: %v", mech, err)
		}
		// An unknown user gets a full exchange, and the same failure.
		s, _ = NewServer(mech)
		if _, err := client(t, mech, "mallory", "secret", s, lookup); !errors.Is(err, ErrAuth) {
			t.Errorf("%s unknown user: %v", mech, err)
		}
	}
}

func TestFirstRefuses(t *testing.T) {
	lookup := func(string) (Credential, bool) { return Credential{}, false }
	for _, msg := range []string{
		"", "n", "p=tls-unique,,n=u,r=x", "n,,", "n,,r=x,n=u", "n,,n=u", "n,,n=,r=x",
		"n,,m=ext,n=u,r=x", "n,a=bob,n=alice,r=x", "n,,n=a=2Xb,r=x", "x,,n=u,r=x",
	} {
		s, _ := NewServer(SHA256)
		if _, err := s.First([]byte(msg), lookup, []byte("k"), "sn"); err == nil {
			t.Errorf("First(%q) accepted", msg)
		}
	}
	s, _ := NewServer(SHA256)
	if _, _, err := s.Final([]byte("c=biws,r=x,p=AA==")); err == nil {
		t.Error("Final before First accepted")
	}
	if _, err := NewServer("SCRAM-MD5"); err == nil {
		t.Error("unknown mechanism accepted")
	}
}

func TestCredentialRoundTrip(t *testing.T) {
	c, _ := NewCredential(SHA512, "pw", []byte("salt-salt-salt-s"), 8192)
	got, err := ParseCredential(c.String())
	if err != nil || got.String() != c.String() || !got.CheckPassword("pw") {
		t.Fatalf("round trip: %v, %v", got, err)
	}
	for _, bad := range []string{"", "SCRAM-SHA-256 4096 a b", "SCRAM-MD5 4096 AA== AA== AA==", "SCRAM-SHA-256 100 AA== AA== AA==", "SCRAM-SHA-256 4096 AA== AA== AA=="} {
		if _, err := ParseCredential(bad); err == nil {
			t.Errorf("ParseCredential(%q) accepted", bad)
		}
	}
}

func FuzzServer(f *testing.F) {
	f.Add([]byte("n,,n=user,r=abc"), []byte("c=biws,r=abcsn,p=AAAA"))
	cred, _ := NewCredential(SHA256, "pw", []byte("0123456789abcdef"), 4096)
	f.Fuzz(func(t *testing.T, first, final []byte) {
		s, _ := NewServer(SHA256)
		if _, err := s.First(first, func(string) (Credential, bool) { return cred, true }, []byte("k"), "sn"); err != nil {
			return
		}
		if _, _, err := s.Final(final); err == nil {
			t.Fatalf("a fuzzed proof authenticated: %q %q", first, final)
		}
	})
}

func TestClientAgainstServer(t *testing.T) {
	for _, mech := range []string{SHA256, SHA512} {
		cred, _ := NewCredential(mech, "p,w=", []byte("0123456789abcdef"), 4096)
		lookup := func(u string) (Credential, bool) { return cred, u == "a,b=c" }
		s, _ := NewServer(mech)
		c := NewClient(mech, "a,b=c", "p,w=", "cn")
		first, err := s.First(c.First(), lookup, []byte("k"), "sn")
		if err != nil {
			t.Fatal(err)
		}
		final, err := c.Final(first)
		if err != nil {
			t.Fatal(err)
		}
		last, user, err := s.Final(final)
		if err != nil || user != "a,b=c" || c.Verify(last) != nil {
			t.Fatalf("%s: user %q, %v, verify %v", mech, user, err, c.Verify(last))
		}
	}
}

// A credential from a salted password, as AlterUserScramCredentials sends
// it, is the one the password gives, and logs in.
func TestFromSalted(t *testing.T) {
	for _, m := range []string{SHA256, SHA512} {
		want, err := NewCredential(m, "pw", []byte("salt"), 4096)
		if err != nil {
			t.Fatal(err)
		}
		h, _ := hashFor(m)
		salted, _ := pbkdf2Key(h, "pw", []byte("salt"), 4096)
		got, err := FromSalted(m, []byte("salt"), salted, 4096)
		if err != nil || got.String() != want.String() || !got.CheckPassword("pw") {
			t.Fatalf("%s: %v, %v", m, got, err)
		}
		if _, err := FromSalted(m, []byte("salt"), salted[:5], 4096); err == nil {
			t.Fatalf("%s: a short salted password accepted", m)
		}
	}
}
