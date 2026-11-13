package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUsersFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "users")
	for _, u := range []struct{ name, pw string }{{"alice", "a-pw"}, {"bob", "b-pw"}, {"alice", "a-new"}} {
		if ok, err := runUser([]string{"add", "-file", file, "-name", u.name}, strings.NewReader(u.pw+"\n"), io.Discard); !ok || err != nil {
			t.Fatalf("user add %s: %v", u.name, err)
		}
	}
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "a-pw") || strings.Contains(string(b), "a-new") {
		t.Fatal("a password was written to the users file")
	}
	if info, _ := os.Stat(file); info.Mode().Perm() != 0o600 {
		t.Fatalf("users file mode %v, want 0600", info.Mode().Perm())
	}
	s, err := loadSASL("SCRAM-SHA-512,PLAIN", file)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(s.Mechanisms, ",") != "PLAIN,SCRAM-SHA-512" || len(s.Users) != 2 || len(s.Users["alice"]) != 2 {
		t.Fatalf("loaded %+v", s)
	}
	if !s.Users["alice"][0].CheckPassword("a-new") || s.Users["alice"][0].CheckPassword("a-pw") {
		t.Fatal("alice's replaced password is not the one that checks")
	}
	for _, tc := range []struct{ mechs, content, want string }{
		{"GSSAPI", "x", "unknown mechanism"},
		{"PLAIN", "", "no users"},
		{"PLAIN", "bob SCRAM-SHA-256 4096 AA==", "want: mechanism"},
	} {
		f := filepath.Join(t.TempDir(), "u")
		os.WriteFile(f, []byte(tc.content), 0o600) //nolint:errcheck // the load reports it
		if _, err := loadSASL(tc.mechs, f); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("loadSASL(%q, %q) = %v, want %q", tc.mechs, tc.content, err, tc.want)
		}
	}
	if ok, err := runUser([]string{"add", "-file", file, "-name", "carol"}, strings.NewReader(""), io.Discard); ok || err == nil {
		t.Fatal("an empty password was accepted")
	}
}

func TestLoadAuthorization(t *testing.T) {
	if a, err := loadAuthorization(false, "", false); a != nil || err != nil {
		t.Fatalf("off: %+v, %v", a, err)
	}
	if _, err := loadAuthorization(false, "User:admin", false); err == nil {
		t.Fatal("-super-users without -acl accepted")
	}
	if _, err := loadAuthorization(true, "admin", false); err == nil {
		t.Fatal("a super user without a type accepted")
	}
	a, err := loadAuthorization(true, "User:admin, User:ops", true)
	if err != nil || len(a.SuperUsers) != 2 || a.SuperUsers[1] != "User:ops" || !a.AllowIfNoACL {
		t.Fatalf("%+v, %v", a, err)
	}
}
