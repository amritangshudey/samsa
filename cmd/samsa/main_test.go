package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
)

func TestParsePeers(t *testing.T) {
	peers, addrs, err := parsePeers("1=a:9092/a:9192,2=b:9093/b:9193")
	if err != nil || len(peers) != 2 || peers[1].Host != "b" || peers[1].Port != 9093 || addrs[1] != "a:9192" || addrs[2] != "b:9193" {
		t.Fatalf("got %v %v %v", peers, addrs, err)
	}
	for _, bad := range []string{"", "1", "1=a:1", "x=a:1/a:2", "-1=a:1/a:2", "1=a/a:2", "1=a:1/a", "1=a:x/a:2", "1=a:1/a:2,1=b:1/b:2"} {
		if _, _, err := parsePeers(bad); err == nil {
			t.Errorf("%q was accepted", bad)
		}
	}
}

func TestLoadConfig(t *testing.T) {
	write := func(text string) string {
		p := filepath.Join(t.TempDir(), "c")
		if err := os.WriteFile(p, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	parse := func(text string, args ...string) (*flag.FlagSet, error) {
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		fs.String("sync", "always", "")
		fs.Int("node-id", 1, "")
		fs.String("config", "", "")
		if err := fs.Parse(args); err != nil {
			t.Fatal(err)
		}
		return fs, loadConfig(fs, write(text))
	}
	fs, err := parse("# comment\n\n sync = never \nnode-id=3\n", "-node-id", "2")
	if err != nil || fs.Lookup("sync").Value.String() != "never" || fs.Lookup("node-id").Value.String() != "2" {
		t.Fatalf("got sync=%v node-id=%v err=%v; the command line must win", fs.Lookup("sync").Value, fs.Lookup("node-id").Value, err)
	}
	for _, bad := range []string{"snyc = never", "sync never", "node-id = x", "sync = a\nsync = b", "config = other"} {
		if _, err := parse(bad); err == nil {
			t.Errorf("%q was accepted", bad)
		}
	}
}
