package main

import (
	"bytes"
	"flag"
	"strings"
	"testing"
)

func testFlags() *flag.FlagSet {
	fs := flag.NewFlagSet("samsa", flag.ContinueOnError)
	fs.String("sync", "always", "")
	fs.String("log-level", "info", "")
	fs.String("scrub-interval", "0s", "")
	fs.String("producer-expiry", "24h0m0s", "")
	fs.String("peers", "", "")
	fs.String("peer-tls-cert", "", "")
	fs.String("tls-cert", "", "")
	fs.String("sasl-mechanisms", "", "")
	fs.String("profile", "", "")
	return fs
}

func TestProfileSetsOnlyWhatWasNotSet(t *testing.T) {
	fs := testFlags()
	_ = fs.Set("log-level", "debug")
	set := map[string]string{"log-level": "command line"}
	if err := applyProfile(fs, "test", set, false); err != nil {
		t.Fatal(err)
	}
	if got := fs.Lookup("log-level").Value.String(); got != "debug" {
		t.Fatalf("log-level %q: the command line must win", got)
	}
	if got := fs.Lookup("sync").Value.String(); got != "never" || set["sync"] != "profile test" {
		t.Fatalf("sync %q from %q", got, set["sync"])
	}
	// A cluster keeps -sync=always whatever the profile says.
	fs = testFlags()
	if err := applyProfile(fs, "dev", map[string]string{}, true); err != nil {
		t.Fatal(err)
	}
	if got := fs.Lookup("sync").Value.String(); got != "always" {
		t.Fatalf("a clustered dev node has -sync=%s", got)
	}
	if err := applyProfile(fs, "staging", map[string]string{}, false); err == nil {
		t.Fatal("an unknown profile was accepted")
	}
}

func TestDoctorReportsSourcesAndRisks(t *testing.T) {
	fs := testFlags()
	set := map[string]string{}
	_ = fs.Set("sync", "interval")
	set["sync"] = "config file"
	var out bytes.Buffer
	danger := doctor(&out, fs, set, "prod")
	text := out.String()
	for _, want := range []string{"sync", "interval", "(config file)", "! writes acknowledged", "! client traffic is not encrypted", "! any client", "! a single node"} {
		if !strings.Contains(text, want) {
			t.Errorf("the report lacks %q:\n%s", want, text)
		}
	}
	if !danger {
		t.Fatal("a node with no TLS and no SASL is not reported as risky")
	}
	// A defaults-only report names nothing that is fine.
	_ = fs.Set("sync", "always")
	_ = fs.Set("tls-cert", "x.pem")
	_ = fs.Set("sasl-mechanisms", "SCRAM-SHA-256")
	_ = fs.Set("peers", "1=a:1/b:2,2=c:3/d:4,3=e:5/f:6")
	out.Reset()
	if !doctor(&out, fs, map[string]string{}, "prod") || !strings.Contains(out.String(), "nodes do not authenticate each other") {
		t.Fatalf("a cluster with plain peer traffic is not reported:\n%s", out.String())
	}
	_ = fs.Set("peer-tls-cert", "peer.pem")
	out.Reset()
	if doctor(&out, fs, map[string]string{}, "prod") {
		t.Fatalf("a secured three-node cluster is reported as risky:\n%s", out.String())
	}
}
