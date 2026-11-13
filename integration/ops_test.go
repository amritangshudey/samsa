package integration

import (
	"context"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// samsaCheck runs `samsa check` and returns its output and whether it
// exited 0.
func samsaCheck(t *testing.T, dir string) (string, bool) {
	t.Helper()
	bin := os.Getenv("SAMSA_BIN")
	if bin == "" {
		bin = "../bin/samsa"
	}
	out, err := exec.Command(bin, "check", "-data", dir).CombinedOutput()
	if _, exited := err.(*exec.ExitError); err != nil && !exited {
		t.Fatal(err)
	}
	return string(out), err == nil
}

// TestOperations: settings from a config file, the HTTP endpoints of a
// running node, and `samsa check` on its data before and after a planted
// torn tail (docs/design/b4-operations.md, section 5).
func TestOperations(t *testing.T) {
	metricsAddr := freeAddr(t)
	config := filepath.Join(t.TempDir(), "samsa.conf")
	if err := os.WriteFile(config, []byte("# test\nmetrics-listen = "+metricsAddr+"\nsync = always\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	b := startBroker(t, "-config", config)
	get := func(path string) (int, string) {
		t.Helper()
		resp, err := http.Get("http://" + metricsAddr + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}
	waitUntil(t, "/readyz", func() bool { code, _ := get("/readyz"); return code == 200 })
	if code, body := get("/healthz"); code != 200 {
		t.Fatalf("/healthz: %d %s", code, body)
	}

	cl := franz(t, b)
	createTopic(t, cl, "ops", 1)
	if err := cl.ProduceSync(context.Background(), &kgo.Record{Topic: "ops", Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	cl.Close()
	waitUntil(t, "metrics to show the produce", func() bool {
		_, body := get("/metrics")
		return strings.Contains(body, `samsa_requests_total{api_key="0"} 1`) && strings.Contains(body, "\nsamsa_topics 1\n")
	})

	if out, ok := samsaCheck(t, b.dir); ok || !strings.Contains(out, "in use") {
		t.Fatalf("check of a running node's data: ok=%v\n%s", ok, out)
	}
	b.kill()
	if out, ok := samsaCheck(t, b.dir); !ok {
		t.Fatalf("check of clean data failed:\n%s", out)
	}

	// Bytes after the last record, as a crash in mid-write leaves them.
	segs, _ := filepath.Glob(filepath.Join(b.dir, "raft", "*.log"))
	active := segs[len(segs)-1]
	f, err := os.OpenFile(active, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("torn write"))
	f.Close()
	before, _ := os.Stat(active)
	out, ok := samsaCheck(t, b.dir)
	if ok || !strings.Contains(out, "repair needed: cut raft/") {
		t.Fatalf("torn tail not reported: ok=%v\n%s", ok, out)
	}
	if after, _ := os.Stat(active); after.Size() != before.Size() {
		t.Fatalf("check changed the segment: %d bytes, was %d", after.Size(), before.Size())
	}

	// A typo in the config file stops the start.
	os.WriteFile(config, []byte("snyc = never\n"), 0o600)
	bin := os.Getenv("SAMSA_BIN")
	if bin == "" {
		bin = "../bin/samsa"
	}
	if out, err := exec.Command(bin, "-config", config, "-data", t.TempDir()).CombinedOutput(); err == nil || !strings.Contains(string(out), `unknown key "snyc"`) {
		t.Fatalf("unknown key accepted: %v\n%s", err, out)
	}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}
