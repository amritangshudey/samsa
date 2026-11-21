package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestStorageRotRepair (docs/design/b1-storage-faults.md): one node of three
// has a byte of a sealed segment of its Raft log changed on disk. Its scrub
// finds it and the process exits; its next start cuts the damage out and
// fetches the log again from the others; and nothing acknowledged is lost,
// though the other nodes are then taken down one at a time.
func TestStorageRotRepair(t *testing.T) {
	if testing.Short() {
		t.Skip("writes about 150 MB per node")
	}
	c := startCluster(t, 3, "-log-level", "warn", "-scrub-interval", "1s", "-scrub-bytes-per-second", "268435456")
	cl := c.client(kgo.MaxBufferedRecords(10000))
	first := c.leader(cl)
	createTopic(t, cl, "rot", 3)

	// More than two 64 MiB segments of 1 KiB records, so that two are sealed.
	const n = 150_000
	value := make([]byte, 1024)
	produce := func(from, to int) {
		t.Helper()
		var recs []*kgo.Record
		for i := from; i < to; i++ {
			v := append(fmt.Appendf(nil, "r-%d-", i), value...)
			recs = append(recs, &kgo.Record{Topic: "rot", Partition: int32(i % 3), Value: v})
			if len(recs) == 5000 || i == to-1 {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				err := cl.ProduceSync(ctx, recs...).FirstErr()
				cancel()
				if err != nil {
					t.Fatalf("produce: %v", err)
				}
				recs = recs[:0]
			}
		}
	}
	produce(0, n)

	// Damage node 2 (not the leader, if it can be helped) in its oldest segment.
	victim := int32(2)
	if first == victim {
		victim = 3
	}
	b := c.nodes[victim]
	segs, _ := filepath.Glob(filepath.Join(b.dir, "raft", "*.log"))
	sort.Strings(segs)
	if len(segs) < 3 {
		t.Fatalf("node %d has %d segments; want at least three", victim, len(segs))
	}
	f, err := os.OpenFile(segs[0], os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := f.Stat()
	if _, err := f.WriteAt([]byte("rot rot rot"), info.Size()/2); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// The scrub finds it and the process ends.
	deadline := time.Now().Add(60 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", b.addr, time.Second)
		if err != nil {
			break
		}
		conn.Close()
		if time.Now().After(deadline) {
			t.Fatalf("node %d is still running 60 s after its segment was damaged:\n%s", victim, last(b.log(), 2000))
		}
		time.Sleep(500 * time.Millisecond)
	}
	b.kill() // reaps it
	if log := b.log(); !contains(log, "damaged") {
		t.Fatalf("node %d stopped, but its log does not say why:\n%s", victim, last(log, 2000))
	}

	// Its next start repairs it. More writes meanwhile.
	b.start()
	produce(n, n+3000)

	// It holds the log again once its directory is as large as another's.
	other := first
	for deadline := time.Now().Add(120 * time.Second); ; time.Sleep(time.Second) {
		have, want := dirBytes(t, filepath.Join(b.dir, "raft")), dirBytes(t, filepath.Join(c.nodes[other].dir, "raft"))
		if have >= want*9/10 {
			t.Logf("node %d holds %d MB of log, node %d %d MB", victim, have>>20, other, want>>20)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %d holds %d MB of log, node %d %d MB, 120 s after its restart", victim, have>>20, other, want>>20)
		}
	}

	// Without the third node, the repaired one is needed: everything is still there.
	third := int32(6) - victim - other
	c.nodes[third].kill()
	c.leader(cl)
	got := countRecords(t, c, "rot", 3)
	if got != n+3000 {
		t.Fatalf("%d records are readable, %d were acknowledged", got, n+3000)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// countRecords reads every partition of topic from the start and counts
// what it reads until a poll comes back empty.
func countRecords(t *testing.T, c *cluster, topic string, parts int) int {
	t.Helper()
	m := map[int32]kgo.Offset{}
	for p := range int32(parts) {
		m[p] = kgo.NewOffset().AtStart()
	}
	cl := c.client(kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: m}))
	defer cl.Close()
	total := 0
	for quiet := 0; quiet < 3; {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		fetches := cl.PollFetches(ctx)
		cancel()
		n := 0
		fetches.EachRecord(func(*kgo.Record) { n++ })
		total += n
		if n == 0 {
			quiet++
		} else {
			quiet = 0
		}
	}
	return total
}
