package integration

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ledger records which group member read each record. A record read twice
// is a duplicate: the tests arrange that everything read is committed
// before a member dies, so a correct broker causes none.
type ledger struct {
	mu       sync.Mutex
	by       map[string]string // "partition/offset" -> member
	dups     []string
	assigned map[string]int // member -> partitions held
	idle     map[string]bool
}

func newLedger() *ledger {
	return &ledger{by: map[string]string{}, assigned: map[string]int{}, idle: map[string]bool{}}
}

func (l *ledger) record(member string, partition int32, offset int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := fmt.Sprintf("%d/%d", partition, offset)
	if first, ok := l.by[key]; ok {
		l.dups = append(l.dups, fmt.Sprintf("%s read by %s and again by %s", key, first, member))
	}
	l.by[key] = member
	l.idle[member] = false
}

// wait polls until cond holds, or fails the test.
func (l *ledger) wait(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(60 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		l.mu.Lock()
		ok, dups := cond(), l.dups
		state := fmt.Sprintf("%d records read, assignments %v", len(l.by), l.assigned)
		l.mu.Unlock()
		if len(dups) > 0 {
			t.Fatalf("%s: records delivered twice, first: %s", what, dups[0])
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s (%s)", what, state)
		}
	}
}

func (l *ledger) readBy(member string) int {
	n := 0
	for _, who := range l.by {
		if who == member {
			n++
		}
	}
	return n
}

func produceN(t *testing.T, b *broker, topic string, partitions int32, from, n int) {
	t.Helper()
	var recs []*kgo.Record
	for i := from; i < from+n; i++ {
		recs = append(recs, &kgo.Record{Topic: topic, Partition: int32(i) % partitions, Value: []byte(fmt.Sprintf("v%d", i))})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Uncompressed: the Java class path used here has no snappy library.
	if err := franz(t, b, kgo.ProducerBatchCompression(kgo.NoCompression())).ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

// joinsSince counts JoinGroup requests in the broker's log after mark.
func (b *broker) joinsSince(mark int) int {
	n := 0
	for _, m := range requestLine.FindAllStringSubmatch(b.log()[mark:], -1) {
		if m[2] == "11" {
			n++
		}
	}
	return n
}

// severable is a dialer whose connections can all be cut at once and never
// re-established: the nearest thing to kill -9 for a client in this process.
type severable struct {
	mu    sync.Mutex
	conns []net.Conn
	dead  bool
}

func (s *severable) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead {
		return nil, errors.New("this client is dead")
	}
	c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
	if err == nil {
		s.conns = append(s.conns, c)
	}
	return c, err
}

func (s *severable) kill() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dead = true
	for _, c := range s.conns {
		c.Close()
	}
}

// Roadmap 3.6 with franz-go: two consumers share a topic; one dies without
// leaving; the other takes over from the committed offsets; then the broker
// is killed and restarted under the stable group.
func TestFranzGoConsumerGroup(t *testing.T) {
	b := startBroker(t)
	const topic, parts = "grouped", 4
	createTopic(t, franz(t, b), topic, parts)

	l := newLedger()
	var stopB atomic.Bool
	var wires [2]severable
	var done [2]chan struct{}
	for i, name := range []string{"A", "B"} {
		track := func(delta int) func(context.Context, *kgo.Client, map[string][]int32) {
			return func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
				l.mu.Lock()
				l.assigned[name] += delta * len(m[topic])
				l.mu.Unlock()
			}
		}
		cl := franz(t, b, kgo.Dialer(wires[i].dial), kgo.ConsumerGroup("franz-group"), kgo.ConsumeTopics(topic),
			kgo.DisableAutoCommit(), kgo.SessionTimeout(6*time.Second), kgo.HeartbeatInterval(500*time.Millisecond),
			kgo.OnPartitionsAssigned(track(1)), kgo.OnPartitionsRevoked(track(-1)), kgo.OnPartitionsLost(track(-1)))
		done[i] = make(chan struct{})
		go func() {
			defer close(done[i])
			for name != "B" || !stopB.Load() {
				ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
				fetches := cl.PollFetches(ctx)
				cancel()
				if fetches.IsClientClosed() {
					return
				}
				fetches.EachRecord(func(r *kgo.Record) { l.record(name, r.Partition, r.Offset) })
				if fetches.NumRecords() > 0 {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					if err := cl.CommitUncommittedOffsets(ctx); err != nil {
						t.Errorf("%s: commit: %v", name, err)
					}
					cancel()
				}
			}
		}()
	}
	l.wait(t, "both members to hold two partitions", func() bool { return l.assigned["A"] == 2 && l.assigned["B"] == 2 })

	produceN(t, b, topic, parts, 0, 200)
	l.wait(t, "the first 200 records", func() bool { return len(l.by) == 200 })
	if a, bb := l.readBy("A"), l.readBy("B"); a != 100 || bb != 100 {
		t.Fatalf("records were not shared evenly: A read %d, B read %d", a, bb)
	}

	// B stops after its commit and then vanishes without LeaveGroup.
	stopB.Store(true)
	<-done[1]
	wires[1].kill()
	produceN(t, b, topic, parts, 200, 200)
	l.wait(t, "A to take over all partitions and read the rest", func() bool { return len(l.by) == 400 })
	if a := l.readBy("A"); a != 300 {
		t.Fatalf("A read %d records in total, want 300: its own 100 and all 200 new ones", a)
	}

	// kill -9 the broker under the now stable group.
	mark := len(b.log())
	b.kill()
	b.start()
	produceN(t, b, topic, parts, 400, 100)
	l.wait(t, "A to continue after the broker restart", func() bool { return len(l.by) == 500 })
	if n := b.joinsSince(mark); n != 0 {
		t.Errorf("the restart caused %d JoinGroup requests: the group should have stayed stable", n)
	}
}

// A consumer that assigns its own partitions can still store offsets under
// a group id (roadmap 3.4 step 3), and they survive a restart.
func TestFranzGoCommitWithoutMembership(t *testing.T) {
	b := startBroker(t)
	cl := franz(t, b)
	createTopic(t, cl, "manual", 2)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Group, commit.Generation = "manual-group", -1
	ct := kmsg.NewOffsetCommitRequestTopic()
	ct.Topic = "manual"
	for p, offset := range []int64{11, 22} {
		cp := kmsg.NewOffsetCommitRequestTopicPartition()
		cp.Partition, cp.Offset = int32(p), offset
		ct.Partitions = append(ct.Partitions, cp)
	}
	commit.Topics = append(commit.Topics, ct)
	cresp, err := commit.RequestWith(ctx, cl)
	if err != nil || cresp.Topics[0].Partitions[0].ErrorCode != 0 || cresp.Topics[0].Partitions[1].ErrorCode != 0 {
		t.Fatalf("commit: %v %+v", err, cresp)
	}

	b.kill()
	b.start()
	fetch := kmsg.NewPtrOffsetFetchRequest()
	fetch.Group = "manual-group"
	ft := kmsg.NewOffsetFetchRequestTopic()
	ft.Topic, ft.Partitions = "manual", []int32{0, 1}
	fetch.Topics = append(fetch.Topics, ft)
	fresp, err := fetch.RequestWith(ctx, franz(t, b))
	if err != nil {
		t.Fatalf("offset fetch: %v", err)
	}
	got := fresp.Topics
	if len(got) != 1 || len(got[0].Partitions) != 2 || got[0].Partitions[0].Offset != 11 || got[0].Partitions[1].Offset != 22 {
		t.Fatalf("offsets after restart: %+v", got)
	}
}

// Roadmap 3.6 with the official Java client, as two real processes, one of
// which is killed with SIGKILL. See java/GroupMember.java.
func TestJavaConsumerGroup(t *testing.T) {
	java, jars := javaAndJars(t)
	b := startBroker(t)
	const topic, parts = "jgrouped", 4
	createTopic(t, franz(t, b), topic, parts)

	l := newLedger()
	start := func(name string) *exec.Cmd {
		cmd := exec.Command(java, "-cp", jars, "java/GroupMember.java", b.addr, topic, "java-group")
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
		go func() {
			for sc := bufio.NewScanner(out); sc.Scan(); {
				f := strings.Fields(sc.Text())
				switch {
				case len(f) == 2 && f[0] == "ASSIGNED":
					l.mu.Lock()
					var n int
					fmt.Sscan(f[1], &n)
					l.assigned[name] = n
					l.mu.Unlock()
				case len(f) == 4 && f[0] == "REC":
					var p int32
					var off int64
					fmt.Sscan(f[1], &p)
					fmt.Sscan(f[2], &off)
					l.record(name, p, off)
				case len(f) == 1 && f[0] == "COMMITTED":
					l.mu.Lock()
					l.idle[name] = true
					l.mu.Unlock()
				}
			}
		}()
		return cmd
	}
	start("A")
	victim := start("B")
	l.wait(t, "both Java members to hold two partitions", func() bool { return l.assigned["A"] == 2 && l.assigned["B"] == 2 })

	produceN(t, b, topic, parts, 0, 200)
	l.wait(t, "the first 200 records, committed", func() bool { return len(l.by) == 200 && l.idle["A"] && l.idle["B"] })
	if a, bb := l.readBy("A"), l.readBy("B"); a != 100 || bb != 100 {
		t.Fatalf("records were not shared evenly: A read %d, B read %d", a, bb)
	}

	victim.Process.Kill() // SIGKILL: no LeaveGroup
	produceN(t, b, topic, parts, 200, 200)
	l.wait(t, "A to take over all partitions and read the rest", func() bool { return len(l.by) == 400 && l.idle["A"] })
	if a := l.readBy("A"); a != 300 {
		t.Fatalf("A read %d records in total, want 300", a)
	}

	mark := len(b.log())
	b.kill()
	b.start()
	produceN(t, b, topic, parts, 400, 100)
	l.wait(t, "A to continue after the broker restart", func() bool { return len(l.by) == 500 })
	if n := b.joinsSince(mark); n != 0 {
		t.Errorf("the restart caused %d JoinGroup requests: the group should have stayed stable", n)
	}
}

// kcat (librdkafka) as a group member: it reads everything, commits on
// exit, and a second run in the same group starts where the first stopped.
func TestKcatConsumerGroup(t *testing.T) {
	kcat, err := exec.LookPath("kcat")
	if err != nil {
		missing(t, "kcat")
	}
	b := startBroker(t)
	createTopic(t, franz(t, b), "kgroup", 2)
	run := func() []string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, kcat, "-b", b.addr, "-G", "kcat-group", "-X", "auto.offset.reset=earliest", "-e", "-q", "kgroup")
		var stderr strings.Builder
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("kcat -G: %v\n%s", err, stderr.String())
		}
		return strings.Fields(string(out))
	}
	produceN(t, b, "kgroup", 2, 0, 100)
	if got := run(); len(got) != 100 {
		t.Fatalf("first run read %d records, want 100", len(got))
	}
	produceN(t, b, "kgroup", 2, 100, 10)
	if got := run(); len(got) != 10 {
		t.Fatalf("second run read %d records, want only the 10 new ones: %v", len(got), got)
	}
}
