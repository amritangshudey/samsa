package integration

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/amritangshudey/samsa/internal/sim"
)

// The black-box chaos test (docs/design/a4-chaos.md): three real processes
// behind proxies, faults from a seed, real clients, and the simulator's
// own history checker.

const (
	chaosTopic = "chaos"
	chaosParts = 3
)

type chaosCluster struct {
	t       *testing.T
	nodes   map[int32]*broker
	clients map[int32]*proxy    // in front of each node's client port
	peers   map[[2]int32]*proxy // [i, j]: what node i dials to reach j

	mu    sync.Mutex // guards model and failure
	model *sim.Model
	fail  error

	acked, checked atomic.Int64
}

// startChaos starts three nodes of bin with args, every link behind a proxy.
func startChaos(t *testing.T, bin string, args ...string) *chaosCluster {
	t.Helper()
	c := &chaosCluster{t: t, nodes: map[int32]*broker{}, clients: map[int32]*proxy{}, peers: map[[2]int32]*proxy{}, model: sim.NewModel(true)}
	peerReal := map[int32]string{}
	c.model.Rebatched() // franz-go forms its own batches
	for id := int32(1); id <= 3; id++ {
		c.nodes[id] = &broker{t: t, addr: freeAddr(t), dir: t.TempDir(), bin: bin}
		peerReal[id] = freeAddr(t)
		c.clients[id] = newProxy(t, c.nodes[id].addr)
	}
	for i := int32(1); i <= 3; i++ {
		for j := int32(1); j <= 3; j++ {
			if i != j {
				c.peers[[2]int32{i, j}] = newProxy(t, peerReal[j])
			}
		}
	}
	for i, b := range c.nodes {
		var list []string
		for j := int32(1); j <= 3; j++ {
			peer := peerReal[j]
			if j != i {
				peer = c.peers[[2]int32{i, j}].addr()
			}
			list = append(list, fmt.Sprintf("%d=%s/%s", j, c.clients[j].addr(), peer))
		}
		b.args = append([]string{"-node-id", fmt.Sprint(i), "-peers", strings.Join(list, ","), "-advertise", c.clients[i].addr()}, args...)
		b.start()
	}
	t.Cleanup(func() {
		for id, b := range c.nodes {
			b.kill()
			if t.Failed() {
				t.Logf("node %d log (last 3000 bytes):\n%s", id, last(b.log(), 3000))
			}
		}
	})
	return c
}

func (c *chaosCluster) client(opts ...kgo.Opt) *kgo.Client {
	var seeds []string
	for id := int32(1); id <= 3; id++ {
		seeds = append(seeds, c.clients[id].addr())
	}
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(seeds...)}, opts...)...)
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(cl.Close)
	return cl
}

// failed records the first violation; every later check is skipped.
func (c *chaosCluster) failed(err error) {
	if err != nil && c.fail == nil {
		c.fail = err
	}
}

func (c *chaosCluster) failure() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fail
}

// leader returns the node that a majority of the running nodes name as
// leader, or -1.
func (c *chaosCluster) leader(cl *kgo.Client) int32 {
	votes := map[int32]int{}
	for id, b := range c.nodes {
		if b.cmd == nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		resp, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl.Broker(int(id)))
		cancel()
		if err == nil && resp.ControllerID > 0 {
			votes[resp.ControllerID]++
		}
	}
	for id, n := range votes {
		if n >= 2 && c.nodes[id].cmd != nil {
			return id
		}
	}
	return -1
}

// produce writes records until ctx ends, each with an id from the model.
func (c *chaosCluster) produce(ctx context.Context, cl *kgo.Client, rng *rand.Rand, wg *sync.WaitGroup) {
	defer wg.Done()
	for ctx.Err() == nil {
		tp := sim.TP{Topic: chaosTopic, Part: int32(rng.IntN(chaosParts))}
		c.mu.Lock()
		id := c.model.Send(tp, 1)
		c.mu.Unlock()
		rec := &kgo.Record{Topic: tp.Topic, Partition: tp.Part, Value: sim.Value(id)}
		wg.Add(1)
		cl.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
			defer wg.Done()
			c.mu.Lock()
			defer c.mu.Unlock()
			if err != nil {
				c.model.Unknown(id, 1)
				return
			}
			c.failed(c.model.Acked(id, 1, r.Offset, 0))
			c.acked.Add(1)
		})
		time.Sleep(time.Millisecond)
	}
}

// fetch sends one raw Fetch for tp at offset to node and checks the answer
// against the model. It returns the next offset to ask for, or -1.
func (c *chaosCluster) fetch(cl *kgo.Client, node int32, tp sim.TP, offset int64, wait int32) (next int64, answered bool) {
	c.mu.Lock()
	f := sim.Fetch{TP: tp, Offset: offset, Floor: c.model.Floor(tp), Epoch: c.model.Epoch(), Known: c.model.Known(tp), Start: c.model.Start(tp), MustProgress: true}
	c.mu.Unlock()
	req := kmsg.NewPtrFetchRequest()
	req.Version, req.ReplicaID, req.MaxWaitMillis, req.MinBytes, req.MaxBytes, req.SessionEpoch = 11, -1, wait, 1, 1<<20, -1
	rt := kmsg.NewFetchRequestTopic()
	rt.Topic = tp.Topic
	rp := kmsg.NewFetchRequestTopicPartition()
	rp.Partition, rp.FetchOffset, rp.PartitionMaxBytes, rp.LogStartOffset, rp.CurrentLeaderEpoch = tp.Part, offset, 1<<20, -1, -1
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl.Broker(int(node)))
	if err != nil {
		return -1, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		c.failed(fmt.Errorf("node %d: fetch response does not match the request", node))
		return -1, false
	}
	p := resp.Topics[0].Partitions[0]
	if p.ErrorCode == 6 { // NOT_LEADER_OR_FOLLOWER: no answer, as the simulator's clients treat it
		return -1, false
	}
	f.Err, f.HW, f.LogStart, f.Records = p.ErrorCode, p.HighWatermark, p.LogStartOffset, p.RecordBatches
	next, err = c.model.Fetched(f)
	if err != nil {
		c.failed(fmt.Errorf("fetch from node %d: %w", node, err))
		return -1, false
	}
	c.checked.Add(1)
	return next, p.ErrorCode == 0
}

// read fetches from random nodes until ctx ends, each partition from its
// own cursor.
func (c *chaosCluster) read(ctx context.Context, cl *kgo.Client, rng *rand.Rand, wg *sync.WaitGroup) {
	defer wg.Done()
	cursors := make([]int64, chaosParts)
	for ctx.Err() == nil && c.failure() == nil {
		part := rng.IntN(chaosParts)
		next, _ := c.fetch(cl, int32(1+rng.IntN(3)), sim.TP{Topic: chaosTopic, Part: int32(part)}, cursors[part], int32(rng.IntN(3)*50))
		if next >= 0 {
			cursors[part] = next
		}
	}
}

// finalRead reads every partition from 0 at the leader: every acknowledged
// record must be there, once, at its offset.
func (c *chaosCluster) finalRead(cl *kgo.Client) error {
	c.mu.Lock()
	c.model.BeginFinal()
	c.mu.Unlock()
	deadline := time.Now().Add(30 * time.Second)
	for part := int32(0); part < chaosParts; part++ {
		for offset := int64(0); ; {
			if time.Now().After(deadline) {
				return fmt.Errorf("final read of partition %d stuck at offset %d", part, offset)
			}
			leader := c.leader(cl)
			if leader < 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			next, ok := c.fetch(cl, leader, sim.TP{Topic: chaosTopic, Part: part}, offset, 0)
			if err := c.failure(); err != nil {
				return err
			}
			if next >= 0 {
				offset = next
			} else if ok {
				break // answered with nothing: the end of the partition
			}
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.model.CheckFinal()
}

// The faults. Each one runs, holds for a while, and heals itself.
type fault struct {
	name string
	run  func(c *chaosCluster, cl *kgo.Client, rng *rand.Rand) string
}

func hold(rng *rand.Rand) time.Duration { return time.Duration(1000+rng.IntN(2000)) * time.Millisecond }

func (c *chaosCluster) isolate(id int32, cut bool) {
	for j := int32(1); j <= 3; j++ {
		if j != id {
			c.peers[[2]int32{id, j}].setCut(cut)
			c.peers[[2]int32{j, id}].setCut(cut)
		}
	}
}

func (c *chaosCluster) pick(cl *kgo.Client, rng *rand.Rand) int32 {
	if l := c.leader(cl); l > 0 && rng.IntN(2) == 0 {
		return l
	}
	return int32(1 + rng.IntN(3))
}

var faults = []fault{
	{"kill", func(c *chaosCluster, cl *kgo.Client, rng *rand.Rand) string {
		id := c.pick(cl, rng)
		c.nodes[id].kill()
		time.Sleep(hold(rng))
		c.nodes[id].start()
		return fmt.Sprintf("SIGKILL node %d, restart", id)
	}},
	{"stop", func(c *chaosCluster, cl *kgo.Client, rng *rand.Rand) string {
		id := c.pick(cl, rng)
		b := c.nodes[id]
		b.cmd.Process.Signal(syscall.SIGTERM)
		b.cmd.Wait()
		b.cmd = nil
		b.start()
		return fmt.Sprintf("SIGTERM node %d, restart", id)
	}},
	{"isolate", func(c *chaosCluster, cl *kgo.Client, rng *rand.Rand) string {
		id := c.pick(cl, rng)
		c.isolate(id, true)
		time.Sleep(hold(rng))
		c.isolate(id, false)
		return fmt.Sprintf("isolate node %d from its peers", id)
	}},
	{"cut", func(c *chaosCluster, cl *kgo.Client, rng *rand.Rand) string {
		i := int32(1 + rng.IntN(3))
		j := i%3 + 1 + int32(rng.IntN(2))
		if j > 3 {
			j -= 3
		}
		p := c.peers[[2]int32{i, j}]
		p.setCut(true)
		time.Sleep(hold(rng))
		p.setCut(false)
		return fmt.Sprintf("cut link %d->%d", i, j)
	}},
	{"slow", func(c *chaosCluster, cl *kgo.Client, rng *rand.Rand) string {
		var slow []*proxy
		for _, p := range c.allProxies() {
			if rng.IntN(2) == 0 {
				p.setDelay(time.Duration(5+rng.IntN(45)) * time.Millisecond)
				slow = append(slow, p)
			}
		}
		time.Sleep(hold(rng))
		for _, p := range slow {
			p.setDelay(0)
		}
		return fmt.Sprintf("delay %d links", len(slow))
	}},
	{"reset", func(c *chaosCluster, cl *kgo.Client, rng *rand.Rand) string {
		for range 3 {
			for _, p := range c.allProxies() {
				p.reset()
			}
			time.Sleep(300 * time.Millisecond)
		}
		return "reset every connection, three times"
	}},
}

func (c *chaosCluster) allProxies() []*proxy {
	var all []*proxy
	for id := int32(1); id <= 3; id++ {
		all = append(all, c.clients[id])
		for j := int32(1); j <= 3; j++ {
			if p := c.peers[[2]int32{id, j}]; p != nil {
				all = append(all, p)
			}
		}
	}
	return all
}

func chaosSeed(t *testing.T) uint64 {
	if s := os.Getenv("CHAOS_SEED"); s != "" {
		seed, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("CHAOS_SEED: %v", err)
		}
		return seed
	}
	return uint64(time.Now().UnixNano())
}

// run starts the topic, two producers and two readers, applies steps until
// done reports true, then stops the clients and makes the final read.
func (c *chaosCluster) run(seed uint64, step func(cl *kgo.Client, rng *rand.Rand) (string, bool)) error {
	t := c.t
	admin := c.client()
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		req := kmsg.NewPtrCreateTopicsRequest()
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic, rt.NumPartitions, rt.ReplicationFactor = chaosTopic, chaosParts, -1
		req.Topics = append(req.Topics, rt)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := req.RequestWith(ctx, admin)
		cancel()
		if err == nil && resp.Topics[0].ErrorCode == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("create topic: %v %+v", err, resp)
		}
	}
	c.mu.Lock()
	c.model.TopicAttempted(chaosTopic, chaosParts)
	c.model.TopicCreated(chaosTopic, 0)
	c.mu.Unlock()

	rng := rand.New(rand.NewPCG(seed, 0))
	ctx, stop := context.WithCancel(context.Background())
	var producers, readers sync.WaitGroup
	var pcls []*kgo.Client
	for i := range 2 {
		// Uncompressed: the checker decodes record values, and the wire
		// package (like the broker) never decompresses.
		pcl := c.client(kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.RecordDeliveryTimeout(20*time.Second), kgo.ProducerBatchCompression(kgo.NoCompression()))
		pcls = append(pcls, pcl)
		producers.Add(1)
		go c.produce(ctx, pcl, rand.New(rand.NewPCG(seed, uint64(1+i))), &producers)
		readers.Add(1)
		go c.read(ctx, c.client(), rand.New(rand.NewPCG(seed, uint64(10+i))), &readers)
	}
	for c.failure() == nil {
		acked, checked := c.acked.Load(), c.checked.Load()
		what, more := step(admin, rng)
		t.Logf("%s: %s", time.Now().Format("15:04:05.000"), what)
		// Healed: the cluster must make progress again.
		for deadline := time.Now().Add(20 * time.Second); c.failure() == nil && (c.acked.Load() == acked || c.checked.Load() == checked); time.Sleep(50 * time.Millisecond) {
			if time.Now().After(deadline) {
				stop()
				return fmt.Errorf("no progress for 20 s after %q (acked %d, checked %d)", what, c.acked.Load(), c.checked.Load())
			}
		}
		if !more {
			break
		}
	}
	stop()
	readers.Wait()
	for _, pcl := range pcls {
		fctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		pcl.Flush(fctx) //nolint:errcheck // unresolved records end as Unknown below
		cancel()
	}
	producers.Wait()
	if err := c.failure(); err != nil {
		return err
	}
	return c.finalRead(admin)
}

func TestChaos(t *testing.T) {
	duration := 40 * time.Second
	if s := os.Getenv("CHAOS_DURATION"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			t.Fatalf("CHAOS_DURATION: %v", err)
		}
		duration = d
	}
	seed := chaosSeed(t)
	t.Logf("seed %d; replay: CHAOS_SEED=%d make chaos", seed, seed)
	c := startChaos(t, "")
	end := time.Now().Add(duration)
	steps := 0
	err := c.run(seed, func(cl *kgo.Client, rng *rand.Rand) (string, bool) {
		f := faults[rng.IntN(len(faults))]
		steps++
		return f.run(c, cl, rng), time.Now().Before(end)
	})
	if err != nil {
		t.Fatalf("seed %d: %v\nreplay: CHAOS_SEED=%d make chaos", seed, err, seed)
	}
	t.Logf("%d steps, %d records acknowledged, %d fetches checked", steps, c.acked.Load(), c.checked.Load())
}

// probeStale waits for the nodes other than old to elect a leader, writes
// one record through it, and then reads from old: the read most likely to
// be stale, if old still answers reads.
func (c *chaosCluster) probeStale(cl *kgo.Client, old int32) string {
	next := int32(-1)
	for deadline := time.Now().Add(4 * time.Second); next < 0 && time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		for j := int32(1); j <= 3; j++ {
			if j == old {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			resp, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl.Broker(int(j)))
			cancel()
			if err == nil && resp.ControllerID > 0 && resp.ControllerID != old {
				next = resp.ControllerID
			}
		}
	}
	if next < 0 {
		return "no new leader"
	}
	pcl, err := kgo.NewClient(kgo.SeedBrokers(c.clients[next].addr()), kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerBatchCompression(kgo.NoCompression()), kgo.RecordDeliveryTimeout(2*time.Second))
	if err != nil {
		c.t.Fatal(err)
	}
	defer pcl.Close()
	tp := sim.TP{Topic: chaosTopic, Part: 0}
	c.mu.Lock()
	id := c.model.Send(tp, 1)
	c.mu.Unlock()
	r, err := pcl.ProduceSync(context.Background(), &kgo.Record{Topic: tp.Topic, Partition: tp.Part, Value: sim.Value(id)}).First()
	c.mu.Lock()
	if err != nil {
		c.model.Unknown(id, 1)
		c.mu.Unlock()
		return fmt.Sprintf("new leader %d, probe write failed: %v", next, err)
	}
	c.failed(c.model.Acked(id, 1, r.Offset, 0))
	c.acked.Add(1)
	c.mu.Unlock()
	c.fetch(cl, old, tp, 0, 0)
	return fmt.Sprintf("new leader %d acknowledged offset %d; read from %d", next, r.Offset, old)
}

// A build with stale reads on purpose must fail the checker: an isolated
// leader that keeps serving reads (no lease, no CheckQuorum) answers below
// writes the majority acknowledged meanwhile. The same scenario must pass
// on the real build, or the catch proves nothing.
func TestChaosCatchesBrokenBuild(t *testing.T) {
	bin := os.Getenv("SAMSA_BIN")
	if bin == "" {
		bin = "../bin/samsa"
	}
	unsafe := filepath.Join(filepath.Dir(bin), "samsa-unsafe")
	if _, err := os.Stat(unsafe); err != nil {
		missing(t, "bin/samsa-unsafe (run `make build`)")
	}
	isolations := func(t *testing.T, bin string, args ...string) (*chaosCluster, int, error) {
		c := startChaos(t, bin, args...)
		n := 0
		err := c.run(1, func(cl *kgo.Client, rng *rand.Rand) (string, bool) {
			n++
			id := c.leader(cl)
			if id < 0 {
				return "no leader", n < 3
			}
			c.isolate(id, true)
			end := time.Now().Add(6 * time.Second)
			probe := c.probeStale(cl, id)
			time.Sleep(time.Until(end))
			c.isolate(id, false)
			return fmt.Sprintf("isolate leader %d for 6 s: %s", id, probe), n < 3
		})
		return c, n, err
	}
	t.Run("real build passes", func(t *testing.T) {
		if c, _, err := isolations(t, bin); err != nil {
			t.Fatalf("%v (%d acknowledged, %d fetches checked)", err, c.acked.Load(), c.checked.Load())
		}
	})
	t.Run("broken build is caught", func(t *testing.T) {
		c, n, err := isolations(t, unsafe, "-unsafe", "no-lease,no-check-quorum")
		if err == nil {
			t.Fatalf("passed three leader isolations (%d acknowledged, %d fetches checked)", c.acked.Load(), c.checked.Load())
		}
		t.Logf("caught after %d isolations: %v", n, err)
	})
}
