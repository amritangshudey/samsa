package sim

import (
	"bytes"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/amritangshudey/samsa/internal/broker"
	"github.com/amritangshudey/samsa/internal/simsys"
	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// Config describes one simulation run.
type Config struct {
	// Seed determines everything: the configuration, the workload, the
	// network timing and the faults.
	Seed uint64
	// Duration is how much virtual time the workload runs. Default 10s.
	Duration time.Duration
	// Trace, if set, receives one line per event and the node's logs.
	Trace io.Writer
	// Mutate, if set, may change the broker's configuration after the seed
	// has chosen it. Tests use it to break the broker on purpose.
	Mutate func(*broker.Config)
}

// Result is the outcome of a run.
type Result struct {
	Seed uint64
	// Err is the first violation, or nil.
	Err error
	// Setup describes the configuration the seed chose.
	Setup string
	Stats Stats
	// TraceHash covers every event delivered to the node and every byte it
	// sent, with their virtual times. Equal seeds must give equal hashes.
	TraceHash uint32
}

type fault uint8

const (
	faultPower     fault = iota // power loss at an arbitrary instant
	faultDiskCrash              // power loss at the n-th disk operation from now
	faultKill                   // the process dies; the disk keeps unsynced data
	faultSync                   // the next sync fails
)

// options is the swarm configuration: what kind of run the seed selected.
type options struct {
	faults      []fault
	diskCrash   bool // also crash inside recovery
	disconnects bool
	faultEvery  time.Duration
	producers   int
	consumers   int
	vandals     int
	members     int
	idempotent  int           // idempotent producers (idempotent.go)
	expiry      time.Duration // producer expiry as chosen, before any mutation
	nodes       int
	// Between nodes (network.go): drop and duplicate probabilities per
	// message, and whether partition scenarios run.
	drop, dup  float64
	partitions bool
}

func (f fault) String() string {
	return [...]string{"power", "diskcrash", "kill", "syncfail"}[f]
}

// Run executes one simulation and returns what happened. It is a pure
// function of cfg.
func Run(cfg Config) Result {
	if cfg.Duration <= 0 {
		cfg.Duration = 10 * time.Second
	}
	rng := simsys.NewRand(cfg.Seed)
	w := &world{
		rng: rng.Fork(), now: epochStart, trace: cfg.Trace,
		conns: map[sys.ConnID]*conn{}, producerIDs: map[int64]bool{}, leaders: map[int64]int32{}, cut: map[[2]int32]bool{},
	}
	diskRng := rng.Fork()

	// Swarm testing: each seed enables a random subset of faults and picks
	// its own sizes, so that seeds differ in kind and not only in detail.
	pick := rng.Fork()
	w.bcfg = broker.Config{
		NodeID: 1, Host: "sim", Port: 9092,
		Sync:          []broker.SyncPolicy{broker.SyncAlways, broker.SyncAlways, broker.SyncAlways, broker.SyncInterval, broker.SyncNever}[pick.Intn(5)],
		SyncInterval:  time.Duration(5+pick.Intn(200)) * time.Millisecond,
		SegmentBytes:  []int64{4096, 16384, 1 << 20}[pick.Intn(3)],
		MaxBatchBytes: maxBatchBytes,
		// Short, so that expiries and rebalances happen within a run.
		MinSessionTimeout: 50 * time.Millisecond,
		Logger:            nodeLogger(cfg.Trace),
	}
	for f := faultPower; f <= faultSync; f++ {
		if pick.Chance(0.6) {
			w.opts.faults = append(w.opts.faults, f)
			w.opts.diskCrash = w.opts.diskCrash || f == faultDiskCrash
		}
	}
	w.opts.disconnects = pick.Chance(0.6)
	w.opts.faultEvery = []time.Duration{20 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond, 2 * time.Second}[pick.Intn(4)]
	w.opts.producers, w.opts.consumers, w.opts.vandals = 1+pick.Intn(3), 1+pick.Intn(3), pick.Intn(2)
	w.opts.members = pick.Intn(5)
	w.opts.nodes = []int{1, 3, 3, 5}[pick.Intn(4)]
	// Short, so that elections happen many times within a run.
	w.bcfg.ElectionTimeout = []time.Duration{100, 150, 300}[pick.Intn(3)] * time.Millisecond
	w.bcfg.HeartbeatInterval = w.bcfg.ElectionTimeout / 5
	if w.opts.nodes > 1 {
		w.opts.drop = []float64{0, 0.01, 0.1, 0.3}[pick.Intn(4)]
		w.opts.dup = []float64{0, 0.01, 0.1}[pick.Intn(3)]
		w.opts.partitions = pick.Chance(0.5)
		// One entry per message, sometimes: then a leader's old entries
		// and its own no-op travel separately, which Figure 8 needs.
		w.bcfg.MaxAppendBytes = []int{0, 0, 1}[pick.Intn(3)]
	}
	w.bcfg.SnapshotEvery = []int64{-1, 1, 7, 100}[pick.Intn(4)]
	w.bcfg.RetentionBytes = []int64{0, 0, 64 << 10, 8 << 10}[pick.Intn(4)]
	w.opts.idempotent = pick.Intn(3)
	// Longer than any copy of a batch can linger: a request is answered
	// within answerBound on a live connection, and a producer retries for
	// giveUp. Kafka's default day plays the same role against its
	// two-minute delivery timeout.
	w.opts.expiry = []time.Duration{-1, 15 * time.Second}[pick.Intn(2)]
	w.bcfg.ProducerExpiry = w.opts.expiry
	if cfg.Mutate != nil {
		cfg.Mutate(&w.bcfg)
	}
	strict := w.bcfg.Sync == broker.SyncAlways
	if w.opts.nodes > 1 && !strict {
		// A cluster whose nodes acknowledge before syncing has no defence
		// against a power cut: one node forgetting what it acknowledged
		// breaks what Raft is built on (ADR 0012). That combination is
		// not simulated; only process kills, which lose nothing.
		w.opts.diskCrash = false
		w.opts.faults = slices.DeleteFunc(w.opts.faults, func(f fault) bool { return f != faultKill })
	}
	if !strict {
		// shortcut: a retry re-sends records a power loss may have taken
		// after they were seen, which the model reports as I3; idempotent
		// producers run only where nothing seen can be lost, until the
		// model learns that a lost record may legitimately come back.
		w.opts.idempotent = 0
	}
	w.model = NewModel(strict)
	if w.bcfg.RetentionBytes > 0 {
		w.model.Trimming()
	}
	res := Result{Seed: cfg.Seed, Setup: fmt.Sprintf("nodes=%d sync=%d segment=%d election=%v faults=%v every=%v drop=%v dup=%v partitions=%v append=%d snapshot=%d retention=%d disconnects=%v producers=%d consumers=%d vandals=%d members=%d idempotent=%d expiry=%v",
		w.opts.nodes, w.bcfg.Sync, w.bcfg.SegmentBytes, w.bcfg.ElectionTimeout, w.opts.faults, w.opts.faultEvery, w.opts.drop, w.opts.dup, w.opts.partitions, w.bcfg.MaxAppendBytes, w.bcfg.SnapshotEvery, w.bcfg.RetentionBytes, w.opts.disconnects, w.opts.producers, w.opts.consumers, w.opts.vandals, w.opts.members, w.opts.idempotent, w.opts.expiry)}
	if w.trace != nil {
		_, _ = fmt.Fprintf(w.trace, "seed %d: %s\n", cfg.Seed, res.Setup)
	}

	for id := int32(1); id <= int32(w.opts.nodes); id++ {
		w.bcfg.Peers = append(w.bcfg.Peers, broker.Peer{ID: id, Host: "sim", Port: 9091 + id})
	}
	for _, p := range w.bcfg.Peers {
		nd := &simNode{id: p.ID, cfg: w.bcfg, disk: simsys.NewDisk(diskRng.Fork(), simsys.DiskConfig{})}
		nd.cfg.NodeID, nd.cfg.Port = p.ID, p.Port
		nd.cfg.Logger = w.bcfg.Logger.With("node", p.ID)
		w.nodes = append(w.nodes, nd)
	}
	for _, nd := range w.nodes {
		w.start(nd)
	}
	w.after(100*time.Millisecond, w.compareReplicas)
	behaviours := []struct {
		n    int
		step func(*client)
	}{{w.opts.producers, (*client).produce}, {w.opts.consumers, (*client).consume}, {w.opts.vandals, (*client).vandalise}, {w.opts.members, (*client).groupMember}, {w.opts.idempotent, (*client).produceIdempotent}}
	for _, b := range behaviours {
		for range b.n {
			cl := &client{w: w, rng: rng.Fork(), step: b.step, cursors: map[TP]int64{}, idem: &idemProducer{id: -1}}
			// Timeouts well under answerBound: a held JoinGroup or
			// SyncGroup is answered within the rebalance timeout.
			cl.mem = &membership{group: groupNames[cl.rng.Intn(len(groupNames))], session: int32(100 + cl.rng.Intn(1900)), rebalance: int32(100 + cl.rng.Intn(2900))}
			w.clients = append(w.clients, cl)
			w.after(time.Duration(cl.rng.Intn(int(50*time.Millisecond))), cl.tick)
		}
	}
	w.after(w.opts.faultEvery, w.chaos)
	if w.opts.partitions {
		w.after(time.Duration(w.rng.Intn(1000))*time.Millisecond, w.partition)
	}
	w.runUntil(w.now+sys.Instant(cfg.Duration), nil)

	// Quiesce: no new faults or requests. Let the nodes come back and every
	// outstanding request finish or time out, then judge the final state.
	w.quiet = true
	w.heal()
	w.opts.drop, w.opts.dup = 0, 0
	for _, nd := range w.nodes {
		nd.disk.CrashAfter(0)
		nd.disk.SetSyncErrorRate(0)
	}
	w.runUntil(w.now+sys.Instant(2*answerBound), nil)
	if w.err == nil {
		w.finalChecks()
	}
	res.Err, res.Stats, res.TraceHash = w.err, w.stats, w.hash
	return res
}

// finalChecks reads every partition from the start (the whole-history half
// of I1 to I5) and then checks that recovery is repeatable (I6).
func (w *world) finalChecks() {
	leader := w.settled()
	if leader == nil || !w.writable(leader) {
		return
	}
	var tps []TP
	for _, t := range catalogue {
		for p := range t.parts {
			tps = append(tps, TP{t.name, p})
		}
	}
	w.model.BeginFinal()
	v := &client{w: w, rng: w.rng.Fork(), pin: leader, fetchMin: 5} // v5+ tells where the log starts
	v.c = w.connect(v)
	done := false
	var read func(i int, offset int64)
	read = func(i int, offset int64) {
		if i == len(tps) {
			// G3 and G6 for the whole history: every committed offset.
			v.fetchOffsets(groupNames[0], tps, false, func() {
				v.fetchOffsets(groupNames[1], tps, false, func() { done = true })
			})
			return
		}
		tp := tps[i]
		f := Fetch{TP: tp, Offset: offset, Floor: w.model.Floor(tp), Epoch: w.model.Epoch(), Known: w.model.Known(tp), Start: w.model.Start(tp)}
		req := &wire.FetchRequest{MaxBytes: 1 << 20, Topics: []wire.FetchTopic{{Name: tp.Topic, Partitions: []wire.FetchPartition{
			{Index: tp.Part, Offset: offset, MaxBytes: 1 << 20},
		}}}}
		v.fetch(req, []Fetch{f}, func(f Fetch, next int64) {
			switch {
			case next >= 0:
				read(i, next)
			case f.Err == wire.ErrOffsetOutOfRange && f.LogStart > offset:
				read(i, f.LogStart) // retention cut the start
			default:
				read(i+1, 0) // end of this partition, or it does not exist
			}
		})
	}
	read(0, 0)
	w.runUntil(w.now+sys.Instant(10*answerBound), func() bool { return done })
	if w.err != nil {
		return
	}
	if !done {
		w.fail("the final read did not finish")
		return
	}
	if err := w.model.CheckFinal(); err != nil {
		w.fail("%v", err)
		return
	}

	// I6. A kill loses nothing, so a restart must rebuild the same state.
	// Under the strict policy the same holds across a power loss, since
	// everything applied was synced first. Every node is restarted at
	// once; the cluster must come back to the state it had.
	_, hash := leader.inc.n.StateHash()
	w.stats.ProducersExpired += leader.inc.n.ExpiredProducers()
	// A restarted leader may legitimately trim again; I6 is about replay.
	for _, nd := range w.nodes {
		nd.cfg.RetentionBytes = 0
	}
	for _, power := range []bool{false, true} {
		if power && !w.model.strict {
			break
		}
		for _, nd := range w.nodes {
			w.down(nd, power, "final restart")
		}
		if leader = w.settled(); leader == nil {
			return
		}
		if _, h := leader.inc.n.StateHash(); h != hash {
			w.fail("I6: after a restart (power loss: %v) the state hash is %08x; before it was %08x", power, h, hash)
			return
		}
	}
}

// writable checks L1's second half: once the cluster has settled, a new
// write is acknowledged within liveBound. It creates topic t0 if needed
// and produces one record there, which the final read must then find.
func (w *world) writable(leader *simNode) bool {
	const liveBound = 5 * time.Second
	m, tp := w.model, TP{catalogue[0].name, 0}
	v := &client{w: w, rng: w.rng.Fork(), pin: leader}
	v.c = w.connect(v)
	acked := false
	m.TopicAttempted(tp.Topic, catalogue[0].parts)
	epoch := m.Epoch()
	create := &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{{Name: tp.Topic, NumPartitions: catalogue[0].parts, ReplicationFactor: int16(len(w.nodes))}}}
	cresp := &wire.CreateTopicsResponse{}
	v.request(wire.KeyCreateTopics, 4, create, cresp, func(ok bool) {
		if !ok || len(cresp.Topics) != 1 || (cresp.Topics[0].ErrorCode != wire.ErrNone && cresp.Topics[0].ErrorCode != wire.ErrTopicAlreadyExists) {
			w.fail("L1: on a settled cluster, creating %s failed (connection ok: %v, %+v)", tp.Topic, ok, cresp.Topics)
			return
		}
		m.TopicCreated(tp.Topic, epoch)
		first := m.Send(tp, 1)
		batch := wire.EncodeBatch([]wire.Record{{Value: Value(first), Timestamp: int64(w.now / 1e6)}})
		req := &wire.ProduceRequest{Acks: -1, TimeoutMs: 1000, Topics: []wire.ProduceTopic{{Name: tp.Topic, Partitions: []wire.ProducePartition{{Index: tp.Part, Records: batch}}}}}
		presp := &wire.ProduceResponse{}
		v.request(wire.KeyProduce, 8, req, presp, func(ok bool) {
			if !ok || presp.Topics[0].Partitions[0].ErrorCode != wire.ErrNone {
				m.Unknown(first, 1)
				w.fail("L1: on a settled cluster, a produce failed (connection ok: %v, %+v)", ok, presp.Topics)
				return
			}
			v.check(m.Acked(first, 1, presp.Topics[0].Partitions[0].BaseOffset, epoch))
			acked = true
		})
	})
	w.runUntil(w.now+sys.Instant(liveBound), func() bool { return acked || w.err != nil })
	if !acked && w.err == nil {
		w.fail("L1: a produce on a settled cluster was not acknowledged within %v", liveBound)
	}
	v.hangUp()
	return acked && w.err == nil
}

// settled waits, with no faults and no clients, until every node is up,
// one of them serves as leader, and all have applied the same entries. It
// then checks that they agree (S2 on whole logs, S4) and returns the
// leader; or it records a violation and returns nil.
func (w *world) settled() *simNode {
	var leader *simNode
	agree := func() bool {
		leader = nil
		for _, nd := range w.nodes {
			if nd.inc == nil {
				return false
			}
			if nd.inc.n.Serving() {
				leader = nd
			}
		}
		if leader == nil {
			return false
		}
		last := leader.inc.n.Raft().LastIndex()
		for _, nd := range w.nodes {
			if a, _ := nd.inc.n.StateHash(); a != last+1 || nd.inc.n.Raft().LastIndex() != last {
				return false
			}
		}
		return true
	}
	w.runUntil(w.now+sys.Instant(answerBound), agree)
	if w.err != nil {
		return nil
	}
	if !agree() {
		w.fail("with faults stopped for %v the nodes did not all come back, elect a leader and catch up", answerBound)
		return nil
	}
	lr := leader.inc.n.Raft()
	_, want := leader.inc.n.StateHash()
	for _, nd := range w.nodes {
		if _, h := nd.inc.n.StateHash(); h != want {
			w.fail("S4: nodes %d and %d applied the same %d entries and differ: state hash %08x and %08x", leader.id, nd.id, lr.LastIndex()+1, want, h)
			return nil
		}
		r := nd.inc.n.Raft()
		for i := max(lr.LogStart(), r.LogStart()); nd != leader && i <= lr.LastIndex(); i++ {
			a, _, errA := lr.Read(i)
			b, _, errB := r.Read(i)
			if errA != nil || errB != nil || lr.EntryTerm(i) != r.EntryTerm(i) || !bytes.Equal(a, b) {
				w.fail("S2: nodes %d and %d differ at log index %d (%v, %v)", leader.id, nd.id, i, errA, errB)
				return nil
			}
		}
	}
	return leader
}
