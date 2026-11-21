package sim

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"slices"
	"time"

	"github.com/amritangshudey/samsa/internal/broker"
	"github.com/amritangshudey/samsa/internal/raft"
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
	// Skip names faults, by their sequence number in Result.Faults, that
	// are drawn as usual but do not act. Used to shrink a failing seed.
	Skip map[int]bool
	// Timeline, if set, receives an HTML page of the run when it ends.
	Timeline io.Writer
	// Calm sets the drop and duplicate probabilities between nodes to 0.
	// Every draw is still made, so the rest of the run stays aligned.
	Calm bool
}

// Fault is one fault of a run, numbered in the order it happened.
type Fault struct {
	Seq     int
	At      time.Duration // virtual time since the run began
	What    string
	Skipped bool // drawn, but Config.Skip kept it from acting
}

// Result is the outcome of a run.
type Result struct {
	Seed uint64
	// Err is the first violation, or nil.
	Err error
	// Setup describes the configuration the seed chose.
	Setup string
	Stats Stats
	// Faults lists every fault drawn, skipped ones included.
	Faults []Fault
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
	faultStop                   // a graceful stop (SIGTERM): hand over, then exit
	faultNone                   // a fault that was drawn and then skipped
	faultRot                    // latent damage to a sealed segment of one node (rot.go)
)

// options is the swarm configuration: what kind of run the seed selected.
type options struct {
	faults       []fault
	diskCrash    bool // also crash inside recovery
	disconnects  bool
	faultEvery   time.Duration
	producers    int
	consumers    int
	vandals      int
	members      int
	cgMembers    int           // KIP-848 consumer group members (cg.go)
	sgMembers    int           // KIP-932 share group members (sg.go)
	txnProducers int           // transactional producers (txn.go)
	rcConsumers  int           // read_committed consumers (txn.go)
	tier         bool          // tiered object storage (ADR 0037)
	idempotent   int           // idempotent producers (idempotent.go)
	expiry       time.Duration // producer expiry as chosen, before any mutation
	classic      bool          // Raft without PreVote and CheckQuorum
	admins       int           // admin clients (admin.go)
	nodes        int
	// changes adds spares nodes that start with no configuration and a
	// client that adds and removes voters (voters.go, design C1).
	changes bool
	// retainMs is the broker's default retention.ms (0: none); t0 then
	// keeps everything, so the cleaner has live data to copy (design C2).
	retainMs int64
	// compact adds the compacted topic tc (compaction.go) with these
	// min.compaction.lag.ms and delete.retention.ms.
	rot                       bool // faultRot is among the faults (rot.go)
	compact                   bool
	compactLag, compactRetain int64
	// Between nodes (network.go): drop and duplicate probabilities per
	// message, and whether partition scenarios run.
	drop, dup  float64
	partitions bool
}

func (f fault) String() string {
	return [...]string{"power", "diskcrash", "kill", "syncfail", "stop", "none", "rot"}[f]
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
		skip: cfg.Skip,
	}
	if cfg.Timeline != nil {
		w.tl = newTimeline(0) // lanes are counted once the setup is known
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
		MinSessionTimeout:           50 * time.Millisecond,
		ConsumerHeartbeatInterval:   50 * time.Millisecond,
		ConsumerSessionTimeout:      300 * time.Millisecond,
		ShareAcquisitionLockTimeout: 500 * time.Millisecond,
		ShareDeliveryCountLimit:     20,
		Logger:                      nodeLogger(cfg.Trace),
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
	if w.opts.nodes > 1 && pick.Chance(0.6) { // drawn last, so older seeds keep their other choices
		w.opts.faults = append(w.opts.faults, faultStop)
	}
	if w.opts.classic = pick.Chance(0.5); w.opts.classic {
		// The paper's Raft: PreVote makes contested elections rare, and
		// the vote bugs need them to do harm (docs/design/b3-raft-hardening.md).
		w.bcfg.UnsafeRaft |= raft.MutNoPreVote | raft.MutNoCheckQuorum
	}
	w.opts.admins = pick.Intn(2)
	w.opts.changes = w.opts.nodes > 1 && pick.Chance(0.5)     // drawn last, so older seeds keep their other choices
	w.opts.retainMs = []int64{0, 0, 1000, 3000}[pick.Intn(4)] // last again
	if w.opts.retainMs > 0 {
		w.bcfg.RetentionTime = time.Duration(w.opts.retainMs) * time.Millisecond
		w.bcfg.RetentionSlack = []float64{1.5, 2}[pick.Intn(2)]
	}
	w.opts.compact = pick.Chance(0.5) // drawn last, so older seeds keep their other choices
	w.opts.compactLag = []int64{0, 0, 500, 2000}[pick.Intn(4)]
	w.opts.compactRetain = []int64{0, 1000, 3000}[pick.Intn(3)]
	// Rot needs a copy to repair from, and a scrub fast enough to find it.
	// Not together with membership changes: a configuration of two voters has
	// no minority to lose (design B1, section 7).
	if w.opts.rot = pick.Chance(0.5) && w.opts.nodes >= 3 && !w.opts.changes; w.opts.rot { // drawn last
		w.opts.faults = append(w.opts.faults, faultRot)
	}
	w.opts.cgMembers = pick.Intn(4) // drawn last
	w.opts.sgMembers = pick.Intn(4) // drawn last
	w.opts.txnProducers = pick.Intn(3)
	w.opts.rcConsumers = pick.Intn(2)
	w.opts.tier = pick.Chance(0.35) && !w.opts.rot && !w.opts.compact
	if cfg.Calm {
		w.opts.drop, w.opts.dup = 0, 0
	}
	if cfg.Mutate != nil {
		cfg.Mutate(&w.bcfg)
		w.opts.retainMs = w.bcfg.RetentionTime.Milliseconds()
		w.opts.compact = w.opts.compact || w.bcfg.UnsafeCompact != 0 // a compaction bug needs a compacted topic
		if w.bcfg.UnsafeAssignBeforeRevoke && w.opts.cgMembers < 2 {
			w.opts.cgMembers = 2
		}
		if w.bcfg.UnsafeDropShareLock && w.opts.sgMembers < 1 {
			w.opts.sgMembers = 1
		}
		if w.bcfg.UnsafeReadUncommittedInReadCommitted {
			if w.opts.txnProducers < 1 {
				w.opts.txnProducers = 1
			}
			if w.opts.rcConsumers < 1 {
				w.opts.rcConsumers = 1
			}
		}
		if w.bcfg.UnsafeDeleteBeforeUpload {
			w.opts.tier = true
			w.bcfg.SegmentBytes = 16384
			w.bcfg.ObjCacheBytes = 1
		}
	}
	if w.opts.tier {
		faults := simsys.ObjFaults{LostPut: 0.15, LostGet: 0.10, TornRead: 0.15, MaxDelay: 20 * time.Millisecond}
		if w.bcfg.UnsafeDeleteBeforeUpload {
			faults.LostPut = 0.5
		}
		w.objs = simsys.NewSharedObjects(faults)
	}
	strict := w.bcfg.Sync == broker.SyncAlways
	if w.opts.nodes > 1 && !strict {
		// A cluster whose nodes acknowledge before syncing has no defence
		// against a power cut: one node forgetting what it acknowledged
		// breaks what Raft is built on (ADR 0012). That combination is
		// not simulated; only process kills, which lose nothing.
		w.opts.diskCrash = false
		w.opts.faults = slices.DeleteFunc(w.opts.faults, func(f fault) bool { return f != faultKill && f != faultStop && f != faultRot })
	}
	if !strict {
		// shortcut: a retry re-sends records a power loss may have taken
		// after they were seen, which the model reports as I3; idempotent
		// producers run only where nothing seen can be lost, until the
		// model learns that a lost record may legitimately come back.
		w.opts.idempotent = 0
	}
	w.model = NewModel(strict)
	if w.bcfg.RetentionBytes > 0 || w.opts.admins > 0 || w.opts.retainMs > 0 { // DeleteRecords moves the start as retention does
		w.model.Trimming()
	}
	if w.opts.retainMs > 0 && w.bcfg.RetentionBytes == 0 && w.opts.admins == 0 {
		// Only topic retention moves a start: R9 holds every move to it.
		w.model.Retention(func(topic string) int64 {
			if topic == keptTopic || topic == compactTopic {
				return -1
			}
			return w.opts.retainMs
		}, func() int64 { return int64(w.now / 1e6) })
	}
	if w.opts.compact {
		w.model.Compaction(compactTopic, w.opts.compactLag, w.opts.compactRetain, func() int64 { return int64(w.now / 1e6) })
	}
	res := Result{Seed: cfg.Seed, Setup: fmt.Sprintf("nodes=%d sync=%d segment=%d election=%v faults=%v every=%v drop=%v dup=%v partitions=%v append=%d snapshot=%d retention=%d disconnects=%v producers=%d consumers=%d vandals=%d members=%d idempotent=%d expiry=%v classic=%v admins=%d changes=%v retain-ms=%d compact=%v rot=%v",
		w.opts.nodes, w.bcfg.Sync, w.bcfg.SegmentBytes, w.bcfg.ElectionTimeout, w.opts.faults, w.opts.faultEvery, w.opts.drop, w.opts.dup, w.opts.partitions, w.bcfg.MaxAppendBytes, w.bcfg.SnapshotEvery, w.bcfg.RetentionBytes, w.opts.disconnects, w.opts.producers, w.opts.consumers, w.opts.vandals, w.opts.members, w.opts.idempotent, w.opts.expiry, w.opts.classic, w.opts.admins, w.opts.changes, w.opts.retainMs, w.opts.compact, w.opts.rot)}
	if w.trace != nil {
		_, _ = fmt.Fprintf(w.trace, "seed %d: %s\n", cfg.Seed, res.Setup)
	}

	for id := int32(1); id <= int32(w.opts.nodes); id++ {
		w.bcfg.Peers = append(w.bcfg.Peers, broker.Peer{ID: id, Host: "sim", Port: 9091 + id, Rack: fmt.Sprintf("r%d", id%3)})
	}
	all := slices.Clone(w.bcfg.Peers)
	if w.opts.changes {
		for id := int32(w.opts.nodes) + 1; id <= int32(w.opts.nodes)+spares; id++ {
			all = append(all, broker.Peer{ID: id, Host: "sim", Port: 9091 + id})
		}
	}
	for _, p := range all {
		nd := &simNode{id: p.ID, cfg: w.bcfg, disk: simsys.NewDisk(diskRng.Fork(), simsys.DiskConfig{})}
		nd.cfg.NodeID, nd.cfg.Port = p.ID, p.Port
		nd.cfg.ScrubInterval = -1 // off, unless there is rot to find
		if w.opts.rot {
			nd.cfg.ScrubBytesPerSecond, nd.cfg.ScrubInterval = 1<<20, time.Second
		}
		nd.cfg.Join = p.ID > int32(w.opts.nodes) // a spare waits to be added
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
	}{{w.opts.producers, (*client).produce}, {w.opts.consumers, (*client).consume}, {w.opts.vandals, (*client).vandalise}, {w.opts.members, (*client).groupMember}, {w.opts.cgMembers, (*client).cgMember}, {w.opts.sgMembers, (*client).sgMember}, {w.opts.txnProducers, (*client).txnProduce}, {w.opts.rcConsumers, (*client).rcConsume}, {w.opts.idempotent, (*client).produceIdempotent}, {w.opts.admins, (*client).admin}, {voterClients(w.opts.changes), (*client).changeVoters}}
	for _, b := range behaviours {
		for range b.n {
			cl := &client{w: w, lane: len(w.clients), rng: rng.Fork(), step: b.step, cursors: map[TP]int64{}, idem: &idemProducer{id: -1}}
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
	if w.objs != nil {
		w.objs.Faults = simsys.ObjFaults{}
	}
	for _, nd := range w.nodes {
		nd.disk.CrashAfter(0)
		nd.disk.SetSyncErrorRate(0)
	}
	w.runUntil(w.now+sys.Instant(2*answerBound), nil)
	if w.err == nil {
		w.finalChecks()
	}
	if w.objs != nil {
		w.stats.TieredPuts = int(w.objs.Puts)
		w.stats.TieredGets = int(w.objs.Gets)
	}
	var maxTerm int64
	for _, nd := range w.nodes {
		maxTerm = max(maxTerm, nd.term)
	}
	w.stats.TermsWithoutLeader = int(maxTerm) - len(w.leaders)
	res.Err, res.Stats, res.TraceHash, res.Faults = w.err, w.stats, w.hash, w.faults
	if w.tl != nil {
		w.tl.nodes, w.tl.clients, w.tl.end = len(w.nodes), len(w.clients), w.now
		if err := w.tl.write(cfg.Timeline, fmt.Sprintf("Seed %d", cfg.Seed), res.Setup, w.faults); err != nil && res.Err == nil {
			res.Err = fmt.Errorf("write timeline: %w", err)
		}
	}
	return res
}

// finalChecks reads every partition from the start (the whole-history half
// of I1 to I5) and then checks that recovery is repeatable (I6).
func (w *world) finalChecks() {
	leader := w.settled()
	if leader == nil || !w.writable(leader) {
		return
	}
	// U1: with every node on one build, the cluster reaches its format
	// level once settled (design G7: reports every second, fresh for 3 s).
	atLevel := func() bool { c, b := leader.inc.n.Level(); return c == b }
	if w.runUntil(w.now+sys.Instant(5*time.Second), atLevel); w.err != nil {
		return
	}
	if c, b := leader.inc.n.Level(); c != b {
		w.fail("U1: the settled cluster is at format level %d, below its build's %d", c, b)
		return
	}
	var tps []TP
	for _, t := range catalogue {
		for p := range t.max {
			tps = append(tps, TP{t.name, p})
		}
	}
	tps = append(tps, TP{doomedTopic, 0})
	if w.opts.compact {
		tps = append(tps, TP{compactTopic, 0})
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
		f := Fetch{TP: tp, Offset: offset, Floor: w.model.Floor(tp), Epoch: w.model.Epoch(), Known: w.model.Known(tp), Start: w.model.Start(tp), Deleted: w.model.Deleted(tp.Topic)}
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
	w.drainShareGroup(leader)
	if err := w.model.CheckShareFinal(); err != nil {
		w.fail("%v", err)
		return
	}

	// I6. A kill loses nothing, so a restart must rebuild the same state.
	// Under the strict policy the same holds across a power loss, since
	// everything applied was synced first. Every node is restarted at
	// once; the cluster must come back to the state it had.
	// The leader proposes on its own (retention, the cleaner): wait until
	// nothing it proposed is still unapplied, or it would apply after the
	// restart and change the state. Nodes go down in this same instant.
	w.runUntil(w.now+sys.Instant(10*answerBound), func() bool {
		m := leader.inc.n.Metrics()
		return m.Applied == m.LastIndex+1
	})
	_, hash := leader.inc.n.StateHash()
	w.stats.ProducersExpired += leader.inc.n.ExpiredProducers()
	w.stats.BytesRelocated += int(leader.inc.n.Metrics().RelocatedBytes)
	w.stats.RecordsCompacted += int(leader.inc.n.Metrics().CompactedRecords)
	for _, nd := range w.nodes {
		if nd.inc != nil {
			w.stats.ScrubbedBytes += int(nd.inc.n.Metrics().Scrubbed)
		}
	}
	// A restarted leader may legitimately trim again; I6 is about replay.
	for _, nd := range w.nodes {
		nd.cfg.RetentionBytes, nd.cfg.RetentionTime = 0, 0
		nd.cfg.RetentionSlack = math.Inf(1) // and may clean again
		nd.cfg.CompactionOff = true
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
	create := &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{{Name: tp.Topic, NumPartitions: catalogue[0].parts, ReplicationFactor: w.replication()}}}
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

// settleBound is how long settled waits. Catching up is the slow part: a
// node that lost its whole log to damage, in a run with append=1 (one entry
// per AppendEntries), refetches at one entry per round trip, which took
// longer than answerBound in nightly seeds (BUGS.md M24).
// shortcut: a fixed bound, not progress-aware; if a seed still catches up
// too slowly, wait while the slowest node's log keeps growing.
const settleBound = 6 * answerBound

// settled waits, with no faults and no clients, until every node is up,
// one of them serves as leader, and all have applied the same entries. It
// then checks that they agree (S2 on whole logs, S4) and returns the
// leader; or it records a violation and returns nil.
func (w *world) settled() *simNode {
	var leader *simNode
	w.findRot()
	agree := func() bool {
		leader = nil
		if w.rot.state == rotFound || w.rot.state == rotRecovered {
			return false // a node is repairing: wait until it holds what it lost
		}
		for _, nd := range w.nodes {
			if nd.inc == nil || nd.inc.n.Raft().Recovering() {
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
		for _, nd := range w.voters(leader) {
			if a, _ := nd.inc.n.StateHash(); a != last+1 || nd.inc.n.Raft().LastIndex() != last {
				return false
			}
		}
		return true
	}
	w.runUntil(w.now+sys.Instant(settleBound), agree)
	if w.err != nil {
		return nil
	}
	if !agree() {
		var state []string
		for _, nd := range w.nodes {
			if nd.inc == nil {
				state = append(state, fmt.Sprintf("node %d down", nd.id))
				continue
			}
			r := nd.inc.n.Raft()
			state = append(state, fmt.Sprintf("node %d %v term %d last %d commit %d recovering %v serving %v", nd.id, r.Role(), r.Term(), r.LastIndex(), r.Committed(), r.Recovering(), nd.inc.n.Serving()))
		}
		w.fail("with faults stopped for %v the nodes did not all come back, elect a leader and catch up (rot %d): %v", settleBound, w.rot.state, state)
		return nil
	}
	lr := leader.inc.n.Raft()
	_, want := leader.inc.n.StateHash()
	for _, nd := range w.voters(leader) {
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
