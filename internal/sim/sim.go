package sim

import (
	"fmt"
	"io"
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
		conns: map[sys.ConnID]*conn{},
	}
	w.disk = simsys.NewDisk(rng.Fork(), simsys.DiskConfig{})

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
	w.opts.faultEvery = []time.Duration{100 * time.Millisecond, 500 * time.Millisecond, 2 * time.Second}[pick.Intn(3)]
	w.opts.producers, w.opts.consumers, w.opts.vandals = 1+pick.Intn(3), 1+pick.Intn(3), pick.Intn(2)
	w.opts.members = pick.Intn(5)
	if cfg.Mutate != nil {
		cfg.Mutate(&w.bcfg)
	}
	w.model = NewModel(w.bcfg.Sync == broker.SyncAlways)
	res := Result{Seed: cfg.Seed, Setup: fmt.Sprintf("sync=%d segment=%d faults=%v every=%v disconnects=%v producers=%d consumers=%d vandals=%d members=%d",
		w.bcfg.Sync, w.bcfg.SegmentBytes, w.opts.faults, w.opts.faultEvery, w.opts.disconnects, w.opts.producers, w.opts.consumers, w.opts.vandals, w.opts.members)}
	if w.trace != nil {
		_, _ = fmt.Fprintf(w.trace, "seed %d: %s\n", cfg.Seed, res.Setup)
	}

	w.start()
	behaviours := []struct {
		n    int
		step func(*client)
	}{{w.opts.producers, (*client).produce}, {w.opts.consumers, (*client).consume}, {w.opts.vandals, (*client).vandalise}, {w.opts.members, (*client).groupMember}}
	for _, b := range behaviours {
		for range b.n {
			cl := &client{w: w, rng: rng.Fork(), step: b.step, cursors: map[TP]int64{}}
			// Timeouts well under answerBound: a held JoinGroup or
			// SyncGroup is answered within the rebalance timeout.
			cl.mem = &membership{group: groupNames[cl.rng.Intn(len(groupNames))], session: int32(100 + cl.rng.Intn(1900)), rebalance: int32(100 + cl.rng.Intn(2900))}
			w.clients = append(w.clients, cl)
			w.after(time.Duration(cl.rng.Intn(int(50*time.Millisecond))), cl.tick)
		}
	}
	w.after(w.opts.faultEvery, w.chaos)
	w.runUntil(w.now+sys.Instant(cfg.Duration), nil)

	// Quiesce: no new faults or requests. Let the node come back and every
	// outstanding request finish or time out, then judge the final state.
	w.quiet = true
	w.disk.CrashAfter(0)
	w.disk.SetSyncErrorRate(0)
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
	if w.node == nil {
		w.fail("the node did not come back after faults stopped")
		return
	}
	var tps []TP
	for _, t := range catalogue {
		for p := range t.parts {
			tps = append(tps, TP{t.name, p})
		}
	}
	w.model.BeginFinal()
	v := &client{w: w, rng: w.rng.Fork()}
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
		f := Fetch{TP: tp, Offset: offset, Floor: w.model.Floor(tp), Epoch: w.model.Epoch(), Known: w.model.Known(tp)}
		req := &wire.FetchRequest{MaxBytes: 1 << 20, Topics: []wire.FetchTopic{{Name: tp.Topic, Partitions: []wire.FetchPartition{
			{Index: tp.Part, Offset: offset, MaxBytes: 1 << 20},
		}}}}
		v.fetch(req, []Fetch{f}, func(_ Fetch, next int64) {
			if next >= 0 {
				read(i, next)
			} else {
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
	// everything applied was synced first.
	applied, hash := w.node.n.StateHash()
	for _, power := range []bool{false, true} {
		if power && !w.model.strict {
			break
		}
		w.node.dead = true
		if power {
			w.disk.Restart()
		}
		inc := &incarnation{w: w, timers: map[sys.TimerID]uint64{}}
		inc.n = broker.New(w.bcfg, inc, inc, w.disk, w.rng.Fork())
		if err := inc.n.OnStart(); err != nil {
			w.fail("I6: restart on a quiet disk failed: %v", err)
			return
		}
		w.node = inc
		// Each start elects the node again, which adds one no-op entry
		// to the log and nothing to the state.
		applied++
		if a, h := inc.n.StateHash(); a != applied || h != hash {
			w.fail("I6: after a restart (power loss: %v) the node applied %d entries with state hash %08x; expected %d with %08x", power, a, h, applied, hash)
			return
		}
	}
}
