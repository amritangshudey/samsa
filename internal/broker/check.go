package broker

import (
	"fmt"
	"time"

	"github.com/amritangshudey/samsa/internal/raft"
	"github.com/amritangshudey/samsa/internal/storage"
	"github.com/amritangshudey/samsa/internal/sys"
)

// CheckResult is what Check found in a data directory that is sound.
type CheckResult struct {
	LogStart, LastIndex int64 // the Raft log holds entries LogStart..LastIndex
	Snapshot            int64 // index of the snapshot, -1 if none
	Term                int64
	Topics, Groups      int
}

// Check reads a node's data the way a start does: the Raft log (segments,
// indexes, a torn tail), the snapshot, the install marker and the hard
// state. It then reads every entry in the log, which verifies each one's
// checksum, and replays every command after the snapshot into a scratch
// state, so a command that would stop the node on apply is found now. It
// does not decide what is committed: entries past the commit point are
// replayed too, as they would be if they commit.
//
// Check writes nothing itself, but recovery repairs (a torn tail is cut, a
// missing index rebuilt). The caller passes a disk that refuses or records
// writes to learn of those.
func Check(disk sys.Disk, cfg Config) (CheckResult, error) {
	res, _, _, err := load(disk, cfg)
	return res, err
}

// load is Check, returning also the loaded log and the replayed state.
func load(disk sys.Disk, cfg Config) (CheckResult, *raft.Raft, *state, error) {
	cfg = cfg.withDefaults()
	r := raft.New(raft.Config{
		// Two peers, so that a start does not campaign (which writes).
		ID: sys.NodeID(cfg.NodeID), Peers: []sys.NodeID{sys.NodeID(cfg.NodeID), sys.NodeID(cfg.NodeID) + 1}, Dir: cfg.Dir,
		Log: storage.Config{SegmentBytes: cfg.SegmentBytes, MaxPayloadBytes: cfg.MaxBatchBytes + commandOverhead},
	}, quietEnv{}, disk)
	if err := r.Start(); err != nil {
		return CheckResult{}, nil, nil, err
	}
	res := CheckResult{LogStart: r.LogStart(), LastIndex: r.LastIndex(), Snapshot: -1, Term: r.Term()}
	st, from := newState(), int64(0)
	if i, b := r.Snapshot(); i >= 0 {
		s, err := decodeState(b)
		if err != nil {
			return res, nil, nil, fmt.Errorf("snapshot at %d: %w", i, err)
		}
		st, from, res.Snapshot = s, i+1, i
	}
	for i := res.LogStart; i <= res.LastIndex; i++ {
		cmd, noop, err := r.Read(i)
		if err != nil {
			return res, nil, nil, fmt.Errorf("raft log entry %d: %w", i, err)
		}
		if i < from || noop {
			continue
		}
		if _, err := st.apply(i, cmd); err != nil {
			return res, nil, nil, fmt.Errorf("raft log entry %d: %w", i, err)
		}
	}
	res.Topics, res.Groups = len(st.topics), len(st.groups)
	return res, r, st, nil
}

// quietEnv is a raft.Env for a node that never runs: Check only loads.
type quietEnv struct{}

func (quietEnv) Send(sys.NodeID, []byte)            {}
func (quietEnv) SetTimer(raft.Timer, time.Duration) {}
func (quietEnv) CancelTimer(raft.Timer)             {}
func (quietEnv) Rand() uint64                       { return 0 }
func (quietEnv) Now() sys.Instant                   { return 0 }
