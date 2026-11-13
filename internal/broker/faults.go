package broker

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/amritangshudey/samsa/internal/wire"
)

// Faults are Kafka-level faults a node injects into what it answers
// clients, so that an application can be tested against them
// (docs/design/a5b-app-faults.md). The zero value injects nothing and
// draws nothing.
type Faults struct {
	// Seed starts the faults' own random stream; it is never the node's
	// Random, so a run with faults off is the run it always was.
	Seed uint64
	// Rate is the chance, per eligible request, that a fault is injected.
	Rate float64
	// Kinds are the faults that may be injected, sorted, from FaultKinds.
	Kinds []string
}

// The fault kinds. Each is eligible on certain requests only.
const (
	// FaultLostAck commits a produce request and then closes the
	// connection instead of answering: the write happened, the client
	// does not know. A retry writes it again unless the producer is
	// idempotent.
	FaultLostAck = "lostack"
	// FaultRebalance answers a heartbeat of a stable group with
	// REBALANCE_IN_PROGRESS and starts a rebalance: every member must
	// rejoin, and its partitions are revoked in the middle of whatever
	// it was doing.
	FaultRebalance = "rebalance"
	// FaultNotLeader answers a produce, fetch or offset commit with
	// NOT_LEADER_OR_FOLLOWER (NOT_COORDINATOR for the commit) without
	// acting on it, as a node does during a leader change.
	FaultNotLeader = "notleader"
)

// FaultKinds is every fault kind, sorted.
var FaultKinds = []string{FaultLostAck, FaultNotLeader, FaultRebalance}

// DefaultFaultRate is the rate of a spec that names none.
const DefaultFaultRate = 0.05

// ParseFaults reads a spec of comma-separated key=value pairs:
// seed=N (required), rate=R (0 < R <= 1, default 0.05) and
// kinds=a+b (default all of FaultKinds).
func ParseFaults(spec string) (Faults, error) {
	f := Faults{Rate: DefaultFaultRate, Kinds: FaultKinds}
	seen := map[string]bool{}
	for part := range strings.SplitSeq(spec, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return Faults{}, fmt.Errorf("faults %q: %q is not key=value", spec, part)
		}
		if seen[k] {
			return Faults{}, fmt.Errorf("faults %q: %s given twice", spec, k)
		}
		seen[k] = true
		switch k {
		case "seed":
			s, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return Faults{}, fmt.Errorf("faults %q: seed: %w", spec, err)
			}
			f.Seed = s
		case "rate":
			r, err := strconv.ParseFloat(v, 64)
			if err != nil || !(r > 0 && r <= 1) {
				return Faults{}, fmt.Errorf("faults %q: rate must be a number in (0, 1]", spec)
			}
			f.Rate = r
		case "kinds":
			f.Kinds = nil
			for kind := range strings.SplitSeq(v, "+") {
				if !slices.Contains(FaultKinds, kind) {
					return Faults{}, fmt.Errorf("faults %q: unknown kind %q (known: %s)", spec, kind, strings.Join(FaultKinds, ", "))
				}
				if !slices.Contains(f.Kinds, kind) {
					f.Kinds = append(f.Kinds, kind)
				}
			}
			slices.Sort(f.Kinds)
		default:
			return Faults{}, fmt.Errorf("faults %q: unknown key %q (known: seed, rate, kinds)", spec, k)
		}
	}
	if !seen["seed"] {
		return Faults{}, fmt.Errorf("faults %q: seed is required, so that the run can be repeated", spec)
	}
	return f, nil
}

// String is the spec that ParseFaults reads back to f.
func (f Faults) String() string {
	return fmt.Sprintf("seed=%d,rate=%s,kinds=%s", f.Seed, strconv.FormatFloat(f.Rate, 'g', -1, 64), strings.Join(f.Kinds, "+"))
}

// faulter draws the faults of one node: a splitmix64 stream of its own.
// Each eligible request draws exactly once, whatever the outcome, so the
// faults depend only on the seed and the order of eligible requests.
type faulter struct {
	cfg   Faults
	state uint64
	count int64 // faults injected so far
}

// inject reports whether a fault of kind hits the eligible request that
// is being handled now. It draws only if kind is enabled.
func (f *faulter) inject(kind string) bool {
	if f.cfg.Rate <= 0 || !slices.Contains(f.cfg.Kinds, kind) {
		return false
	}
	f.state += 0x9e3779b97f4a7c15
	z := f.state
	z = (z ^ z>>30) * 0xbf58476d1ce4e5b9
	z = (z ^ z>>27) * 0x94d049bb133111eb
	z ^= z >> 31
	if float64(z>>11)/(1<<53) >= f.cfg.Rate {
		return false
	}
	f.count++
	return true
}

// notLeaderFault returns the answer of FaultNotLeader to req, or nil if
// req is not one it applies to.
func notLeaderFault(req wire.Message) wire.Message {
	switch req.(type) {
	case *wire.ProduceRequest, *wire.FetchRequest, *wire.OffsetCommitRequest:
		return refusal(req)
	}
	return nil
}
