package sim

import (
	"strings"
	"testing"

	"github.com/amritangshudey/samsa/internal/broker"
	"github.com/amritangshudey/samsa/internal/wire"
)

// TestGroupModel feeds the group checker hand-made histories, as TestModel
// does for the log: every bad one must be refused for the stated reason.
func TestGroupModel(t *testing.T) {
	tp := TP{"t", 0}
	assign := []wire.MemberAssignment{{MemberID: "a", Assignment: []byte{0, 2}}, {MemberID: "b", Assignment: []byte{1}}}
	// commit sends a commit and reports its response.
	commit := func(m *Model, code int16, gen int32) (int64, error) {
		off, floor, epoch := m.CommitSend("g", tp), m.GenFloor("g"), m.Epoch()
		return off, m.CommitDone(off, code, gen, floor, epoch)
	}
	fetched := func(m *Model, offset int64) error {
		return m.OffsetFetched("g", tp, offset, m.CommitFloor("g", tp), m.Epoch())
	}
	tests := []struct {
		name   string
		strict bool
		run    func(m *Model) error
		want   string // substring of the error; "" means the history is fine
	}{
		{"G1: everyone receives what the leader sent", true, func(m *Model) error {
			m.AssignmentSent("g", 1, assign)
			if err := m.AssignmentReceived("g", 1, "a", []byte{0, 2}, 0); err != nil {
				return err
			}
			return m.AssignmentReceived("g", 1, "b", []byte{1}, 0)
		}, ""},
		{"G1: a member receives something else", true, func(m *Model) error {
			m.AssignmentSent("g", 1, assign)
			return m.AssignmentReceived("g", 1, "b", []byte{2}, 0)
		}, "G1: member b"},
		{"G1: a slot given to two members", true, func(m *Model) error {
			_ = m.AssignmentReceived("g", 1, "a", []byte{0, 2}, 0)
			return m.AssignmentReceived("g", 1, "b", []byte{2}, 0)
		}, "G1: slot 2"},
		{"G1: the same slot in another generation", true, func(m *Model) error {
			_ = m.AssignmentReceived("g", 1, "a", []byte{0}, 0)
			return m.AssignmentReceived("g", 2, "b", []byte{0}, 0)
		}, ""},

		{"G2: generations move forward", true, func(m *Model) error {
			_ = m.Joined("g", 1, "a", "p", 0, 0)
			return m.Joined("g", 2, "a", "p", m.GenFloor("g"), 0)
		}, ""},
		{"G2: a generation older than one already seen", true, func(m *Model) error {
			_ = m.Joined("g", 2, "a", "p", 0, 0)
			return m.Joined("g", 1, "a", "p", m.GenFloor("g"), 0)
		}, "G2: group g answered a join with generation 1"},
		{"G2: an older generation, but the request was sent before the newer was seen", true, func(m *Model) error {
			floor := m.GenFloor("g")
			_ = m.Joined("g", 2, "a", "p", 0, 0)
			return m.Joined("g", 1, "a", "p", floor, 0)
		}, ""},
		{"G2: two leaders in one generation", true, func(m *Model) error {
			_ = m.Joined("g", 1, "a", "p", 0, 0)
			return m.Joined("g", 1, "b", "p", 0, 0)
		}, "told different things"},
		{"G2: generations rolled back by a permitted power loss", false, func(m *Model) error {
			_ = m.Joined("g", 2, "a", "p", 0, 0)
			m.PowerLoss()
			return m.Joined("g", 1, "b", "p", m.GenFloor("g"), m.Epoch())
		}, ""},

		{"G3: the acknowledged commit is returned", true, func(m *Model) error {
			off, _ := commit(m, wire.ErrNone, 1)
			return fetched(m, off)
		}, ""},
		{"G3: nothing returned after an acknowledged commit", true, func(m *Model) error {
			_, _ = commit(m, wire.ErrNone, 1)
			return fetched(m, -1)
		}, "no committed offset"},
		{"G3: an offset nobody committed", true, func(m *Model) error {
			return fetched(m, 77)
		}, "nobody committed"},
		{"G3: an offset committed to another partition", true, func(m *Model) error {
			off := m.CommitSend("g", TP{"t", 1})
			return fetched(m, off)
		}, "nobody committed"},
		{"G3: a refused commit is returned", true, func(m *Model) error {
			off, _ := commit(m, wire.ErrIllegalGeneration, 1)
			return fetched(m, off)
		}, "the broker refused"},
		{"G3: a replaced commit is returned", true, func(m *Model) error {
			old, _ := commit(m, wire.ErrNone, 1)
			_, _ = commit(m, wire.ErrNone, 1)
			return fetched(m, old)
		}, "had replaced"},
		{"G3: either of two concurrent commits may be returned", true, func(m *Model) error {
			first, second := m.CommitSend("g", tp), m.CommitSend("g", tp)
			_ = m.CommitDone(second, wire.ErrNone, 1, 0, 0)
			_ = m.CommitDone(first, wire.ErrNone, 1, 0, 0)
			if err := fetched(m, first); err != nil {
				return err
			}
			return fetched(m, second)
		}, ""},
		{"G3: a commit with no answer may have happened", true, func(m *Model) error {
			_, _ = commit(m, wire.ErrNone, 1)
			return fetched(m, m.CommitSend("g", tp))
		}, ""},
		{"G6: an acknowledged commit lost in a power loss, strict policy", true, func(m *Model) error {
			_, _ = commit(m, wire.ErrNone, 1)
			m.PowerLoss()
			return fetched(m, -1)
		}, "no committed offset"},
		{"G6: the same under a policy that permits it", false, func(m *Model) error {
			_, _ = commit(m, wire.ErrNone, 1)
			m.PowerLoss()
			return fetched(m, -1)
		}, ""},

		{"G4: a commit from the current generation", true, func(m *Model) error {
			_ = m.Joined("g", 2, "a", "p", 0, 0)
			_, err := commit(m, wire.ErrNone, 2)
			return err
		}, ""},
		{"G4: a zombie's commit is refused", true, func(m *Model) error {
			_ = m.Joined("g", 2, "a", "p", 0, 0)
			_, err := commit(m, wire.ErrIllegalGeneration, 1)
			return err
		}, ""},
		{"G4: a zombie's commit is accepted", true, func(m *Model) error {
			_ = m.Joined("g", 2, "a", "p", 0, 0)
			_, err := commit(m, wire.ErrNone, 1)
			return err
		}, "G4: group g accepted a commit from generation 1"},
	}
	for _, tc := range tests {
		err := tc.run(NewModel(tc.strict))
		switch {
		case tc.want == "" && err != nil:
			t.Errorf("%s: unexpected violation: %v", tc.name, err)
		case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
			t.Errorf("%s: got %v, want a violation containing %q", tc.name, err, tc.want)
		}
	}
}

// TestSimCatchesMissingFencing removes the generation check on offset
// commits and requires the simulator to notice (G4).
func TestSimCatchesMissingFencing(t *testing.T) {
	caught := 0
	results := runMany(1, 200, func(c *broker.Config) { c.UnsafeNoFencing = true })
	for _, res := range results {
		if res.Err != nil {
			if caught++; caught == 1 {
				t.Logf("seed %d: %v", res.Seed, res.Err)
			}
			if !strings.Contains(res.Err.Error(), "G4") {
				t.Errorf("seed %d failed for another reason: %v", res.Seed, res.Err)
			}
		}
	}
	t.Logf("broker without fencing caught by %d of %d seeds", caught, len(results))
	if caught == 0 {
		t.Fatal("no seed noticed a broker that accepts commits from old generations")
	}
}
