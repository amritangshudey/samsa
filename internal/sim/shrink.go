package sim

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Shrunk is a failing run cut down to the faults it needs.
type Shrunk struct {
	Config Config // the seed, with Calm and Skip as found
	Result Result // the run of Config: it fails with the same rule
	Faults int    // faults acting in the original run
	Runs   int    // runs the search took
}

// rule names a violation for comparing runs: its invariant tag ("I1",
// "S6", "I1/I3"), or else its message up to the first digit.
func rule(err error) string {
	msg := err.Error()
	if tag, _, ok := strings.Cut(msg, ":"); ok && isTag(tag) {
		return tag
	}
	if i := strings.IndexAny(msg, "0123456789"); i >= 0 {
		msg = msg[:i]
	}
	return msg
}

// isTag reports whether s is like "I1" or "I1/I3": letters then digits,
// joined by slashes.
func isTag(s string) bool {
	for _, part := range strings.Split(s, "/") {
		letters := strings.TrimLeft(part, "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
		if letters == part || letters == "" || strings.Trim(letters, "0123456789") != "" {
			return false
		}
	}
	return true
}

// acting returns the sequence numbers of the faults that acted in res.
func acting(res Result) []int {
	var seqs []int
	for _, f := range res.Faults {
		if !f.Skipped {
			seqs = append(seqs, f.Seq)
		}
	}
	return seqs
}

// Shrink cuts a failing seed down (docs/design/a3-sim-tools.md, 2.1): calm
// network, then delta debugging over its faults, keeping a candidate only
// if it fails by the same rule. It stops after budget runs with the best
// found so far.
func Shrink(cfg Config, budget int) (Shrunk, error) {
	cfg.Trace = nil
	best := Run(cfg)
	if best.Err == nil {
		return Shrunk{}, errors.New("the seed does not fail")
	}
	s := Shrunk{Config: cfg, Result: best, Faults: len(acting(best)), Runs: 1}
	want := rule(best.Err)
	try := func(c Config) bool {
		s.Runs++
		r := Run(c)
		if r.Err == nil || rule(r.Err) != want {
			return false
		}
		s.Config, s.Result = c, r
		return true
	}
	if !cfg.Calm {
		c := cfg
		c.Calm = true
		try(c)
	}
	// Delta debugging: remove chunks of the acting faults, finer and
	// finer, until no single fault can go.
	for n := 2; s.Runs < budget; {
		active := acting(s.Result)
		if len(active) == 0 {
			break
		}
		n = min(n, len(active))
		removed := false
		for i := 0; i < n && s.Runs < budget; i++ {
			chunk := active[i*len(active)/n : (i+1)*len(active)/n]
			c := s.Config
			c.Skip = map[int]bool{}
			for seq := range s.Config.Skip {
				c.Skip[seq] = true
			}
			for _, seq := range chunk {
				c.Skip[seq] = true
			}
			if try(c) {
				removed = true
				n = max(n-1, 2)
				break
			}
		}
		if !removed {
			if n == len(active) {
				break // no single fault can be removed
			}
			n = min(2*n, len(active))
		}
	}
	return s, nil
}

// String is the shrunk schedule, for a person to read.
func (s Shrunk) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "seed %d, calm=%v: %s\n", s.Config.Seed, s.Config.Calm, s.Result.Setup)
	left := acting(s.Result)
	fmt.Fprintf(&b, "%d of %d faults left after %d runs:\n", len(left), s.Faults, s.Runs)
	for _, f := range s.Result.Faults {
		if !f.Skipped {
			fmt.Fprintf(&b, "  %9.3fs  #%d %s\n", f.At.Seconds(), f.Seq, f.What)
		}
	}
	fmt.Fprintf(&b, "violation: %v\nreplay: make sim SEED=%d%s", s.Result.Err, s.Config.Seed, ReplayArgs(s.Config))
	return b.String()
}

// ReplayArgs gives the make arguments beyond SEED that reproduce cfg.
func ReplayArgs(cfg Config) string {
	var b strings.Builder
	if cfg.Calm {
		b.WriteString(" CALM=1")
	}
	if len(cfg.Skip) > 0 {
		b.WriteString(" SKIP=" + FormatSkip(cfg.Skip))
	}
	return b.String()
}

// FormatSkip writes a skip set as ranges: "1-4,7".
func FormatSkip(skip map[int]bool) string {
	seqs := make([]int, 0, len(skip))
	for seq := range skip {
		seqs = append(seqs, seq)
	}
	slices.Sort(seqs)
	var parts []string
	for i := 0; i < len(seqs); {
		j := i
		for j+1 < len(seqs) && seqs[j+1] == seqs[j]+1 {
			j++
		}
		if i == j {
			parts = append(parts, strconv.Itoa(seqs[i]))
		} else {
			parts = append(parts, fmt.Sprintf("%d-%d", seqs[i], seqs[j]))
		}
		i = j + 1
	}
	return strings.Join(parts, ",")
}

// ParseSkip reads what FormatSkip writes.
func ParseSkip(text string) (map[int]bool, error) {
	skip := map[int]bool{}
	for _, part := range strings.Split(text, ",") {
		if part == "" {
			continue
		}
		lo, hi, isRange := strings.Cut(part, "-")
		a, err := strconv.Atoi(lo)
		b := a
		if err == nil && isRange {
			b, err = strconv.Atoi(hi)
		}
		if err != nil || a < 1 || b < a || b-a > 1<<20 {
			return nil, fmt.Errorf("skip: bad range %q", part)
		}
		for i := a; i <= b; i++ {
			skip[i] = true
		}
	}
	return skip, nil
}
