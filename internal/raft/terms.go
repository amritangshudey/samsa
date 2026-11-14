package raft

import (
	"slices"
	"sort"
)

// termIndex holds the term of every entry 0..last. Entries below from are
// kept as runs, one per change of term; from on, one term per entry. The
// node moves from up to its log start as the log is cut (trim), so memory
// is bounded by the log it keeps plus the number of terms, instead of one
// int64 for every entry ever written (found by the 24 h soak, BUGS.md L1).
type termIndex struct {
	below []termRun // terms of entries 0..from-1, ascending by first
	from  int64     // index of tail[0]
	tail  []int64
}

// termsOf builds the index for entries 0..index of runs (which may go on
// past index), with one term per entry from keep on.
func termsOf(runs []termRun, index, keep int64) termIndex {
	keep = min(max(keep, 0), index+1)
	t := termIndex{from: keep}
	for i, r := range runs {
		end := index + 1
		if i+1 < len(runs) {
			end = min(end, runs[i+1].first)
		}
		if r.first > index {
			break
		}
		if r.first < keep {
			t.below = append(t.below, r)
		}
		for j := max(r.first, keep); j < end; j++ {
			t.tail = append(t.tail, r.term)
		}
	}
	return t
}

// last returns the index of the last entry, or -1.
func (t *termIndex) last() int64 { return t.from + int64(len(t.tail)) - 1 }

// at returns the term of entry i, which must exist.
func (t *termIndex) at(i int64) int64 {
	if i >= t.from {
		return t.tail[i-t.from]
	}
	// The last run that starts at or before i.
	k := sort.Search(len(t.below), func(k int) bool { return t.below[k].first > i }) - 1
	return t.below[k].term
}

// lastTerm returns the term of the last entry, or 0 if there is none.
func (t *termIndex) lastTerm() int64 {
	if n := t.last(); n >= 0 {
		return t.at(n)
	}
	return 0
}

func (t *termIndex) append(term int64) { t.tail = append(t.tail, term) }

// truncate keeps entries below n. Raft never truncates committed entries,
// and from is at most the commit index, so n > from.
func (t *termIndex) truncate(n int64) { t.tail = t.tail[:n-t.from] }

// runsTo returns the runs of entries 0..index.
func (t *termIndex) runsTo(index int64) []termRun {
	var runs []termRun
	for _, r := range t.below {
		if r.first <= index {
			runs = append(runs, r)
		}
	}
	for i := t.from; i <= index && i <= t.last(); i++ {
		if term := t.at(i); len(runs) == 0 || runs[len(runs)-1].term != term {
			runs = append(runs, termRun{i, term})
		}
	}
	return runs
}

// trim moves entries below keep into runs. The tail is copied once it has
// shed half its capacity, so that the memory is actually returned.
func (t *termIndex) trim(keep int64) {
	keep = min(keep, t.last()+1)
	if keep <= t.from {
		return
	}
	for i := t.from; i < keep; i++ {
		if term := t.tail[i-t.from]; len(t.below) == 0 || t.below[len(t.below)-1].term != term {
			t.below = append(t.below, termRun{i, term})
		}
	}
	t.tail = t.tail[keep-t.from:]
	t.from = keep
	if len(t.tail) < cap(t.tail)/2 {
		t.tail = slices.Clone(t.tail)
	}
}
