package raft

import "testing"

// termIndex answers like a plain slice for every index, through appends,
// truncations, trims and rebuilds, and keeps only the tail after a trim.
func TestTermIndex(t *testing.T) {
	var ref []int64
	var ti termIndex
	check := func(step string) {
		t.Helper()
		if ti.last() != int64(len(ref))-1 {
			t.Fatalf("%s: last %d, want %d", step, ti.last(), len(ref)-1)
		}
		for i, want := range ref {
			if got := ti.at(int64(i)); got != want {
				t.Fatalf("%s: at(%d) = %d, want %d", step, i, got, want)
			}
		}
		if n := int64(len(ref)) - 1; n >= 0 {
			if got := termsOf(ti.runsTo(n), n, 0).tail; len(got) != len(ref) {
				t.Fatalf("%s: runsTo lost entries", step)
			}
			if cut := termsOf(ti.runsTo(n), n/2, n/3); cut.last() != n/2 {
				t.Fatalf("%s: cut at %d has last %d", step, n/2, cut.last())
			}
			re := termsOf(ti.runsTo(n), n, n/2)
			for i, want := range ref {
				if re.at(int64(i)) != want {
					t.Fatalf("%s: rebuilt at(%d) = %d, want %d", step, i, re.at(int64(i)), want)
				}
			}
		}
	}
	term := int64(1)
	for i := range 10000 {
		if i%37 == 0 {
			term++
		}
		ref = append(ref, term)
		ti.append(term)
		if i%500 == 499 {
			ti.truncate(int64(len(ref) - 3))
			ref = ref[:len(ref)-3]
			ti.trim(int64(len(ref) - 100))
			check("trim")
		}
	}
	if len(ti.tail) > 600 || cap(ti.tail) > 4096 {
		t.Fatalf("tail holds %d (cap %d): trimmed entries were not released", len(ti.tail), cap(ti.tail))
	}
	if len(ti.below) > len(ref)/37+2 {
		t.Fatalf("%d runs for %d terms", len(ti.below), term)
	}
}
