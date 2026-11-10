// Package simsys holds the simulated implementations of the sys interfaces:
// everything in it is deterministic given a seed.
package simsys

// Rand is a SplitMix64 pseudo-random generator.
//
// It is written out by hand, rather than taken from math/rand, so that a
// seed means the same execution on every Go version and so that the core
// never needs a random-number import.
type Rand struct{ state uint64 }

// NewRand returns a generator whose sequence is determined by seed.
func NewRand(seed uint64) *Rand { return &Rand{state: seed} }

// Uint64 returns the next value. It implements sys.Random.
func (r *Rand) Uint64() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// Intn returns a value in [0, n). It panics if n <= 0.
func (r *Rand) Intn(n int) int {
	if n <= 0 {
		panic("simsys: Intn of non-positive n")
	}
	// shortcut: modulo bias is at most n/2^64, irrelevant for test scheduling.
	return int(r.Uint64() % uint64(n))
}

// Chance reports true with probability p.
func (r *Rand) Chance(p float64) bool {
	return float64(r.Uint64()>>11)/(1<<53) < p
}

// Fork returns a new generator seeded from this one, so that independent
// parts of a simulation do not disturb each other's sequences.
func (r *Rand) Fork() *Rand { return NewRand(r.Uint64()) }
