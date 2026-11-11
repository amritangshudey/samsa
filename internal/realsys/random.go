package realsys

import (
	"crypto/rand"
	"encoding/binary"
)

// Random is the real sys.Random: the operating system's generator.
type Random struct{}

// Uint64 implements sys.Random.
func (Random) Uint64() uint64 {
	var b [8]byte
	rand.Read(b[:]) //nolint:errcheck // documented never to fail
	return binary.BigEndian.Uint64(b[:])
}
