package simsys

import (
	"errors"
	"fmt"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
)

// ObjFaults controls fault injection on a simulated ObjectStore.
type ObjFaults struct {
	// LostPut / LostGet are probabilities in [0, 1] of a failed Put or Get.
	LostPut float64
	LostGet float64
	// TornRead is the probability in [0, 1] that a Get returns truncated or
	// bit-flipped bytes (caught by CRC32C in the broker).
	TornRead float64
	// MaxDelay is the maximum artificial latency for slow operations.
	MaxDelay time.Duration
}

// SharedObjects is the backing store shared by all nodes in a simulated cluster.
type SharedObjects struct {
	data   map[string][]byte
	Puts   int64
	Gets   int64
	Dels   int64
	Bytes  int64 // live bytes currently stored
	Faults ObjFaults
}

// NewSharedObjects returns an empty shared object store for a simulated cluster.
func NewSharedObjects(faults ObjFaults) *SharedObjects {
	return &SharedObjects{
		data:   map[string][]byte{},
		Faults: faults,
	}
}

// ObjectCount returns the number of live objects currently stored.
func (s *SharedObjects) ObjectCount() int { return len(s.data) }

// LiveBytes returns the total size in bytes of live objects currently stored.
func (s *SharedObjects) LiveBytes() int64 { return s.Bytes }

// NodeObjectStore is one node's handle to a SharedObjects store.
// Completions are scheduled via schedule(delay, fn) on the simulator's queue.
type NodeObjectStore struct {
	shared   *SharedObjects
	rng      *Rand
	schedule func(delay time.Duration, fn func())
	deliver  func(id sys.ObjOpID, data []byte, err error)
}

// NewNodeObjectStore binds one simulated node to shared.
func NewNodeObjectStore(
	shared *SharedObjects,
	rng *Rand,
	schedule func(delay time.Duration, fn func()),
	deliver func(id sys.ObjOpID, data []byte, err error),
) *NodeObjectStore {
	return &NodeObjectStore{shared: shared, rng: rng, schedule: schedule, deliver: deliver}
}

func (s *NodeObjectStore) delay() time.Duration {
	if s.shared.Faults.MaxDelay <= 0 {
		return time.Millisecond
	}
	return time.Duration(s.rng.Uint64()%uint64(s.shared.Faults.MaxDelay)) + time.Millisecond
}

func (s *NodeObjectStore) Put(id sys.ObjOpID, key string, data []byte) {
	cp := append([]byte(nil), data...)
	d := s.delay()
	s.schedule(d, func() {
		if s.shared.Faults.LostPut > 0 && s.rng.Chance(s.shared.Faults.LostPut) {
			s.deliver(id, nil, errors.New("simobj: put lost"))
			return
		}
		if old, ok := s.shared.data[key]; ok {
			s.shared.Bytes -= int64(len(old))
		}
		s.shared.data[key] = cp
		s.shared.Bytes += int64(len(cp))
		s.shared.Puts++
		s.deliver(id, nil, nil)
	})
}

func (s *NodeObjectStore) Get(id sys.ObjOpID, key string, offset, length int64) {
	d := s.delay()
	s.schedule(d, func() {
		if s.shared.Faults.LostGet > 0 && s.rng.Chance(s.shared.Faults.LostGet) {
			s.deliver(id, nil, errors.New("simobj: get lost"))
			return
		}
		b, ok := s.shared.data[key]
		if !ok {
			s.deliver(id, nil, fmt.Errorf("%w: %s", sys.ErrNotExist, key))
			return
		}
		s.shared.Gets++
		if offset < 0 || offset > int64(len(b)) {
			s.deliver(id, nil, fmt.Errorf("simobj: offset %d out of range %d", offset, len(b)))
			return
		}
		end := int64(len(b))
		if length > 0 {
			end = offset + length
			if end > int64(len(b)) {
				s.deliver(id, nil, fmt.Errorf("simobj: range [%d,%d) out of range %d", offset, end, len(b)))
				return
			}
		}
		out := append([]byte(nil), b[offset:end]...)
		if s.shared.Faults.TornRead > 0 && len(out) > 1 && s.rng.Chance(s.shared.Faults.TornRead) {
			// Flip a byte or truncate to simulate a torn/corrupt object read.
			if s.rng.Intn(2) == 0 {
				out[s.rng.Intn(len(out))] ^= 0xff
			} else {
				out = out[:len(out)/2]
			}
		}
		s.deliver(id, out, nil)
	})
}

func (s *NodeObjectStore) Delete(id sys.ObjOpID, key string) {
	d := s.delay()
	s.schedule(d, func() {
		if old, ok := s.shared.data[key]; ok {
			s.shared.Bytes -= int64(len(old))
			delete(s.shared.data, key)
		}
		s.shared.Dels++
		s.deliver(id, nil, nil)
	})
}
