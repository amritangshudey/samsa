package broker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"path"

	"github.com/amritangshudey/samsa/internal/raft"
	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// Producer ids (ADR 0015). Two producers with one id would have their
// batches taken for each other's retries and dropped, so an id must never
// be handed out twice: not by two nodes, not by one node in two lives, not
// after a power loss under a sync policy that may lose the end of the log.
//
//	id = node id (16 bits) | block (28 bits) | index in the block (19 bits)
//
// The block is a number the node makes durable, always with a sync, before
// it uses any id of the block.
const (
	pidIndexBits = 19
	pidBlockBits = 28
	// maxNodeID is the largest node id that fits above block and index in
	// a positive int64.
	maxNodeID = 1<<16 - 1

	producerBlockFile  = "producer-block"
	producerBlockMagic = 0x53415042 // "SAPB"
	producerBlockSize  = 4 + 8 + 4
)

var (
	// errBadProducerBlock means the block file is not intact, which a crash
	// cannot cause (it is replaced atomically): the node must not guess.
	errBadProducerBlock   = errors.New("broker: producer-block file is corrupt")
	errProducerIDsUsedUp  = errors.New("broker: this node has used all its producer id blocks")
	errNodeIDForProducers = fmt.Errorf("broker: node id must be between 0 and %d", maxNodeID)
)

// producerIDs hands out this node's producer ids.
type producerIDs struct {
	disk  sys.Disk
	dir   string
	node  int64
	block int64 // the block reserved last
	used  int64 // how many of its ids are handed out
}

// reserve makes the next block this node's, durably, before it returns.
func (p *producerIDs) reserve() error {
	b, err := raft.ReadFile(p.disk, path.Join(p.dir, producerBlockFile))
	if err != nil {
		return err
	}
	next := int64(0)
	if b != nil { // nil: a node that never had a block
		if len(b) != producerBlockSize || binary.BigEndian.Uint32(b) != producerBlockMagic ||
			crc32.Checksum(b[:12], crc32.MakeTable(crc32.Castagnoli)) != binary.BigEndian.Uint32(b[12:]) {
			return errBadProducerBlock
		}
		next = int64(binary.BigEndian.Uint64(b[4:])) + 1
	}
	if next < 0 || next >= 1<<pidBlockBits {
		return errProducerIDsUsedUp
	}
	b = binary.BigEndian.AppendUint32(make([]byte, 0, producerBlockSize), producerBlockMagic)
	b = binary.BigEndian.AppendUint64(b, uint64(next))
	b = binary.BigEndian.AppendUint32(b, crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)))
	if err := raft.WriteAtomic(p.disk, p.dir, producerBlockFile, b); err != nil {
		return err
	}
	p.block, p.used = next, 0
	return nil
}

// next returns an id no producer has had.
func (p *producerIDs) next() (int64, error) {
	if p.used == 1<<pidIndexBits {
		if err := p.reserve(); err != nil {
			return -1, err
		}
	}
	p.used++
	return p.node<<(pidBlockBits+pidIndexBits) | p.block<<pidIndexBits | (p.used - 1), nil
}

// initProducerID answers InitProducerId. Any node does, leader or not:
// clients ask whichever node they like (Sender.java, leastLoadedNode), and
// the ids need nothing from the cluster.
func (n *Node) initProducerID(m *wire.InitProducerIDRequest) *wire.InitProducerIDResponse {
	resp := &wire.InitProducerIDResponse{ProducerID: -1, ProducerEpoch: -1}
	if m.Transactional {
		// shortcut: no transactions yet (docs/PRODUCTION.md item 15); a
		// client that asks for them is told so at once.
		resp.ErrorCode = wire.ErrInvalidRequest
		return resp
	}
	id, err := n.pids.next()
	if err != nil {
		n.fatal = fmt.Errorf("producer id: %w", err) // respond sends nothing after this
		return resp
	}
	resp.ProducerID, resp.ProducerEpoch = id, 0
	return resp
}
