// Package storage implements a segmented, append-only, crash-safe log.
//
// The log stores opaque payloads in checksummed frames and names records by
// dense 64-bit offsets. It does all I/O through sys.Disk and assumes exactly
// the durability contract documented there. See docs/design/phase-1.md.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	"github.com/amritangshudey/samsa/internal/sys"
)

// headerSize is the length of the fixed part of a frame:
// crc u32 | length u32 | base i64 | count u32.
const headerSize = 20

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// errBadFrame marks bytes that are not a whole, valid frame: a torn tail
// during recovery, corruption anywhere else.
var errBadFrame = errors.New("bad frame")

// Batch is one frame's content: an opaque payload occupying Count
// consecutive offsets starting at Base.
type Batch struct {
	Base    int64
	Count   uint32
	Payload []byte
}

// appendFrame appends the encoding of one frame to dst.
func appendFrame(dst []byte, base int64, count uint32, payload []byte) []byte {
	start := len(dst)
	dst = binary.BigEndian.AppendUint32(dst, 0) // crc, filled in below
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(payload)))
	dst = binary.BigEndian.AppendUint64(dst, uint64(base))
	dst = binary.BigEndian.AppendUint32(dst, count)
	dst = append(dst, payload...)
	binary.BigEndian.PutUint32(dst[start:], crc32.Checksum(dst[start+4:], castagnoli))
	return dst
}

// frameHeader is a decoded, bounds-checked frame header.
type frameHeader struct {
	crc    uint32
	length int64 // payload length
	base   int64
	count  uint32
	raw    [headerSize]byte
}

// readHeader reads the frame header at pos in a file of the given size and
// checks everything that can be checked without the payload. In particular
// the length is bounded by maxPayload and by the file before anyone
// allocates a buffer for it.
func readHeader(f sys.File, pos, size int64, maxPayload int) (frameHeader, error) {
	var h frameHeader
	if size-pos < headerSize {
		return h, fmt.Errorf("%w: %d bytes left, header needs %d", errBadFrame, size-pos, headerSize)
	}
	if _, err := f.ReadAt(h.raw[:], pos); err != nil {
		return h, fmt.Errorf("read frame header at %d: %w", pos, err)
	}
	h.crc = binary.BigEndian.Uint32(h.raw[0:])
	h.length = int64(binary.BigEndian.Uint32(h.raw[4:]))
	h.base = int64(binary.BigEndian.Uint64(h.raw[8:]))
	h.count = binary.BigEndian.Uint32(h.raw[16:])
	switch {
	case h.length > int64(maxPayload):
		return h, fmt.Errorf("%w: payload length %d exceeds limit %d", errBadFrame, h.length, maxPayload)
	case h.length > size-pos-headerSize:
		return h, fmt.Errorf("%w: payload length %d runs past end of file", errBadFrame, h.length)
	case h.count == 0:
		return h, fmt.Errorf("%w: zero record count", errBadFrame)
	case h.base < 0 || h.base > math.MaxInt64-int64(h.count):
		return h, fmt.Errorf("%w: offsets %d+%d out of range", errBadFrame, h.base, h.count)
	}
	return h, nil
}

// readPayload reads and checksums the payload of the frame whose header h
// was read at pos.
func readPayload(f sys.File, pos int64, h frameHeader) ([]byte, error) {
	payload := make([]byte, h.length)
	if _, err := f.ReadAt(payload, pos+headerSize); err != nil && (!errors.Is(err, io.EOF) || h.length > 0) {
		return nil, fmt.Errorf("read frame payload at %d: %w", pos, err)
	}
	crc := crc32.Update(crc32.Checksum(h.raw[4:], castagnoli), castagnoli, payload)
	if crc != h.crc {
		return nil, fmt.Errorf("%w: checksum mismatch at %d", errBadFrame, pos)
	}
	return payload, nil
}
