package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// indexMagic identifies an index file ("SAIX").
const indexMagic = 0x53414958

// indexEntry says that the frame whose first offset is rel past the
// segment's base starts at byte pos of the segment.
type indexEntry struct{ rel, pos uint32 }

var errBadIndex = errors.New("bad index")

// encodeIndex returns the index file content for entries:
// magic u32 | n u32 | n × (rel u32, pos u32) | crc u32.
func encodeIndex(entries []indexEntry) []byte {
	b := make([]byte, 0, 12+8*len(entries))
	b = binary.BigEndian.AppendUint32(b, indexMagic)
	b = binary.BigEndian.AppendUint32(b, uint32(len(entries)))
	for _, e := range entries {
		b = binary.BigEndian.AppendUint32(b, e.rel)
		b = binary.BigEndian.AppendUint32(b, e.pos)
	}
	return binary.BigEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

// decodeIndex parses an index file for a segment of segSize bytes. It
// accepts only a file that is exactly well-formed: an index is derived data,
// so anything doubtful is rejected and rebuilt from the segment.
func decodeIndex(b []byte, segSize int64) ([]indexEntry, error) {
	if len(b) < 12 || binary.BigEndian.Uint32(b) != indexMagic {
		return nil, errBadIndex
	}
	n := int(binary.BigEndian.Uint32(b[4:]))
	if n != (len(b)-12)/8 || len(b) != 12+8*n { // n is checked against the real size before allocating
		return nil, errBadIndex
	}
	if crc32.Checksum(b[:len(b)-4], castagnoli) != binary.BigEndian.Uint32(b[len(b)-4:]) {
		return nil, errBadIndex
	}
	entries := make([]indexEntry, n)
	for i := range entries {
		e := indexEntry{binary.BigEndian.Uint32(b[8+8*i:]), binary.BigEndian.Uint32(b[12+8*i:])}
		if int64(e.pos) >= segSize || (i > 0 && (e.rel <= entries[i-1].rel || e.pos <= entries[i-1].pos)) {
			return nil, errBadIndex
		}
		entries[i] = e
	}
	return entries, nil
}
