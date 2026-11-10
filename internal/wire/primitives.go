// Package wire encodes and decodes the Kafka protocol: primitive types,
// headers, the messages of each supported API, and record batches.
//
// Every layout here was taken from the Apache Kafka sources (message
// definitions, Type.java, ByteUtils.java, DefaultRecordBatch.java); see
// docs/design/phase-2.md and ADR 0006.
//
// Input is never trusted. A Reader never allocates from a length it has not
// compared with the bytes actually present, and it never panics.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// ErrMalformed is wrapped by every decoding error.
var ErrMalformed = errors.New("wire: malformed input")

// Reader decodes Kafka primitives from a byte slice.
//
// Errors are sticky: after the first failure every method returns a zero
// value, and the caller checks Err once at the end. Slices returned by
// Bytes alias the input.
type Reader struct {
	b   []byte
	err error
}

// NewReader returns a Reader over b.
func NewReader(b []byte) *Reader { return &Reader{b: b} }

// Err returns the first decoding error, or nil.
func (r *Reader) Err() error { return r.err }

// Len returns the number of bytes not yet read.
func (r *Reader) Len() int { return len(r.b) }

// Rest returns the unread bytes without consuming them.
func (r *Reader) Rest() []byte { return r.b }

func (r *Reader) fail(format string, args ...any) {
	if r.err == nil {
		r.err = fmt.Errorf("%w: %s", ErrMalformed, fmt.Sprintf(format, args...))
	}
}

// take consumes n bytes, or fails and returns nil.
func (r *Reader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || n > len(r.b) {
		r.fail("need %d bytes, have %d", n, len(r.b))
		return nil
	}
	p := r.b[:n]
	r.b = r.b[n:]
	return p
}

// Int8 reads a signed byte.
func (r *Reader) Int8() int8 {
	if p := r.take(1); p != nil {
		return int8(p[0])
	}
	return 0
}

// Bool reads a byte; any non-zero value is true.
func (r *Reader) Bool() bool { return r.Int8() != 0 }

// Int16 reads a big-endian 16-bit integer.
func (r *Reader) Int16() int16 {
	if p := r.take(2); p != nil {
		return int16(binary.BigEndian.Uint16(p))
	}
	return 0
}

// Int32 reads a big-endian 32-bit integer.
func (r *Reader) Int32() int32 {
	if p := r.take(4); p != nil {
		return int32(binary.BigEndian.Uint32(p))
	}
	return 0
}

// Int64 reads a big-endian 64-bit integer.
func (r *Reader) Int64() int64 {
	if p := r.take(8); p != nil {
		return int64(binary.BigEndian.Uint64(p))
	}
	return 0
}

// Uvarint reads an unsigned 32-bit variable-length integer: seven bits per
// byte, least significant group first, high bit set on all but the last.
func (r *Reader) Uvarint() uint32 {
	var v uint32
	for shift := 0; shift < 35; shift += 7 {
		p := r.take(1)
		if p == nil {
			return 0
		}
		v |= uint32(p[0]&0x7f) << shift
		if p[0]&0x80 == 0 {
			if shift == 28 && p[0] > 0x0f {
				r.fail("varint overflows 32 bits")
				return 0
			}
			return v
		}
	}
	r.fail("varint longer than 5 bytes")
	return 0
}

// Varint reads a zig-zag encoded signed 32-bit integer.
func (r *Reader) Varint() int32 {
	u := r.Uvarint()
	return int32(u>>1) ^ -int32(u&1)
}

// Varlong reads a zig-zag encoded signed 64-bit integer.
func (r *Reader) Varlong() int64 {
	var u uint64
	for shift := 0; shift < 70; shift += 7 {
		p := r.take(1)
		if p == nil {
			return 0
		}
		u |= uint64(p[0]&0x7f) << shift
		if p[0]&0x80 == 0 {
			if shift == 63 && p[0] > 1 {
				r.fail("varlong overflows 64 bits")
				return 0
			}
			return int64(u>>1) ^ -int64(u&1)
		}
	}
	r.fail("varlong longer than 10 bytes")
	return 0
}

// length reads the length prefix of a string, byte sequence or array in
// either encoding and returns it, or -1 for null. wide selects a 32-bit
// prefix (bytes, arrays) over a 16-bit one (strings) in the old encoding.
func (r *Reader) length(flex, wide bool) int {
	var n int
	switch {
	case flex:
		n = int(r.Uvarint()) - 1 // compact: N+1, with 0 meaning null
	case wide:
		n = int(r.Int32())
	default:
		n = int(r.Int16())
	}
	if n < -1 {
		r.fail("negative length %d", n)
		return -1
	}
	return n
}

// NullableString reads a string that may be null.
func (r *Reader) NullableString(flex bool) (s string, null bool) {
	n := r.length(flex, false)
	if n < 0 {
		return "", true
	}
	return string(r.take(n)), false
}

// String reads a string that may not be null.
func (r *Reader) String(flex bool) string {
	s, null := r.NullableString(flex)
	if null {
		r.fail("null string where one is required")
	}
	return s
}

// Bytes reads a byte sequence, returning nil for null. The result aliases
// the input.
func (r *Reader) Bytes(flex bool) []byte {
	n := r.length(flex, true)
	if n < 0 {
		return nil
	}
	return r.take(n)
}

// ArrayLen reads an array's element count, or -1 for a null array. The
// count is checked against the bytes remaining (every element occupies at
// least one byte), so it is safe to loop to it.
func (r *Reader) ArrayLen(flex bool) int {
	n := r.length(flex, true)
	if n > len(r.b) {
		r.fail("array of %d elements in %d bytes", n, len(r.b))
		return -1
	}
	return n
}

// Tags skips the tagged fields that end every structure in a flexible
// version. It does nothing when flex is false.
func (r *Reader) Tags(flex bool) {
	if !flex {
		return
	}
	for n := r.Uvarint(); n > 0 && r.err == nil; n-- {
		r.Uvarint() // tag
		r.take(int(r.Uvarint()))
	}
}

// Array reads an array, calling elem once per element. A null or empty
// array yields nil.
func Array[T any](r *Reader, flex bool, elem func() T) []T {
	n := r.ArrayLen(flex)
	if n <= 0 {
		return nil
	}
	// The capacity is capped: n is bounded by the input size, but an
	// element in memory can be much larger than its one-byte minimum.
	out := make([]T, 0, min(n, 256))
	for ; n > 0 && r.err == nil; n-- {
		out = append(out, elem())
	}
	if r.err != nil {
		return nil
	}
	return out
}

// Writer encodes Kafka primitives by appending to B.
type Writer struct{ B []byte }

// Int8 appends a signed byte.
func (w *Writer) Int8(v int8) { w.B = append(w.B, byte(v)) }

// Bool appends 1 for true, 0 for false.
func (w *Writer) Bool(v bool) {
	if v {
		w.B = append(w.B, 1)
	} else {
		w.B = append(w.B, 0)
	}
}

// Int16 appends a big-endian 16-bit integer.
func (w *Writer) Int16(v int16) { w.B = binary.BigEndian.AppendUint16(w.B, uint16(v)) }

// Int32 appends a big-endian 32-bit integer.
func (w *Writer) Int32(v int32) { w.B = binary.BigEndian.AppendUint32(w.B, uint32(v)) }

// Int64 appends a big-endian 64-bit integer.
func (w *Writer) Int64(v int64) { w.B = binary.BigEndian.AppendUint64(w.B, uint64(v)) }

// Uvarint appends an unsigned variable-length integer.
func (w *Writer) Uvarint(v uint32) { w.B = binary.AppendUvarint(w.B, uint64(v)) }

// Varint appends a zig-zag encoded signed 32-bit integer.
func (w *Writer) Varint(v int32) { w.Uvarint(uint32(v<<1) ^ uint32(v>>31)) }

// Varlong appends a zig-zag encoded signed 64-bit integer.
func (w *Writer) Varlong(v int64) { w.B = binary.AppendUvarint(w.B, uint64(v<<1)^uint64(v>>63)) }

// length appends a length prefix; n = -1 encodes null.
func (w *Writer) length(flex, wide bool, n int) {
	switch {
	case flex:
		w.Uvarint(uint32(n + 1))
	case wide:
		w.Int32(int32(n))
	default:
		w.Int16(int16(n))
	}
}

// String appends a non-null string.
func (w *Writer) String(flex bool, s string) {
	w.length(flex, false, len(s))
	w.B = append(w.B, s...)
}

// NullString appends a null string.
func (w *Writer) NullString(flex bool) { w.length(flex, false, -1) }

// Bytes appends a byte sequence; nil is encoded as null.
func (w *Writer) Bytes(flex bool, p []byte) {
	if p == nil {
		w.length(flex, true, -1)
		return
	}
	w.length(flex, true, len(p))
	w.B = append(w.B, p...)
}

// ArrayLen appends an array's element count; -1 encodes a null array.
func (w *Writer) ArrayLen(flex bool, n int) { w.length(flex, true, n) }

// Tags appends an empty set of tagged fields when flex is true.
func (w *Writer) Tags(flex bool) {
	if flex {
		w.B = append(w.B, 0)
	}
}
