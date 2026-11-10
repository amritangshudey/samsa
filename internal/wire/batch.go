package wire

import (
	"encoding/binary"
	"hash/crc32"
)

// Layout of a record batch header (format "magic 2"), from
// DefaultRecordBatch.java. All fields are big-endian.
//
//	 0  BaseOffset            int64
//	 8  Length                int32   bytes that follow this field
//	12  PartitionLeaderEpoch  int32
//	16  Magic                 int8    2
//	17  CRC                   uint32  CRC-32C of everything from Attributes on
//	21  Attributes            int16
//	23  LastOffsetDelta       int32
//	27  BaseTimestamp         int64
//	35  MaxTimestamp          int64
//	43  ProducerId            int64
//	51  ProducerEpoch         int16
//	53  BaseSequence          int32
//	57  RecordsCount          int32
//	61  records
//
// The CRC does not cover the base offset or the leader epoch, so a broker
// can assign them without recomputing it.
const (
	batchLengthAt      = 8
	batchMagicAt       = 16
	batchCRCAt         = 17
	batchAttributesAt  = 21
	batchLastDeltaAt   = 23
	batchBaseTimeAt    = 27
	batchMaxTimeAt     = 35
	batchProducerIDAt  = 43
	batchProducerEpoch = 51
	batchBaseSeqAt     = 53
	batchCountAt       = 57

	// BatchOverhead is the size of a batch header.
	BatchOverhead = 61
	// batchLogOverhead is BaseOffset plus Length: what Length does not count.
	batchLogOverhead = 12

	attrControl       = 0x20
	attrTransactional = 0x10
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// BatchInfo is what the broker needs to know about a record batch.
type BatchInfo struct {
	Size         int   // bytes the whole batch occupies
	Count        int32 // number of records, and of offsets it consumes
	MaxTimestamp int64
}

// ParseBatch validates the record batch at the start of b without looking
// inside its records, which may be compressed. It returns a Kafka error
// code, ErrNone if the batch is acceptable. maxSize bounds the batch size.
func ParseBatch(b []byte, maxSize int) (BatchInfo, int16) {
	var info BatchInfo
	if len(b) < batchMagicAt+1 {
		return info, ErrCorruptMessage
	}
	length := int(int32(binary.BigEndian.Uint32(b[batchLengthAt:])))
	// The magic byte sits at the same place in every format, and must be
	// read before anything after it is interpreted.
	if b[batchMagicAt] != 2 {
		return info, ErrUnsupportedForMessageFormat
	}
	info.Size = batchLogOverhead + length
	switch {
	case length < BatchOverhead-batchLogOverhead || info.Size > len(b):
		return info, ErrCorruptMessage
	case info.Size > maxSize:
		return info, ErrMessageTooLarge
	}
	b = b[:info.Size]
	if crc32.Checksum(b[batchAttributesAt:], castagnoli) != binary.BigEndian.Uint32(b[batchCRCAt:]) {
		return info, ErrCorruptMessage
	}
	attributes := binary.BigEndian.Uint16(b[batchAttributesAt:])
	info.Count = int32(binary.BigEndian.Uint32(b[batchCountAt:]))
	lastDelta := int32(binary.BigEndian.Uint32(b[batchLastDeltaAt:]))
	info.MaxTimestamp = int64(binary.BigEndian.Uint64(b[batchMaxTimeAt:]))
	switch {
	case info.Count < 1 || lastDelta != info.Count-1:
		return info, ErrInvalidRecord
	case attributes&(attrControl|attrTransactional) != 0:
		return info, ErrInvalidRecord // transactions are not supported
	}
	return info, ErrNone
}

// SetBatchBaseOffset stamps the offset of a batch's first record into it.
// b must have been accepted by ParseBatch.
func SetBatchBaseOffset(b []byte, offset int64) {
	binary.BigEndian.PutUint64(b, uint64(offset))
}

// BatchBaseOffset returns the base offset stored in a batch that has at
// least a full header.
func BatchBaseOffset(b []byte) int64 { return int64(binary.BigEndian.Uint64(b)) }

// Record is one key/value pair of an uncompressed batch. The broker never
// decodes records; tests and the simulator's clients do.
type Record struct {
	Key, Value []byte
	Timestamp  int64
}

// EncodeBatch builds an uncompressed record batch holding records, with
// base offset 0. Timestamps are stored relative to the first record's.
func EncodeBatch(records []Record) []byte {
	var w Writer
	w.B = make([]byte, BatchOverhead)
	var baseTime, maxTime int64 = -1, -1
	if len(records) > 0 {
		baseTime = records[0].Timestamp
	}
	for i, rec := range records {
		maxTime = max(maxTime, rec.Timestamp)
		var body Writer
		body.Int8(0) // attributes
		body.Varlong(rec.Timestamp - baseTime)
		body.Varint(int32(i)) // offset delta
		body.varBytes(rec.Key)
		body.varBytes(rec.Value)
		body.Varint(0) // header count
		w.Varint(int32(len(body.B)))
		w.B = append(w.B, body.B...)
	}
	b := w.B
	binary.BigEndian.PutUint32(b[batchLengthAt:], uint32(len(b)-batchLogOverhead))
	binary.BigEndian.PutUint32(b[12:], ^uint32(0)) // PartitionLeaderEpoch: -1
	b[batchMagicAt] = 2
	binary.BigEndian.PutUint32(b[batchLastDeltaAt:], uint32(len(records)-1))
	binary.BigEndian.PutUint64(b[batchBaseTimeAt:], uint64(baseTime))
	binary.BigEndian.PutUint64(b[batchMaxTimeAt:], uint64(maxTime))
	binary.BigEndian.PutUint64(b[batchProducerIDAt:], ^uint64(0))  // no producer id
	binary.BigEndian.PutUint16(b[batchProducerEpoch:], ^uint16(0)) // no producer epoch
	binary.BigEndian.PutUint32(b[batchBaseSeqAt:], ^uint32(0))     // no sequence
	binary.BigEndian.PutUint32(b[batchCountAt:], uint32(len(records)))
	binary.BigEndian.PutUint32(b[batchCRCAt:], crc32.Checksum(b[batchAttributesAt:], castagnoli))
	return b
}

// varBytes appends a record-level byte field: a zig-zag varint length (-1
// for null) and the bytes.
func (w *Writer) varBytes(p []byte) {
	if p == nil {
		w.Varint(-1)
		return
	}
	w.Varint(int32(len(p)))
	w.B = append(w.B, p...)
}

func (r *Reader) varBytes() []byte {
	n := r.Varint()
	if n < 0 {
		return nil
	}
	return r.take(int(n))
}

// DecodeBatch returns the records of one uncompressed batch that ParseBatch
// has accepted, together with its base offset.
func DecodeBatch(b []byte) (base int64, records []Record, err error) {
	if len(b) < BatchOverhead {
		return 0, nil, ErrMalformed
	}
	if binary.BigEndian.Uint16(b[batchAttributesAt:])&0x07 != 0 {
		return 0, nil, ErrMalformed // compressed: not decodable here
	}
	base = BatchBaseOffset(b)
	baseTime := int64(binary.BigEndian.Uint64(b[batchBaseTimeAt:]))
	r := NewReader(b[BatchOverhead:])
	for n := int(int32(binary.BigEndian.Uint32(b[batchCountAt:]))); n > 0 && r.Err() == nil; n-- {
		body := NewReader(r.take(int(r.Varint())))
		body.Int8()
		rec := Record{Timestamp: baseTime + body.Varlong()}
		body.Varint() // offset delta
		rec.Key = body.varBytes()
		rec.Value = body.varBytes()
		if body.Err() != nil {
			return 0, nil, body.Err()
		}
		records = append(records, rec)
	}
	return base, records, r.Err()
}
