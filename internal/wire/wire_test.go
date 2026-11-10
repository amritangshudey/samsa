package wire

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"
)

func TestPrimitivesRoundTrip(t *testing.T) {
	for _, flex := range []bool{false, true} {
		var w Writer
		w.Int8(-5)
		w.Bool(true)
		w.Int16(-300)
		w.Int32(1 << 30)
		w.Int64(-1 << 60)
		w.Uvarint(300)
		w.Varint(-70000)
		w.Varlong(-1 << 62)
		w.String(flex, "héllo")
		w.NullString(flex)
		w.Bytes(flex, []byte{1, 2, 3})
		w.Bytes(flex, nil)
		w.Bytes(flex, []byte{})
		w.ArrayLen(flex, 0)
		w.ArrayLen(flex, -1)
		w.Tags(flex)

		r := NewReader(w.B)
		check := func(name string, got, want any) {
			t.Helper()
			if !reflect.DeepEqual(got, want) {
				t.Errorf("flex=%v %s: got %v, want %v", flex, name, got, want)
			}
		}
		check("int8", r.Int8(), int8(-5))
		check("bool", r.Bool(), true)
		check("int16", r.Int16(), int16(-300))
		check("int32", r.Int32(), int32(1<<30))
		check("int64", r.Int64(), int64(-1<<60))
		check("uvarint", r.Uvarint(), uint32(300))
		check("varint", r.Varint(), int32(-70000))
		check("varlong", r.Varlong(), int64(-1<<62))
		check("string", r.String(flex), "héllo")
		_, null := r.NullableString(flex)
		check("null string", null, true)
		check("bytes", r.Bytes(flex), []byte{1, 2, 3})
		check("null bytes", r.Bytes(flex) == nil, true)
		check("empty bytes", len(r.Bytes(flex)), 0)
		check("empty array", r.ArrayLen(flex), 0)
		check("null array", r.ArrayLen(flex), -1)
		r.Tags(flex)
		if r.Err() != nil || r.Len() != 0 {
			t.Errorf("flex=%v: err %v, %d bytes left", flex, r.Err(), r.Len())
		}
	}
}

// Encodings fixed by the specification, independent of our own writer.
func TestKnownEncodings(t *testing.T) {
	tests := []struct {
		name  string
		write func(*Writer)
		want  string
	}{
		{"uvarint 300", func(w *Writer) { w.Uvarint(300) }, "ac02"},
		{"varint -1 is zig-zag 1", func(w *Writer) { w.Varint(-1) }, "01"},
		{"varint 1 is zig-zag 2", func(w *Writer) { w.Varint(1) }, "02"},
		{"compact string is length+1", func(w *Writer) { w.String(true, "ab") }, "036162"},
		{"compact null is 0", func(w *Writer) { w.NullString(true) }, "00"},
		{"string has int16 length", func(w *Writer) { w.String(false, "ab") }, "00026162"},
		{"null string is -1", func(w *Writer) { w.NullString(false) }, "ffff"},
		{"null bytes is -1", func(w *Writer) { w.Bytes(false, nil) }, "ffffffff"},
		{"compact array is count+1", func(w *Writer) { w.ArrayLen(true, 2) }, "03"},
	}
	for _, tt := range tests {
		var w Writer
		tt.write(&w)
		if got := hex.EncodeToString(w.B); got != tt.want {
			t.Errorf("%s: got %s, want %s", tt.name, got, tt.want)
		}
	}
}

func TestReaderRejects(t *testing.T) {
	tests := []struct {
		name string
		in   string
		read func(*Reader)
	}{
		{"short int32", "0000", func(r *Reader) { r.Int32() }},
		{"string longer than input", "0005aa", func(r *Reader) { r.String(false) }},
		{"negative string length", "fffe", func(r *Reader) { r.String(false) }},
		{"null where required", "ffff", func(r *Reader) { r.String(false) }},
		{"array count larger than input", "7fffffff00", func(r *Reader) { r.ArrayLen(false) }},
		{"compact array count larger than input", "ff7f", func(r *Reader) { r.ArrayLen(true) }},
		{"varint too long", "ffffffffff01", func(r *Reader) { r.Uvarint() }},
		{"varint overflow", "ffffffff7f", func(r *Reader) { r.Uvarint() }},
		{"unterminated varint", "ff", func(r *Reader) { r.Uvarint() }},
		{"tagged field longer than input", "010005", func(r *Reader) { r.Tags(true) }},
	}
	for _, tt := range tests {
		in, _ := hex.DecodeString(tt.in)
		r := NewReader(in)
		tt.read(r)
		if !errors.Is(r.Err(), ErrMalformed) {
			t.Errorf("%s: got %v, want ErrMalformed", tt.name, r.Err())
		}
		// After a failure every read must be a harmless zero.
		if r.Int64() != 0 || r.String(false) != "" || r.Bytes(true) != nil {
			t.Errorf("%s: reads after failure returned data", tt.name)
		}
	}
}

// messages returns one populated value of every message, for round trips
// and as fuzz seeds.
func messages() map[string]struct {
	key   int16
	value Message
	fresh func() Message
} {
	type entry = struct {
		key   int16
		value Message
		fresh func() Message
	}
	return map[string]entry{
		"ApiVersionsRequest": {KeyAPIVersions, &APIVersionsRequest{ClientSoftwareName: "samsa-test", ClientSoftwareVersion: "1"},
			func() Message { return &APIVersionsRequest{} }},
		"ApiVersionsResponse": {KeyAPIVersions, &APIVersionsResponse{ErrorCode: 35, APIs: []APIRange{{0, 0, 8}, {18, 0, 4}}},
			func() Message { return &APIVersionsResponse{} }},
		"MetadataRequest": {KeyMetadata, &MetadataRequest{Topics: []string{"a", "b"}},
			func() Message { return &MetadataRequest{} }},
		"MetadataRequestAll": {KeyMetadata, &MetadataRequest{AllTopics: true},
			func() Message { return &MetadataRequest{} }},
		"MetadataResponse": {KeyMetadata, &MetadataResponse{
			Brokers: []Broker{{1, "localhost", 9092}}, ClusterID: "c", ControllerID: 1,
			Topics: []TopicMetadata{{Name: "a", Partitions: []PartitionMetadata{{Index: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}}},
				{ErrorCode: 3, Name: "missing"}},
		}, func() Message { return &MetadataResponse{} }},
		"CreateTopicsRequest": {KeyCreateTopics, &CreateTopicsRequest{Topics: []CreateTopic{{Name: "a", NumPartitions: 3, ReplicationFactor: 1}}, TimeoutMs: 1000},
			func() Message { return &CreateTopicsRequest{} }},
		"CreateTopicsResponse": {KeyCreateTopics, &CreateTopicsResponse{Topics: []CreateTopicResult{{Name: "a"}, {Name: "b", ErrorCode: 36, ErrorMessage: "exists"}}},
			func() Message { return &CreateTopicsResponse{} }},
		"ProduceRequest": {KeyProduce, &ProduceRequest{Acks: -1, TimeoutMs: 30000, Topics: []ProduceTopic{{Name: "a", Partitions: []ProducePartition{{Index: 2, Records: []byte("batch")}}}}},
			func() Message { return &ProduceRequest{} }},
		"ProduceResponse": {KeyProduce, &ProduceResponse{Topics: []ProduceTopicResponse{{Name: "a", Partitions: []ProducePartitionResponse{{Index: 2, BaseOffset: 77}, {Index: 3, ErrorCode: 3, BaseOffset: -1}}}}},
			func() Message { return &ProduceResponse{} }},
		"FetchRequest": {KeyFetch, &FetchRequest{MaxWaitMs: 500, MinBytes: 1, MaxBytes: 1 << 20, Topics: []FetchTopic{{Name: "a", Partitions: []FetchPartition{{Index: 0, Offset: 5, MaxBytes: 1 << 16}}}}},
			func() Message { return &FetchRequest{} }},
		"FetchResponse": {KeyFetch, &FetchResponse{Topics: []FetchTopicResponse{{Name: "a", Partitions: []FetchPartitionResponse{{Index: 0, HighWatermark: 9, Records: []byte("batches")}, {Index: 1, ErrorCode: 1}}}}},
			func() Message { return &FetchResponse{} }},
		"ListOffsetsRequest": {KeyListOffsets, &ListOffsetsRequest{Topics: []ListOffsetsTopic{{Name: "a", Partitions: []ListOffsetsPartition{{Index: 0, Timestamp: TimestampEarliest}}}}},
			func() Message { return &ListOffsetsRequest{} }},
		"ListOffsetsResponse": {KeyListOffsets, &ListOffsetsResponse{Topics: []ListOffsetsTopicResponse{{Name: "a", Partitions: []ListOffsetsPartitionResponse{{Index: 0, Timestamp: -1, Offset: 12}}}}},
			func() Message { return &ListOffsetsResponse{} }},
	}
}

// Every message, at every supported version, must decode to what was
// encoded and consume exactly the bytes written.
func TestMessagesRoundTripAtEveryVersion(t *testing.T) {
	for name, m := range messages() {
		api, _ := LookupAPI(m.key)
		for v := api.Min; v <= api.Max; v++ {
			var w Writer
			m.value.Encode(&w, v)
			got := m.fresh()
			r := NewReader(w.B)
			got.Decode(r, v)
			if r.Err() != nil || r.Len() != 0 {
				t.Errorf("%s v%d: err %v, %d bytes left over", name, v, r.Err(), r.Len())
				continue
			}
			// At the newest version every field exists, so the value must
			// come back whole. Older versions lack some fields; there the
			// test is that re-encoding reproduces the same bytes.
			if v == api.Max && !reflect.DeepEqual(got, m.value) {
				t.Errorf("%s v%d:\n got %+v\nwant %+v", name, v, got, m.value)
			}
			var again Writer
			got.Encode(&again, v)
			if !bytes.Equal(again.B, w.B) {
				t.Errorf("%s v%d: re-encoding differs:\n%x\n%x", name, v, again.B, w.B)
			}
			// Truncating the encoding anywhere must give an error, never a panic.
			for cut := range w.B {
				r := NewReader(w.B[:cut])
				m.fresh().Decode(r, v)
				if r.Err() == nil && r.Len() == 0 && cut < len(w.B) && !prefixIsValid(name) {
					t.Errorf("%s v%d: truncation at %d of %d decoded cleanly", name, v, cut, len(w.B))
				}
			}
		}
	}
}

// prefixIsValid names messages where a strict prefix of one encoding can be
// another complete encoding (none currently; kept so a future case is a
// conscious decision).
func prefixIsValid(string) bool { return false }

func TestRequestHeader(t *testing.T) {
	for _, h := range []RequestHeader{
		{Key: KeyMetadata, Version: 8, CorrelationID: 7, ClientID: "c"},
		{Key: KeyAPIVersions, Version: 3, CorrelationID: 1, ClientID: "flexible"},
		{Key: KeyAPIVersions, Version: 0, CorrelationID: 1},
	} {
		var w Writer
		h.Encode(&w)
		var got RequestHeader
		r := NewReader(w.B)
		got.Decode(r)
		if r.Err() != nil || r.Len() != 0 || got != h {
			t.Errorf("header %+v: got %+v, err %v, %d left", h, got, r.Err(), r.Len())
		}
	}
	// A null client id is legal and reads as empty.
	in, _ := hex.DecodeString("0003" + "0001" + "00000009" + "ffff")
	var h RequestHeader
	r := NewReader(in)
	h.Decode(r)
	if r.Err() != nil || h.ClientID != "" || h.CorrelationID != 9 {
		t.Errorf("null client id: %+v, %v", h, r.Err())
	}
}

func TestSupported(t *testing.T) {
	tests := []struct {
		key, version int16
		want         bool
	}{
		{KeyProduce, 2, false}, // advertised for librdkafka, but rejected
		{KeyProduce, 3, true},
		{KeyProduce, 8, true},
		{KeyProduce, 9, false},
		{KeyFetch, 3, false},
		{KeyFetch, 11, true},
		{KeyAPIVersions, 4, true},
		{KeyAPIVersions, 5, false},
		{99, 0, false},
	}
	for _, tt := range tests {
		if got := Supported(tt.key, tt.version); got != tt.want {
			t.Errorf("Supported(%d, %d) = %v, want %v", tt.key, tt.version, got, tt.want)
		}
	}
}

func TestBatchRoundTrip(t *testing.T) {
	records := []Record{
		{Key: []byte("k1"), Value: []byte("v1"), Timestamp: 1000},
		{Key: nil, Value: []byte("v2"), Timestamp: 1005},
		{Key: []byte("k3"), Value: nil, Timestamp: 990},
	}
	b := EncodeBatch(records)
	info, code := ParseBatch(b, 1<<20)
	if code != ErrNone || info.Size != len(b) || info.Count != 3 || info.MaxTimestamp != 1005 {
		t.Fatalf("ParseBatch = %+v, error %d", info, code)
	}
	SetBatchBaseOffset(b, 42)
	if _, code := ParseBatch(b, 1<<20); code != ErrNone {
		t.Fatalf("setting the base offset invalidated the batch: error %d", code)
	}
	base, got, err := DecodeBatch(b)
	if err != nil || base != 42 || !reflect.DeepEqual(got, records) {
		t.Fatalf("DecodeBatch = %d, %+v, %v", base, got, err)
	}
}

func TestParseBatchRejects(t *testing.T) {
	good := EncodeBatch([]Record{{Value: []byte("hello")}})
	mutate := func(f func(b []byte)) []byte {
		b := bytes.Clone(good)
		f(b)
		return b
	}
	tests := []struct {
		name string
		b    []byte
		max  int
		want int16
	}{
		{"good", good, 1 << 20, ErrNone},
		{"good with trailing bytes", append(bytes.Clone(good), 1, 2, 3), 1 << 20, ErrNone},
		{"empty", nil, 1 << 20, ErrCorruptMessage},
		{"header cut short", good[:30], 1 << 20, ErrCorruptMessage},
		{"body cut short", good[:len(good)-1], 1 << 20, ErrCorruptMessage},
		{"old message format", mutate(func(b []byte) { b[batchMagicAt] = 1 }), 1 << 20, ErrUnsupportedForMessageFormat},
		{"negative length", mutate(func(b []byte) { b[batchLengthAt] = 0x80 }), 1 << 20, ErrCorruptMessage},
		{"length below header size", mutate(func(b []byte) { copy(b[batchLengthAt:], []byte{0, 0, 0, 10}) }), 1 << 20, ErrCorruptMessage},
		{"over size limit", good, len(good) - 1, ErrMessageTooLarge},
		{"flipped record bit", mutate(func(b []byte) { b[len(b)-1] ^= 1 }), 1 << 20, ErrCorruptMessage},
		{"flipped crc bit", mutate(func(b []byte) { b[batchCRCAt] ^= 1 }), 1 << 20, ErrCorruptMessage},
		{"zero records", EncodeBatch(nil), 1 << 20, ErrInvalidRecord},
	}
	for _, tt := range tests {
		if _, got := ParseBatch(tt.b, tt.max); got != tt.want {
			t.Errorf("%s: error code %d, want %d", tt.name, got, tt.want)
		}
	}
	// Flags can only be tested with a correct CRC, so re-encode by hand.
	for name, flag := range map[string]byte{"control batch": attrControl, "transactional batch": attrTransactional} {
		b := mutate(func(b []byte) {
			b[batchAttributesAt+1] |= flag
			w := Writer{B: b[:batchCRCAt]}
			w.Int32(int32(crc32c(b[batchAttributesAt:])))
		})
		if _, got := ParseBatch(b, 1<<20); got != ErrInvalidRecord {
			t.Errorf("%s: error code %d, want %d", name, got, ErrInvalidRecord)
		}
	}
}

// librdkafka rejects a null record set in a fetch response, so "nothing to
// return" must be encoded with length 0, not -1.
func TestFetchResponseEncodesNoRecordsAsEmpty(t *testing.T) {
	var w Writer
	(&FetchResponse{Topics: []FetchTopicResponse{{Name: "t", Partitions: []FetchPartitionResponse{{HighWatermark: 3}}}}}).Encode(&w, 11)
	if !bytes.HasSuffix(w.B, []byte{0, 0, 0, 0}) {
		t.Fatalf("empty record set not encoded as length 0: % x", w.B[len(w.B)-8:])
	}
}
