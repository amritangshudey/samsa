package wire

import (
	"hash/crc32"
	"reflect"
	"testing"
)

func crc32c(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// FuzzDecodeMessage feeds arbitrary bytes to every message decoder at every
// supported version. A decoder must not panic, and whatever it accepts must
// survive encode and decode unchanged.
func FuzzDecodeMessage(f *testing.F) {
	type kind struct {
		key   int16
		fresh func() Message
	}
	var kinds []kind
	names := []string{ // fixed order: the fuzzer's corpus refers to kinds by index
		"ApiVersionsRequest", "ApiVersionsResponse", "MetadataRequest", "MetadataResponse",
		"CreateTopicsRequest", "CreateTopicsResponse", "ProduceRequest", "ProduceResponse",
		"FetchRequest", "FetchResponse", "ListOffsetsRequest", "ListOffsetsResponse",
	}
	all := messages()
	for i, name := range names {
		m := all[name]
		kinds = append(kinds, kind{m.key, m.fresh})
		api, _ := LookupAPI(m.key)
		for v := api.Min; v <= api.Max; v++ {
			var w Writer
			m.value.Encode(&w, v)
			f.Add(uint8(i), v, w.B)
		}
	}
	f.Fuzz(func(t *testing.T, which uint8, version int16, data []byte) {
		k := kinds[int(which)%len(kinds)]
		api, _ := LookupAPI(k.key)
		if version < api.Min || version > api.Max {
			return
		}
		first := k.fresh()
		r := NewReader(data)
		first.Decode(r, version)
		if r.Err() != nil {
			return
		}
		var w Writer
		first.Encode(&w, version)
		second := k.fresh()
		r = NewReader(w.B)
		second.Decode(r, version)
		if r.Err() != nil || r.Len() != 0 {
			t.Fatalf("re-decoding our own encoding failed: %v, %d bytes left", r.Err(), r.Len())
		}
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("not stable under encode/decode:\n first %+v\nsecond %+v", first, second)
		}
	})
}

// FuzzRequestHeader checks the header decoder on arbitrary bytes.
func FuzzRequestHeader(f *testing.F) {
	var w Writer
	(&RequestHeader{Key: KeyAPIVersions, Version: 3, CorrelationID: 1, ClientID: "x"}).Encode(&w)
	f.Add(w.B)
	f.Add([]byte{})
	f.Fuzz(func(_ *testing.T, data []byte) {
		var h RequestHeader
		h.Decode(NewReader(data))
	})
}

// FuzzBatch checks that batch validation and record decoding never panic,
// and that an accepted batch stays accepted when its base offset is set.
func FuzzBatch(f *testing.F) {
	f.Add(EncodeBatch([]Record{{Key: []byte("k"), Value: []byte("v"), Timestamp: 5}}))
	f.Add(EncodeBatch(nil))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		info, code := ParseBatch(data, 1<<16)
		if code != ErrNone {
			return
		}
		if info.Size < BatchOverhead || info.Size > len(data) || info.Count < 1 {
			t.Fatalf("accepted batch with impossible info %+v", info)
		}
		b := append([]byte(nil), data[:info.Size]...)
		SetBatchBaseOffset(b, 1<<40)
		if _, code := ParseBatch(b, 1<<16); code != ErrNone {
			t.Fatalf("base offset changed validity: %d", code)
		}
		DecodeBatch(b) // may fail (compressed or malformed records) but must not panic
	})
}
