package broker

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strings"
	"testing"

	"github.com/amritangshudey/samsa/internal/wire"
)

func keyed(keys ...string) []byte {
	var recs []wire.Record
	for _, k := range keys {
		recs = append(recs, wire.Record{Key: []byte(k), Value: []byte("v")})
	}
	return wire.EncodeBatch(recs)
}

func (h *harness) produceKeyed(topic string, part int32, keys ...string) {
	h.t.Helper()
	var resp wire.ProduceResponse
	h.call(1, wire.KeyProduce, 8, produceReq(topic, part, keyed(keys...)), &resp)
	if code := resp.Topics[0].Partitions[0].ErrorCode; code != wire.ErrNone {
		h.t.Fatalf("produce: %d", code)
	}
}

// commitAs commits offset for partition part of topic "in", as a consumer
// outside any generation (generation -1), the way a manual commit does.
func (h *harness) commitIn(part int32, offset int64) {
	h.t.Helper()
	var resp wire.OffsetCommitResponse
	h.call(1, wire.KeyOffsetCommit, 7, &wire.OffsetCommitRequest{GroupID: "app", GenerationID: -1,
		Topics: []wire.OffsetCommitTopic{{Name: "in", Partitions: []wire.OffsetCommitPartition{{Index: part, Offset: offset, LeaderEpoch: -1}}}}}, &resp)
	if code := resp.Topics[0].Partitions[0].ErrorCode; code != wire.ErrNone {
		h.t.Fatalf("commit: %d", code)
	}
}

func TestJudge(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	for _, name := range []string{"in", "out"} {
		if code := h.createTopic(1, name, 2); code != wire.ErrNone {
			t.Fatalf("create %s: %d", name, code)
		}
	}
	// in/0: a0..a4, in/1: b0..b4.
	for p, prefix := range []string{"a", "b"} {
		for i := range 5 {
			h.produceKeyed("in", int32(p), fmt.Sprintf("%s%d", prefix, i))
		}
	}
	// The application: a0, a1 once; a2 twice; a3, a4 never, although the
	// group committed past a3; b0..b2 once, b3, b4 never, not committed.
	h.produceKeyed("out", 0, "a0", "a1", "a2")
	h.produceKeyed("out", 1, "a2", "b0", "b1", "b2", "stray")
	h.commitIn(0, 4)
	h.commitIn(1, 3)

	v, err := Judge(h.disk, h.cfg, Pipeline{In: "in", Out: "out", Group: "app"})
	if err != nil {
		t.Fatal(err)
	}
	got := fmt.Sprintf("%d %d once=%d dup=%d lost=%d pending=%d unmatched=%d lost=%v dup=%v pending=%v",
		v.Inputs, v.Outputs, v.Once, v.Duplicated, v.Lost, v.Pending, v.Unmatched, v.LostRecords, v.DuplicatedRecords, v.PendingRecords)
	want := "10 8 once=5 dup=1 lost=1 pending=3 unmatched=1 lost=[{0 3 a3 0}] dup=[{0 2 a2 2}] pending=[{0 4 a4 0} {1 3 b3 0} {1 4 b4 0}]"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	// Without a group every record never output is lost.
	v, err = Judge(h.disk, h.cfg, Pipeline{In: "in", Out: "out"})
	if err != nil || v.Lost != 4 || v.Pending != 0 {
		t.Fatalf("no group: %+v, %v", v, err)
	}
}

func TestJudgeRefuses(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		build      func(h *harness)
	}{
		{"no such topic", `no topic "out"`, func(h *harness) {}},
		{"duplicate input key", "keys must be unique", func(h *harness) {
			h.produceKeyed("in", 0, "k", "k")
			h.createTopic(1, "out", 1)
		}},
		{"unkeyed input", "has no key", func(h *harness) {
			var resp wire.ProduceResponse
			h.call(1, wire.KeyProduce, 8, produceReq("in", 0, wire.EncodeBatch([]wire.Record{{Value: []byte("v")}})), &resp)
			h.createTopic(1, "out", 1)
		}},
		{"compressed input", "uncompressed", func(h *harness) {
			b := keyed("k")
			b[22] |= 1 // attributes: gzip; the broker stores the batch as sent
			binary.BigEndian.PutUint32(b[17:], crc32.Checksum(b[21:], crc32.MakeTable(crc32.Castagnoli)))
			var resp wire.ProduceResponse
			h.call(1, wire.KeyProduce, 8, produceReq("in", 0, b), &resp)
			if resp.Topics[0].Partitions[0].ErrorCode != wire.ErrNone {
				h.t.Fatalf("produce of a compressed batch: %d", resp.Topics[0].Partitions[0].ErrorCode)
			}
			h.createTopic(1, "out", 1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, Config{})
			h.open(1)
			h.createTopic(1, "in", 1)
			tc.build(h)
			_, err := Judge(h.disk, h.cfg, Pipeline{In: "in", Out: "out"})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error about %q", err, tc.want)
			}
		})
	}
}
