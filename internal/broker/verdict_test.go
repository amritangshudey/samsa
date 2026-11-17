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

func TestJudgeTransactions(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	for _, name := range []string{"in", "out"} {
		if code := h.createTopic(1, name, 1); code != wire.ErrNone {
			t.Fatalf("create %s: %d", name, code)
		}
	}
	h.produceKeyed("in", 0, "k0", "k1", "k2")

	// Start txn1 and commit k0 + offset 1 via TxnOffsetCommit.
	var initResp wire.InitProducerIDResponse
	h.call(1, wire.KeyInitProducerID, 5, &wire.InitProducerIDRequest{
		Transactional: true, TransactionalID: "tx-app", TransactionTimeoutMs: 60000,
		ProducerID: -1, ProducerEpoch: -1,
	}, &initResp)
	if initResp.ErrorCode != wire.ErrNone {
		t.Fatalf("InitProducerID: %d", initResp.ErrorCode)
	}
	pid, ep := initResp.ProducerID, initResp.ProducerEpoch

	addPart := func(e int16) {
		var r wire.AddPartitionsToTxnResponse
		h.call(1, wire.KeyAddPartitionsToTxn, 5, &wire.AddPartitionsToTxnRequest{
			Transactions: []wire.AddPartitionsToTxnTransaction{{
				TransactionalID: "tx-app", ProducerID: pid, ProducerEpoch: e,
				Topics: []wire.AddPartitionsToTxnTopic{{Name: "out", Partitions: []int32{0}}},
			}},
		}, &r)
		if r.ResultsByTransaction[0].TopicResults[0].Partitions[0].ErrorCode != wire.ErrNone {
			t.Fatalf("AddPartitionsToTxn: %+v", r)
		}
	}
	produceTx := func(key string, seq int32, e int16) {
		b := wire.EncodeBatch([]wire.Record{{Key: []byte(key), Value: []byte("v")}})
		wire.SetBatchProducer(b, pid, e, seq)
		wire.SetBatchTransactional(b)
		var r wire.ProduceResponse
		h.call(1, wire.KeyProduce, 9, &wire.ProduceRequest{
			TransactionalID: "tx-app", HasTransactionalID: true, Acks: -1, TimeoutMs: 1000,
			Topics: []wire.ProduceTopic{{Name: "out", Partitions: []wire.ProducePartition{{Index: 0, Records: b}}}},
		}, &r)
		if r.Topics[0].Partitions[0].ErrorCode != wire.ErrNone {
			t.Fatalf("produce %s: %d", key, r.Topics[0].Partitions[0].ErrorCode)
		}
	}
	endTx := func(commit bool, e int16) (int64, int16) {
		var r wire.EndTxnResponse
		h.call(1, wire.KeyEndTxn, 5, &wire.EndTxnRequest{
			TransactionalID: "tx-app", ProducerID: pid, ProducerEpoch: e, Committed: commit,
		}, &r)
		if r.ErrorCode != wire.ErrNone {
			t.Fatalf("EndTxn(%v): %d", commit, r.ErrorCode)
		}
		return r.ProducerID, r.ProducerEpoch
	}

	// k0: committed in txn.
	addPart(ep)
	produceTx("k0", 0, ep)
	var addOff wire.AddOffsetsToTxnResponse
	h.call(1, wire.KeyAddOffsetsToTxn, 3, &wire.AddOffsetsToTxnRequest{
		TransactionalID: "tx-app", ProducerID: pid, ProducerEpoch: ep, GroupID: "app",
	}, &addOff)
	var toc wire.TxnOffsetCommitResponse
	h.call(1, wire.KeyTxnOffsetCommit, 3, &wire.TxnOffsetCommitRequest{
		TransactionalID: "tx-app", GroupID: "app", ProducerID: pid, ProducerEpoch: ep,
		GenerationID: -1,
		Topics: []wire.TxnOffsetCommitTopic{{Name: "in", Partitions: []wire.TxnOffsetCommitPartition{
			{Index: 0, Offset: 2, LeaderEpoch: -1},
		}}},
	}, &toc)
	pid, ep = endTx(true, ep)

	// k1: written in a second txn that ABORTS (and group already committed offset 2 past k1!).
	addPart(ep)
	produceTx("k1", 0, ep)
	_, ep = endTx(false, ep)

	// k2: written in an open (uncommitted) txn, not committed past in input group.
	addPart(ep)
	produceTx("k2", 0, ep)

	v, err := Judge(h.disk, h.cfg, Pipeline{In: "in", Out: "out", Group: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if v.Inputs != 3 || v.Outputs != 1 || v.Once != 1 || v.Lost != 1 || v.Pending != 1 || v.Unmatched != 0 {
		t.Fatalf("unexpected txn verdict: %+v", v)
	}
}

func TestJudgeShareGroup(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	for _, name := range []string{"in", "out"} {
		if code := h.createTopic(1, name, 1); code != wire.ErrNone {
			t.Fatalf("create %s: %d", name, code)
		}
	}
	h.produceKeyed("in", 0, "s0", "s1", "s2")
	inID := h.node.st.topics["in"].id

	// Member joins share group "sg", acquires 0..2, acks s0 (Accept), rejects s1 (Reject), leaves s2 acquired (Pending).
	var hb wire.ShareGroupHeartbeatResponse
	h.call(1, wire.KeyShareGroupHeartbeat, 1, &wire.ShareGroupHeartbeatRequest{
		GroupID: "sg", MemberID: "m1", MemberEpoch: 0, SubscribedTopics: []string{"in"},
	}, &hb)
	var sf wire.ShareFetchResponse
	h.call(1, wire.KeyShareFetch, 2, &wire.ShareFetchRequest{
		GroupID: "sg", MemberID: "m1", ShareSessionEpoch: 0, MaxRecords: 3,
		Topics: []wire.ShareFetchTopic{{TopicID: inID, Partitions: []wire.ShareFetchPartition{{Index: 0}}}},
	}, &sf)
	var ack wire.ShareAcknowledgeResponse
	h.call(1, wire.KeyShareAcknowledge, 2, &wire.ShareAcknowledgeRequest{
		GroupID: "sg", MemberID: "m1", ShareSessionEpoch: 1,
		Topics: []wire.ShareFetchTopic{{TopicID: inID, Partitions: []wire.ShareFetchPartition{{
			Index: 0,
			AckBatches: []wire.ShareAckBatch{
				{FirstOffset: 0, LastOffset: 0, AcknowledgeTypes: []int8{wire.AckAccept}},
				{FirstOffset: 1, LastOffset: 1, AcknowledgeTypes: []int8{wire.AckReject}},
			},
		}}}},
	}, &ack)
	// Application only wrote s0 to "out" (s1 was rejected/archived without writing!).
	h.produceKeyed("out", 0, "s0")

	v, err := Judge(h.disk, h.cfg, Pipeline{In: "in", Out: "out", Group: "sg"})
	if err != nil {
		t.Fatal(err)
	}
	if v.Inputs != 3 || v.Outputs != 1 || v.Once != 1 || v.Lost != 1 || v.Pending != 1 {
		t.Fatalf("unexpected share group verdict: %+v", v)
	}
}
