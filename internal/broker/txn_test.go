package broker

import (
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/wire"
)

func TestTransactionLifecycleCommitAbortTimeoutAndKIP890(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	h.createTopic(1, "tx-topic", 1)

	// FindCoordinator(KeyType=1, TRANSACTION) returns the leader.
	var fc wire.FindCoordinatorResponse
	h.call(1, wire.KeyFindCoord, 4, &wire.FindCoordinatorRequest{KeyType: 1, Keys: []string{"tx-1"}}, &fc)
	if len(fc.Coordinators) != 1 || fc.Coordinators[0].ErrorCode != 0 || fc.Coordinators[0].NodeID != 1 {
		t.Fatalf("FindCoordinator txn: %+v", fc)
	}

	// InitProducerId(TransactionalID="tx-1") allocates PID and epoch 0.
	var ip wire.InitProducerIDResponse
	h.call(1, wire.KeyInitProducerID, 5, &wire.InitProducerIDRequest{
		Transactional:        true,
		TransactionalID:      "tx-1",
		TransactionTimeoutMs: 500,
		ProducerID:           -1,
		ProducerEpoch:        -1,
	}, &ip)
	if ip.ErrorCode != 0 || ip.ProducerID < 0 || ip.ProducerEpoch != 0 {
		t.Fatalf("InitProducerId: %+v", ip)
	}
	pid, ep := ip.ProducerID, ip.ProducerEpoch

	// AddPartitionsToTxn v5 + VerifyOnly check.
	var ap wire.AddPartitionsToTxnResponse
	h.call(1, wire.KeyAddPartitionsToTxn, 5, &wire.AddPartitionsToTxnRequest{
		Transactions: []wire.AddPartitionsToTxnTransaction{{
			TransactionalID: "tx-1", ProducerID: pid, ProducerEpoch: ep,
			Topics: []wire.AddPartitionsToTxnTopic{{Name: "tx-topic", Partitions: []int32{0}}},
		}},
	}, &ap)
	if ap.ErrorCode != 0 || ap.ResultsByTransaction[0].TopicResults[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("AddPartitionsToTxn: %+v", ap)
	}

	// Produce one transactional batch (offset 0).
	b0 := wire.EncodeBatch([]wire.Record{{Key: []byte("k0"), Value: []byte("v0")}})
	b0[22] |= 0x10 // attrTransactional
	wire.SetBatchProducer(b0, pid, ep, 0)
	var pr wire.ProduceResponse
	h.call(1, wire.KeyProduce, 9, &wire.ProduceRequest{
		TransactionalID: "tx-1", HasTransactionalID: true, Acks: -1, TimeoutMs: 1000,
		Topics: []wire.ProduceTopic{{Name: "tx-topic", Partitions: []wire.ProducePartition{{Index: 0, Records: b0}}}},
	}, &pr)
	if pr.Topics[0].Partitions[0].ErrorCode != 0 || pr.Topics[0].Partitions[0].BaseOffset != 0 {
		t.Fatalf("Produce txn 1: %+v", pr)
	}

	// READ_COMMITTED Fetch and ListOffsets must not see uncommitted offset 0 (LSO == 0).
	var frRC wire.FetchResponse
	h.call(1, wire.KeyFetch, 12, &wire.FetchRequest{
		MaxBytes: 1 << 20, IsolationLevel: 1,
		Topics: []wire.FetchTopic{{Name: "tx-topic", Partitions: []wire.FetchPartition{{Index: 0, Offset: 0, MaxBytes: 1 << 20}}}},
	}, &frRC)
	fpr := frRC.Topics[0].Partitions[0]
	if fpr.LastStableOffset != 0 || len(fpr.Records) != 0 {
		t.Fatalf("READ_COMMITTED saw uncommitted data: lso=%d recs=%d", fpr.LastStableOffset, len(fpr.Records))
	}

	var lo wire.ListOffsetsResponse
	h.call(1, wire.KeyListOffsets, 6, &wire.ListOffsetsRequest{
		IsolationLevel: 1,
		Topics:         []wire.ListOffsetsTopic{{Name: "tx-topic", Partitions: []wire.ListOffsetsPartition{{Index: 0, Timestamp: wire.TimestampLatest}}}},
	}, &lo)
	if lo.Topics[0].Partitions[0].Offset != 0 {
		t.Fatalf("READ_COMMITTED ListOffsets Latest = %d, want 0", lo.Topics[0].Partitions[0].Offset)
	}

	// Stage transactional offset commit and commit via EndTxn v5 (KIP-890 epoch bump).
	var ao wire.AddOffsetsToTxnResponse
	h.call(1, wire.KeyAddOffsetsToTxn, 4, &wire.AddOffsetsToTxnRequest{
		TransactionalID: "tx-1", ProducerID: pid, ProducerEpoch: ep, GroupID: "tx-grp",
	}, &ao)
	if ao.ErrorCode != 0 {
		t.Fatalf("AddOffsetsToTxn: %+v", ao)
	}
	var toc wire.TxnOffsetCommitResponse
	h.call(1, wire.KeyTxnOffsetCommit, 5, &wire.TxnOffsetCommitRequest{
		TransactionalID: "tx-1", GroupID: "tx-grp", ProducerID: pid, ProducerEpoch: ep, GenerationID: -1,
		Topics: []wire.TxnOffsetCommitTopic{{Name: "tx-topic", Partitions: []wire.TxnOffsetCommitPartition{{Index: 0, Offset: 1, LeaderEpoch: -1}}}},
	}, &toc)
	if toc.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("TxnOffsetCommit: %+v", toc)
	}

	// Before EndTxn, OffsetFetch must not see the staged offset.
	var of1 wire.OffsetFetchResponse
	h.call(1, wire.KeyOffsetFetch, 8, &wire.OffsetFetchRequest{
		Groups: []wire.OffsetFetchGroup{{GroupID: "tx-grp", Topics: []wire.OffsetFetchTopic{{Name: "tx-topic", Partitions: []int32{0}}}}},
	}, &of1)
	if of1.Groups[0].Topics[0].Partitions[0].Offset != -1 {
		t.Fatalf("OffsetFetch before commit = %d, want -1", of1.Groups[0].Topics[0].Partitions[0].Offset)
	}

	var et1 wire.EndTxnResponse
	h.call(1, wire.KeyEndTxn, 5, &wire.EndTxnRequest{
		TransactionalID: "tx-1", ProducerID: pid, ProducerEpoch: ep, Committed: true,
	}, &et1)
	if et1.ErrorCode != 0 || et1.ProducerID != pid || et1.ProducerEpoch != ep+1 {
		t.Fatalf("EndTxn v5 commit: %+v (want epoch %d)", et1, ep+1)
	}
	ep = et1.ProducerEpoch

	// After commit: OffsetFetch sees 1, READ_COMMITTED sees offset 0 + commit marker (LSO == 2).
	var of2 wire.OffsetFetchResponse
	h.call(1, wire.KeyOffsetFetch, 8, &wire.OffsetFetchRequest{
		Groups: []wire.OffsetFetchGroup{{GroupID: "tx-grp", Topics: []wire.OffsetFetchTopic{{Name: "tx-topic", Partitions: []int32{0}}}}},
	}, &of2)
	if of2.Groups[0].Topics[0].Partitions[0].Offset != 1 {
		t.Fatalf("OffsetFetch after commit = %d, want 1", of2.Groups[0].Topics[0].Partitions[0].Offset)
	}

	h.call(1, wire.KeyFetch, 12, &wire.FetchRequest{
		MaxBytes: 1 << 20, IsolationLevel: 1,
		Topics: []wire.FetchTopic{{Name: "tx-topic", Partitions: []wire.FetchPartition{{Index: 0, Offset: 0, MaxBytes: 1 << 20}}}},
	}, &frRC)
	if frRC.Topics[0].Partitions[0].LastStableOffset != 2 || len(frRC.Topics[0].Partitions[0].Records) == 0 {
		t.Fatalf("READ_COMMITTED after commit: lso=%d recs=%d", frRC.Topics[0].Partitions[0].LastStableOffset, len(frRC.Topics[0].Partitions[0].Records))
	}

	// Second transaction: produce at offset 2, then let TransactionTimeoutMs (500ms) abort it!
	h.call(1, wire.KeyAddPartitionsToTxn, 5, &wire.AddPartitionsToTxnRequest{
		Transactions: []wire.AddPartitionsToTxnTransaction{{
			TransactionalID: "tx-1", ProducerID: pid, ProducerEpoch: ep,
			Topics: []wire.AddPartitionsToTxnTopic{{Name: "tx-topic", Partitions: []int32{0}}},
		}},
	}, &ap)
	b1 := wire.EncodeBatch([]wire.Record{{Key: []byte("k1"), Value: []byte("aborted")}})
	b1[22] |= 0x10
	wire.SetBatchProducer(b1, pid, ep, 0)
	h.call(1, wire.KeyProduce, 9, &wire.ProduceRequest{
		TransactionalID: "tx-1", HasTransactionalID: true, Acks: -1, TimeoutMs: 1000,
		Topics: []wire.ProduceTopic{{Name: "tx-topic", Partitions: []wire.ProducePartition{{Index: 0, Records: b1}}}},
	}, &pr)
	if pr.Topics[0].Partitions[0].ErrorCode != 0 || pr.Topics[0].Partitions[0].BaseOffset != 2 {
		t.Fatalf("Produce txn 2: %+v", pr)
	}

	// Snapshot v12 round-trip while an open transaction exists.
	snap := h.node.st.encode()
	dec, err := decodeState(snap)
	if err != nil {
		t.Fatalf("decodeState v12: %v", err)
	}
	if dec.hash() != h.node.st.hash() {
		t.Fatalf("snapshot v12 hash mismatch: %08x vs %08x", dec.hash(), h.node.st.hash())
	}

	// Advance clock past TransactionTimeoutMs (500ms) -> auto-abort + epoch bump.
	h.advance(600 * time.Millisecond)

	h.call(1, wire.KeyFetch, 12, &wire.FetchRequest{
		MaxBytes: 1 << 20, IsolationLevel: 1,
		Topics: []wire.FetchTopic{{Name: "tx-topic", Partitions: []wire.FetchPartition{{Index: 0, Offset: 2, MaxBytes: 1 << 20}}}},
	}, &frRC)
	fpr = frRC.Topics[0].Partitions[0]
	if fpr.LastStableOffset != 4 || len(fpr.AbortedTransactions) != 1 || fpr.AbortedTransactions[0].FirstOffset != 2 || fpr.AbortedTransactions[0].ProducerID != pid {
		t.Fatalf("READ_COMMITTED after timeout abort: lso=%d aborted=%+v", fpr.LastStableOffset, fpr.AbortedTransactions)
	}

	// Stale epoch is fenced.
	var stale wire.EndTxnResponse
	h.call(1, wire.KeyEndTxn, 5, &wire.EndTxnRequest{
		TransactionalID: "tx-1", ProducerID: pid, ProducerEpoch: ep, Committed: true,
	}, &stale)
	if stale.ErrorCode != wire.ErrProducerFenced {
		t.Fatalf("stale EndTxn = %d, want ErrProducerFenced (%d)", stale.ErrorCode, wire.ErrProducerFenced)
	}
}
