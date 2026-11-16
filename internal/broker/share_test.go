package broker

import (
	"slices"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/wire"
)

func TestShareGroupLifecycleAcquireAckAndExpiry(t *testing.T) {
	h := newHarness(t, Config{
		ConsumerHeartbeatInterval:   100 * time.Millisecond,
		ConsumerSessionTimeout:      2 * time.Second,
		ShareAcquisitionLockTimeout: 500 * time.Millisecond,
	})
	h.open(1)
	h.open(2)
	h.createTopic(1, "q", 1)

	var md wire.MetadataResponse
	h.call(1, wire.KeyMetadata, 12, &wire.MetadataRequest{Topics: []string{"q"}}, &md)
	tid := md.Topics[0].TopicID

	// Produce 3 records in one batch (offsets 0, 1, 2).
	batch := wire.EncodeBatch([]wire.Record{{Value: []byte("a")}, {Value: []byte("b")}, {Value: []byte("c")}})
	var pr wire.ProduceResponse
	h.call(1, wire.KeyProduce, 9, &wire.ProduceRequest{
		Acks: -1, TimeoutMs: 1000,
		Topics: []wire.ProduceTopic{{Name: "q", Partitions: []wire.ProducePartition{{Index: 0, Records: batch}}}},
	}, &pr)
	if pr.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("produce: %+v", pr)
	}

	// Two share consumers join group "sg".
	var hbA, hbB wire.ShareGroupHeartbeatResponse
	h.call(1, wire.KeyShareGroupHeartbeat, 1, &wire.ShareGroupHeartbeatRequest{
		GroupID: "sg", MemberID: "mA", MemberEpoch: 0, SubscribedTopics: []string{"q"},
	}, &hbA)
	h.call(2, wire.KeyShareGroupHeartbeat, 1, &wire.ShareGroupHeartbeatRequest{
		GroupID: "sg", MemberID: "mB", MemberEpoch: 0, SubscribedTopics: []string{"q"},
	}, &hbB)
	if hbA.ErrorCode != 0 || !hbA.HasAssignment || len(hbA.Assignment) != 1 {
		t.Fatalf("mA join: %+v", hbA)
	}
	if hbB.ErrorCode != 0 || !hbB.HasAssignment || len(hbB.Assignment) != 1 {
		t.Fatalf("mB join: %+v", hbB)
	}

	// ShareGroupDescribe reports Stable, simple assignor, 2 members.
	var desc wire.ShareGroupDescribeResponse
	h.call(1, wire.KeyShareGroupDescribe, 1, &wire.ShareGroupDescribeRequest{Groups: []string{"sg"}}, &desc)
	if len(desc.Groups) != 1 || desc.Groups[0].State != "Stable" || desc.Groups[0].AssignorName != "simple" || len(desc.Groups[0].Members) != 2 {
		t.Fatalf("ShareGroupDescribe: %+v", desc.Groups)
	}

	// ListGroups reports Type: "share".
	var lg wire.ListGroupsResponse
	h.call(1, wire.KeyListGroups, 5, &wire.ListGroupsRequest{Types: []string{"share"}}, &lg)
	if len(lg.Groups) != 1 || lg.Groups[0].ID != "sg" || lg.Groups[0].Type != "share" || lg.Groups[0].State != "Stable" {
		t.Fatalf("ListGroups: %+v", lg.Groups)
	}

	// mA fetches up to 2 records (offsets 0..1) with DeliveryCount = 1.
	var sfA wire.ShareFetchResponse
	h.call(1, wire.KeyShareFetch, 2, &wire.ShareFetchRequest{
		GroupID: "sg", MemberID: "mA", ShareSessionEpoch: 0, MaxBytes: 1 << 20, MaxRecords: 2,
		Topics: []wire.ShareFetchTopic{{TopicID: tid, Partitions: []wire.ShareFetchPartition{{Index: 0}}}},
	}, &sfA)
	if sfA.ErrorCode != 0 || len(sfA.Responses) != 1 || len(sfA.Responses[0].Partitions[0].Acquired) != 1 {
		t.Fatalf("mA ShareFetch: %+v", sfA)
	}
	if acq := sfA.Responses[0].Partitions[0].Acquired[0]; acq.FirstOffset != 0 || acq.LastOffset != 1 || acq.DeliveryCount != 1 {
		t.Fatalf("mA acquired: %+v, want 0..1 del=1", acq)
	}

	// mB fetches remaining record (offset 2) with DeliveryCount = 1.
	var sfB wire.ShareFetchResponse
	h.call(2, wire.KeyShareFetch, 2, &wire.ShareFetchRequest{
		GroupID: "sg", MemberID: "mB", ShareSessionEpoch: 0, MaxBytes: 1 << 20, MaxRecords: 10,
		Topics: []wire.ShareFetchTopic{{TopicID: tid, Partitions: []wire.ShareFetchPartition{{Index: 0}}}},
	}, &sfB)
	if sfB.ErrorCode != 0 || len(sfB.Responses) != 1 || sfB.Responses[0].Partitions[0].Acquired[0] != (wire.AcquiredRange{FirstOffset: 2, LastOffset: 2, DeliveryCount: 1}) {
		t.Fatalf("mB ShareFetch: %+v", sfB)
	}

	// mB tries to ack offset 0 (owned by mA): refused with ErrInvalidRecordState.
	var badAck wire.ShareAcknowledgeResponse
	h.call(2, wire.KeyShareAcknowledge, 2, &wire.ShareAcknowledgeRequest{
		GroupID: "sg", MemberID: "mB", ShareSessionEpoch: 1,
		Topics: []wire.ShareFetchTopic{{TopicID: tid, Partitions: []wire.ShareFetchPartition{{Index: 0, AckBatches: []wire.ShareAckBatch{{FirstOffset: 0, LastOffset: 0, AcknowledgeTypes: []int8{wire.AckAccept}}}}}}},
	}, &badAck)
	if badAck.Responses[0].Partitions[0].ErrorCode != wire.ErrInvalidRecordState {
		t.Fatalf("ack by non-owner: got %d, want %d", badAck.Responses[0].Partitions[0].ErrorCode, wire.ErrInvalidRecordState)
	}

	// mA accepts offset 0 and releases offset 1 via ShareAcknowledge.
	var ackA wire.ShareAcknowledgeResponse
	h.call(1, wire.KeyShareAcknowledge, 2, &wire.ShareAcknowledgeRequest{
		GroupID: "sg", MemberID: "mA", ShareSessionEpoch: 1,
		Topics: []wire.ShareFetchTopic{{TopicID: tid, Partitions: []wire.ShareFetchPartition{{Index: 0, AckBatches: []wire.ShareAckBatch{
			{FirstOffset: 0, LastOffset: 1, AcknowledgeTypes: []int8{wire.AckAccept, wire.AckRelease}},
		}}}}},
	}, &ackA)
	if ackA.ErrorCode != 0 || ackA.Responses[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("mA ack: %+v", ackA)
	}

	// Snapshot v11 round-trip preserves share group state and hash.
	snap := h.node.st.encode()
	dec, err := decodeState(snap)
	if err != nil {
		t.Fatalf("decodeState v11: %v", err)
	}
	if dec.hash() != h.node.st.hash() {
		t.Fatalf("snapshot v11 hash mismatch: %08x vs %08x", dec.hash(), h.node.st.hash())
	}

	// mB parks a ShareFetch waiting for records, and receives offset 1 (released, DeliveryCount = 2).
	var sfB2 wire.ShareFetchResponse
	h.call(2, wire.KeyShareFetch, 2, &wire.ShareFetchRequest{
		GroupID: "sg", MemberID: "mB", ShareSessionEpoch: 2, MaxWaitMs: 200, MaxBytes: 1 << 20, MaxRecords: 10,
	}, &sfB2)
	if sfB2.ErrorCode != 0 || len(sfB2.Responses) != 1 || sfB2.Responses[0].Partitions[0].Acquired[0] != (wire.AcquiredRange{FirstOffset: 1, LastOffset: 1, DeliveryCount: 2}) {
		t.Fatalf("mB redelivery of released offset 1: %+v", sfB2)
	}

	// Advance past ShareAcquisitionLockTimeout (500ms): mB's locks on 1 and 2 expire!
	h.advance(600 * time.Millisecond)

	// mA fetches again (session epoch 2) and acquires 1 (DeliveryCount 3) and 2 (DeliveryCount 2).
	var sfA2 wire.ShareFetchResponse
	h.call(1, wire.KeyShareFetch, 2, &wire.ShareFetchRequest{
		GroupID: "sg", MemberID: "mA", ShareSessionEpoch: 2, MaxBytes: 1 << 20, MaxRecords: 10,
	}, &sfA2)
	if sfA2.ErrorCode != 0 || len(sfA2.Responses) != 1 {
		t.Fatalf("mA fetch after expiry: %+v", sfA2)
	}
	wantAcq := []wire.AcquiredRange{{FirstOffset: 1, LastOffset: 1, DeliveryCount: 3}, {FirstOffset: 2, LastOffset: 2, DeliveryCount: 2}}
	if !slices.Equal(sfA2.Responses[0].Partitions[0].Acquired, wantAcq) {
		t.Fatalf("mA acquired after expiry: %+v, want %+v", sfA2.Responses[0].Partitions[0].Acquired, wantAcq)
	}

	// Wrong ShareSessionEpoch returns ErrInvalidShareSessionEpoch.
	var badSess wire.ShareFetchResponse
	h.call(1, wire.KeyShareFetch, 2, &wire.ShareFetchRequest{
		GroupID: "sg", MemberID: "mA", ShareSessionEpoch: 99, MaxBytes: 1 << 20, MaxRecords: 10,
	}, &badSess)
	if badSess.ErrorCode != wire.ErrInvalidShareSessionEpoch {
		t.Fatalf("wrong session epoch: got %d, want %d", badSess.ErrorCode, wire.ErrInvalidShareSessionEpoch)
	}

	// Mixing protocols (JoinGroup or ConsumerGroupHeartbeat on share group) fails.
	var cg wire.ConsumerGroupHeartbeatResponse
	h.call(1, wire.KeyConsumerGroupHeartbeat, 1, &wire.ConsumerGroupHeartbeatRequest{
		GroupID: "sg", MemberID: "c", MemberEpoch: 0, RebalanceTimeoutMs: 500, SubscribedTopics: []string{"q"},
	}, &cg)
	if cg.ErrorCode != wire.ErrInconsistentGroupProtocol {
		t.Fatalf("consumer heartbeat on share group: got %d, want %d", cg.ErrorCode, wire.ErrInconsistentGroupProtocol)
	}
}
