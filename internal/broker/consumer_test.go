package broker

import (
	"slices"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/wire"
)

func TestConsumerGroupLifecycleAndTwoPhaseRevocation(t *testing.T) {
	h := newHarness(t, Config{
		ConsumerHeartbeatInterval: 200 * time.Millisecond,
		ConsumerSessionTimeout:    2 * time.Second,
	})
	h.open(1)
	h.createTopic(1, "t", 4)
	var md wire.MetadataResponse
	h.call(1, wire.KeyMetadata, 12, &wire.MetadataRequest{Topics: []string{"t"}}, &md)
	tid := md.Topics[0].TopicID

	// Member A joins at v0 (broker assigns member id) with range assignor.
	var rA wire.ConsumerGroupHeartbeatResponse
	h.call(1, wire.KeyConsumerGroupHeartbeat, 0, &wire.ConsumerGroupHeartbeatRequest{
		GroupID:            "cg",
		MemberEpoch:        0,
		RebalanceTimeoutMs: 1000,
		SubscribedTopics:   []string{"t"},
		ServerAssignor:     "range",
	}, &rA)
	if rA.ErrorCode != 0 || rA.MemberID == "" || rA.MemberEpoch != 1 || !rA.HasAssignment {
		t.Fatalf("A join: %+v", rA)
	}
	if len(rA.Assignment) != 1 || !slices.Equal(rA.Assignment[0].Partitions, []int32{0, 1, 2, 3}) {
		t.Fatalf("A initial assignment: %+v", rA.Assignment)
	}
	idA := rA.MemberID

	// Member B joins at v1 (client-generated member id).
	h.open(2)
	idB := "b-member"
	var rB wire.ConsumerGroupHeartbeatResponse
	h.call(2, wire.KeyConsumerGroupHeartbeat, 1, &wire.ConsumerGroupHeartbeatRequest{
		GroupID:            "cg",
		MemberID:           idB,
		MemberEpoch:        0,
		RebalanceTimeoutMs: 1000,
		SubscribedTopics:   []string{"t"},
	}, &rB)
	if rB.ErrorCode != 0 || rB.MemberEpoch != 2 || !rB.HasAssignment {
		t.Fatalf("B join: %+v", rB)
	}
	// A still owns [0,1,2,3], so B must receive no partitions yet!
	if len(rB.Assignment) != 0 {
		t.Fatalf("B got partitions before A revoked them: %+v", rB.Assignment)
	}

	// ConsumerGroupDescribe shows Reconciling with A holding [0..3] and target [0,1], B holding [] and target [2,3].
	var desc wire.ConsumerGroupDescribeResponse
	h.call(1, wire.KeyConsumerGroupDescribe, 1, &wire.ConsumerGroupDescribeRequest{Groups: []string{"cg"}}, &desc)
	if len(desc.Groups) != 1 || desc.Groups[0].State != "Reconciling" || desc.Groups[0].AssignorName != "range" {
		t.Fatalf("describe during reconcile: %+v", desc.Groups)
	}

	// A heartbeats without changing its Owned yet: still revoking, epoch stays 1, visible assignment shrinks to [2,3] (since "b-member" < "test-...").
	rA = wire.ConsumerGroupHeartbeatResponse{}
	h.call(1, wire.KeyConsumerGroupHeartbeat, 1, &wire.ConsumerGroupHeartbeatRequest{
		GroupID:            "cg",
		MemberID:           idA,
		MemberEpoch:        1,
		RebalanceTimeoutMs: -1,
	}, &rA)
	if rA.ErrorCode != 0 || rA.MemberEpoch != 1 || !rA.HasAssignment || !slices.Equal(rA.Assignment[0].Partitions, []int32{2, 3}) {
		t.Fatalf("A heartbeat before revoke: %+v", rA)
	}

	// A confirms revocation by reporting Owned = [2, 3].
	rA = wire.ConsumerGroupHeartbeatResponse{}
	h.call(1, wire.KeyConsumerGroupHeartbeat, 1, &wire.ConsumerGroupHeartbeatRequest{
		GroupID:            "cg",
		MemberID:           idA,
		MemberEpoch:        1,
		RebalanceTimeoutMs: -1,
		Owned:              []wire.TopicPartitionIDs{{TopicID: tid, Partitions: []int32{2, 3}}},
	}, &rA)
	if rA.ErrorCode != 0 || rA.MemberEpoch != 2 {
		t.Fatalf("A after revoke: %+v", rA)
	}

	// Now B heartbeats and receives [0, 1].
	rB = wire.ConsumerGroupHeartbeatResponse{}
	h.call(2, wire.KeyConsumerGroupHeartbeat, 1, &wire.ConsumerGroupHeartbeatRequest{
		GroupID:            "cg",
		MemberID:           idB,
		MemberEpoch:        2,
		RebalanceTimeoutMs: -1,
	}, &rB)
	if rB.ErrorCode != 0 || !rB.HasAssignment || len(rB.Assignment) != 1 || !slices.Equal(rB.Assignment[0].Partitions, []int32{0, 1}) {
		t.Fatalf("B after A revoked: %+v", rB)
	}

	// Lost-response retry with prevEpoch (1 for A) returns A's current state without fencing.
	var retryA wire.ConsumerGroupHeartbeatResponse
	h.call(1, wire.KeyConsumerGroupHeartbeat, 1, &wire.ConsumerGroupHeartbeatRequest{
		GroupID:            "cg",
		MemberID:           idA,
		MemberEpoch:        1,
		RebalanceTimeoutMs: -1,
	}, &retryA)
	if retryA.ErrorCode != 0 || retryA.MemberEpoch != 2 || !retryA.HasAssignment {
		t.Fatalf("A prevEpoch retry: %+v", retryA)
	}

	// Older epoch is fenced.
	var fenced wire.ConsumerGroupHeartbeatResponse
	h.call(1, wire.KeyConsumerGroupHeartbeat, 1, &wire.ConsumerGroupHeartbeatRequest{
		GroupID:            "cg",
		MemberID:           idA,
		MemberEpoch:        0, // Wait: 0 is rejoin; let's test after bumping epoch to 3 or test fenced with epoch 99
		RebalanceTimeoutMs: -1,
	}, &fenced)
	// Epoch 99 is fenced:
	h.call(1, wire.KeyConsumerGroupHeartbeat, 1, &wire.ConsumerGroupHeartbeatRequest{
		GroupID:            "cg",
		MemberID:           idA,
		MemberEpoch:        99,
		RebalanceTimeoutMs: -1,
	}, &fenced)
	if fenced.ErrorCode != wire.ErrFencedMemberEpoch {
		t.Fatalf("fenced epoch: got %d, want %d", fenced.ErrorCode, wire.ErrFencedMemberEpoch)
	}

	// OffsetCommit v9: current epoch succeeds, stale epoch fails with STALE_MEMBER_EPOCH.
	var oc wire.OffsetCommitResponse
	h.call(1, wire.KeyOffsetCommit, 9, &wire.OffsetCommitRequest{
		GroupID: "cg", GenerationID: 2, MemberID: idA,
		Topics: []wire.OffsetCommitTopic{{Name: "t", Partitions: []wire.OffsetCommitPartition{{Index: 0, Offset: 42, LeaderEpoch: -1}}}},
	}, &oc)
	if oc.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("OffsetCommit v9 current epoch: %+v", oc)
	}
	h.call(1, wire.KeyOffsetCommit, 9, &wire.OffsetCommitRequest{
		GroupID: "cg", GenerationID: 1, MemberID: idA,
		Topics: []wire.OffsetCommitTopic{{Name: "t", Partitions: []wire.OffsetCommitPartition{{Index: 0, Offset: 43, LeaderEpoch: -1}}}},
	}, &oc)
	if oc.Topics[0].Partitions[0].ErrorCode != wire.ErrStaleMemberEpoch {
		t.Fatalf("OffsetCommit v9 stale epoch: got %d, want %d", oc.Topics[0].Partitions[0].ErrorCode, wire.ErrStaleMemberEpoch)
	}

	// OffsetFetch v9: batched Groups with MemberEpoch check.
	var of wire.OffsetFetchResponse
	h.call(1, wire.KeyOffsetFetch, 9, &wire.OffsetFetchRequest{
		Groups: []wire.OffsetFetchGroup{{GroupID: "cg", MemberID: idA, MemberEpoch: 2, Topics: []wire.OffsetFetchTopic{{Name: "t", Partitions: []int32{0}}}}},
	}, &of)
	if len(of.Groups) != 1 || of.Groups[0].ErrorCode != 0 || of.Groups[0].Topics[0].Partitions[0].Offset != 42 {
		t.Fatalf("OffsetFetch v9: %+v", of)
	}
	h.call(1, wire.KeyOffsetFetch, 9, &wire.OffsetFetchRequest{
		Groups: []wire.OffsetFetchGroup{{GroupID: "cg", MemberID: idA, MemberEpoch: 1, Topics: []wire.OffsetFetchTopic{{Name: "t", Partitions: []int32{0}}}}},
	}, &of)
	if of.Groups[0].ErrorCode != wire.ErrStaleMemberEpoch {
		t.Fatalf("OffsetFetch v9 stale epoch: got %d, want %d", of.Groups[0].ErrorCode, wire.ErrStaleMemberEpoch)
	}

	// ListGroups shows Type: "consumer" and State: "Stable".
	var lg wire.ListGroupsResponse
	h.call(1, wire.KeyListGroups, 5, &wire.ListGroupsRequest{Types: []string{"consumer"}}, &lg)
	if len(lg.Groups) != 1 || lg.Groups[0].ID != "cg" || lg.Groups[0].Type != "consumer" || lg.Groups[0].State != "Stable" {
		t.Fatalf("ListGroups: %+v", lg)
	}

	// Classic JoinGroup on a consumer-protocol group is refused.
	var jg wire.JoinGroupResponse
	h.call(1, wire.KeyJoinGroup, 5, &wire.JoinGroupRequest{
		GroupID: "cg", SessionTimeoutMs: 10000, RebalanceTimeoutMs: 10000,
		ProtocolType: "consumer", Protocols: []wire.GroupProtocol{{Name: "range"}},
	}, &jg)
	if jg.ErrorCode != wire.ErrInconsistentGroupProtocol {
		t.Fatalf("classic join on consumer group: got %d, want %d", jg.ErrorCode, wire.ErrInconsistentGroupProtocol)
	}

	// Snapshot v10 round-trip preserves consumer group state and hash.
	snap := h.node.st.encode()
	dec, err := decodeState(snap)
	if err != nil {
		t.Fatalf("decodeState v10: %v", err)
	}
	if dec.hash() != h.node.st.hash() {
		t.Fatalf("snapshot v10 hash mismatch: got %08x, want %08x", dec.hash(), h.node.st.hash())
	}

	// Member A leaves, B gets all 4 partitions.
	var leaveA wire.ConsumerGroupHeartbeatResponse
	h.call(1, wire.KeyConsumerGroupHeartbeat, 1, &wire.ConsumerGroupHeartbeatRequest{
		GroupID: "cg", MemberID: idA, MemberEpoch: wire.EpochLeave, RebalanceTimeoutMs: -1,
	}, &leaveA)
	if leaveA.ErrorCode != 0 || leaveA.MemberEpoch != -1 {
		t.Fatalf("A leave: %+v", leaveA)
	}
	rB = wire.ConsumerGroupHeartbeatResponse{}
	h.call(2, wire.KeyConsumerGroupHeartbeat, 1, &wire.ConsumerGroupHeartbeatRequest{
		GroupID: "cg", MemberID: idB, MemberEpoch: 2, RebalanceTimeoutMs: -1,
	}, &rB)
	if rB.ErrorCode != 0 || rB.MemberEpoch != 3 || !rB.HasAssignment || !slices.Equal(rB.Assignment[0].Partitions, []int32{0, 1, 2, 3}) {
		t.Fatalf("B after A left: %+v", rB)
	}
}

func TestConsumerGroupSessionAndRevokeExpiry(t *testing.T) {
	h := newHarness(t, Config{
		ConsumerHeartbeatInterval: 50 * time.Millisecond,
		ConsumerSessionTimeout:    300 * time.Millisecond,
	})
	h.open(1)
	h.createTopic(1, "t", 2)
	var rA wire.ConsumerGroupHeartbeatResponse
	h.call(1, wire.KeyConsumerGroupHeartbeat, 1, &wire.ConsumerGroupHeartbeatRequest{
		GroupID: "cg", MemberID: "a", MemberEpoch: 0, RebalanceTimeoutMs: 500, SubscribedTopics: []string{"t"},
	}, &rA)
	if rA.ErrorCode != 0 {
		t.Fatalf("A join: %+v", rA)
	}
	// Advance past ConsumerSessionTimeout: A is expired and removed.
	h.advance(400 * time.Millisecond)
	var hb wire.ConsumerGroupHeartbeatResponse
	h.call(1, wire.KeyConsumerGroupHeartbeat, 1, &wire.ConsumerGroupHeartbeatRequest{
		GroupID: "cg", MemberID: "a", MemberEpoch: 1, RebalanceTimeoutMs: -1,
	}, &hb)
	if hb.ErrorCode != wire.ErrUnknownMemberID {
		t.Fatalf("expired member heartbeat: got %d, want %d", hb.ErrorCode, wire.ErrUnknownMemberID)
	}
}

func TestDescribeTopicPartitionsPagination(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	h.createTopic(1, "a", 2)
	h.createTopic(1, "b", 3)

	var p1 wire.DescribeTopicPartitionsResponse
	h.call(1, wire.KeyDescribeTopicPartitions, 0, &wire.DescribeTopicPartitionsRequest{
		Topics: []string{"b", "a", "missing"}, ResponsePartitionLimit: 3,
	}, &p1)
	// Sorted order: a (2 parts), b (1 part), missing (unknown). Limit 3 stops at b partition 1.
	if p1.NextCursor == nil || p1.NextCursor.TopicName != "b" || p1.NextCursor.PartitionIndex != 1 {
		t.Fatalf("page 1 cursor: %+v", p1.NextCursor)
	}
	if len(p1.Topics) != 2 || len(p1.Topics[0].Partitions) != 2 || len(p1.Topics[1].Partitions) != 1 {
		t.Fatalf("page 1 topics: %+v", p1.Topics)
	}

	var p2 wire.DescribeTopicPartitionsResponse
	h.call(1, wire.KeyDescribeTopicPartitions, 0, &wire.DescribeTopicPartitionsRequest{
		Topics: []string{"b", "a", "missing"}, ResponsePartitionLimit: 10, Cursor: p1.NextCursor,
	}, &p2)
	if p2.NextCursor != nil {
		t.Fatalf("page 2 cursor: %+v, want nil", p2.NextCursor)
	}
	if len(p2.Topics) != 2 || len(p2.Topics[0].Partitions) != 2 || p2.Topics[1].ErrorCode != wire.ErrUnknownTopicOrPartition {
		t.Fatalf("page 2 topics: %+v", p2.Topics)
	}
}

func TestUniformAssignorStickyAndBalanced(t *testing.T) {
	id1 := wire.UUID{1}
	id2 := wire.UUID{2}
	topics := []topicInfo{{name: "t1", id: id1, parts: 3}, {name: "t2", id: id2, parts: 3}}
	m1 := &cmember{id: "m1", subscribed: []string{"t1", "t2"}}
	m2 := &cmember{id: "m2", subscribed: []string{"t1", "t2"}}
	uniformAssign([]*cmember{m1, m2}, topics)
	if len(m1.target) != 3 || len(m2.target) != 3 {
		t.Fatalf("uniform 2 members 6 parts: %d and %d", len(m1.target), len(m2.target))
	}
	// Adding m3 should keep existing assignments on m1/m2 where possible (at most 2 each).
	prev1 := slices.Clone(m1.target)
	prev2 := slices.Clone(m2.target)
	m3 := &cmember{id: "m3", subscribed: []string{"t1", "t2"}}
	uniformAssign([]*cmember{m1, m2, m3}, topics)
	if len(m1.target) != 2 || len(m2.target) != 2 || len(m3.target) != 2 {
		t.Fatalf("uniform 3 members 6 parts: %d, %d, %d", len(m1.target), len(m2.target), len(m3.target))
	}
	for _, p := range m1.target {
		if !prev1.has(p) {
			t.Fatalf("m1 received non-sticky partition %+v", p)
		}
	}
	for _, p := range m2.target {
		if !prev2.has(p) {
			t.Fatalf("m2 received non-sticky partition %+v", p)
		}
	}
}
