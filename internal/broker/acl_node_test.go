package broker

import (
	"testing"

	"github.com/amritangshudey/samsa/internal/scram"
	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

const (
	admin sys.ConnID = 1 // logged in as User:admin, a super user
	user  sys.ConnID = 2 // logged in as User:alice
)

// aclHarness is a node with SASL users admin and alice, ACLs on, default
// deny, admin a super user. Topic t, a committed offset in group g, and
// topic gone (to delete) exist.
func aclHarness(t *testing.T, auth *Authorization) *harness {
	t.Helper()
	users := map[string][]scram.Credential{}
	for _, name := range []string{"admin", "alice"} {
		c, err := scram.NewCredential(scram.SHA256, name+"-secret", []byte("salt-"+name), 4096)
		if err != nil {
			t.Fatal(err)
		}
		users[name] = []scram.Credential{c}
	}
	auth.SuperUsers = []string{"User:admin"}
	h := newHarness(t, Config{SASL: &SASL{Mechanisms: []string{scram.SHA256}, Users: users}, Authorization: auth})
	h.open(admin)
	h.open(user)
	if !h.scramLogin(admin, scram.SHA256, "admin", "admin-secret") || !h.scramLogin(user, scram.SHA256, "alice", "alice-secret") {
		t.Fatal("login failed")
	}
	if h.createTopic(admin, "t", 1) != 0 || h.createTopic(admin, "gone", 1) != 0 {
		t.Fatal("create topics")
	}
	if r := h.produce(admin, "t", 0, "a", "b"); r.ErrorCode != 0 {
		t.Fatalf("produce: %d", r.ErrorCode)
	}
	var oc wire.OffsetCommitResponse
	h.call(admin, wire.KeyOffsetCommit, 7, &wire.OffsetCommitRequest{GroupID: "g", GenerationID: -1,
		Topics: []wire.OffsetCommitTopic{{Name: "t", Partitions: []wire.OffsetCommitPartition{{Index: 0, Offset: 1, LeaderEpoch: -1}}}}}, &oc)
	if oc.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("commit: %d", oc.Topics[0].Partitions[0].ErrorCode)
	}
	return h
}

func (h *harness) createACLs(c sys.ConnID, acls ...wire.ACL) []wire.ACLResult {
	h.t.Helper()
	var resp wire.CreateAclsResponse
	h.call(c, wire.KeyCreateAcls, 3, &wire.CreateAclsRequest{Creations: acls}, &resp)
	return resp.Results
}

func (h *harness) deleteACLs(c sys.ConnID, filters ...wire.ACLFilter) []wire.DeleteAclsResult {
	h.t.Helper()
	var resp wire.DeleteAclsResponse
	h.call(c, wire.KeyDeleteAcls, 3, &wire.DeleteAclsRequest{Filters: filters}, &resp)
	return resp.Results
}

func (h *harness) describeACLs(c sys.ConnID, f wire.ACLFilter) wire.DescribeAclsResponse {
	h.t.Helper()
	var resp wire.DescribeAclsResponse
	h.call(c, wire.KeyDescribeAcls, 3, &wire.DescribeAclsRequest{ACLFilter: f}, &resp)
	return resp
}

var anyACL = wire.ACLFilter{ResourceType: resAny, PatternType: patAny, Operation: opAny, Permission: permAny}

// allowAlice is an ALLOW ACL for User:alice from any host.
func allowAlice(res int8, name string, op int8) wire.ACL {
	return acl(res, name, patLiteral, "User:alice", "*", op, permAllow)
}

// Every API that checks ACLs: alice is refused with Kafka's code; after
// the ACL Kafka requires is created, she is not; after it is deleted, she
// is refused again. Each case returns the code it observed.
func TestEveryAPIAllowedAndDenied(t *testing.T) {
	for _, c := range []struct {
		api    string
		grant  []wire.ACL
		denied int16
		do     func(h *harness) int16
	}{
		{"Metadata", []wire.ACL{allowAlice(resTopic, "t", opDescribe)}, wire.ErrTopicAuthorizationFailed, func(h *harness) int16 {
			var r wire.MetadataResponse
			h.call(user, wire.KeyMetadata, 8, &wire.MetadataRequest{Topics: []string{"t"}}, &r)
			return r.Topics[0].ErrorCode
		}},
		{"CreateTopics", []wire.ACL{allowAlice(resTopic, "new", opCreate)}, wire.ErrTopicAuthorizationFailed, func(h *harness) int16 {
			var r wire.CreateTopicsResponse
			h.call(user, wire.KeyCreateTopics, 4, &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{{Name: "new", NumPartitions: 1, ReplicationFactor: -1}}, ValidateOnly: true}, &r)
			return r.Topics[0].ErrorCode
		}},
		{"Produce", []wire.ACL{allowAlice(resTopic, "t", opWrite)}, wire.ErrTopicAuthorizationFailed, func(h *harness) int16 {
			return h.produce(user, "t", 0, "c").ErrorCode
		}},
		{"Fetch", []wire.ACL{allowAlice(resTopic, "t", opRead)}, wire.ErrTopicAuthorizationFailed, func(h *harness) int16 {
			var r wire.FetchResponse
			h.call(user, wire.KeyFetch, 11, fetchReq("t", 0, 0, 0, 0), &r)
			return r.Topics[0].Partitions[0].ErrorCode
		}},
		{"ListOffsets", []wire.ACL{allowAlice(resTopic, "t", opDescribe)}, wire.ErrTopicAuthorizationFailed, func(h *harness) int16 {
			var r wire.ListOffsetsResponse
			h.call(user, wire.KeyListOffsets, 5, &wire.ListOffsetsRequest{Topics: []wire.ListOffsetsTopic{{Name: "t", Partitions: []wire.ListOffsetsPartition{{Index: 0, Timestamp: wire.TimestampLatest}}}}}, &r)
			return r.Topics[0].Partitions[0].ErrorCode
		}},
		{"OffsetCommit, group", []wire.ACL{allowAlice(resGroup, "g", opRead), allowAlice(resTopic, "t", opRead)}, wire.ErrGroupAuthorizationFailed, func(h *harness) int16 {
			var r wire.OffsetCommitResponse
			h.call(user, wire.KeyOffsetCommit, 7, &wire.OffsetCommitRequest{GroupID: "g", GenerationID: -1,
				Topics: []wire.OffsetCommitTopic{{Name: "t", Partitions: []wire.OffsetCommitPartition{{Index: 0, Offset: 2, LeaderEpoch: -1}}}}}, &r)
			return r.Topics[0].Partitions[0].ErrorCode
		}},
		{"OffsetCommit, topic", []wire.ACL{allowAlice(resTopic, "t", opRead)}, wire.ErrTopicAuthorizationFailed, func(h *harness) int16 {
			h.createACLs(admin, allowAlice(resGroup, "g", opRead)) // the group is allowed throughout
			var r wire.OffsetCommitResponse
			h.call(user, wire.KeyOffsetCommit, 7, &wire.OffsetCommitRequest{GroupID: "g", GenerationID: -1,
				Topics: []wire.OffsetCommitTopic{{Name: "t", Partitions: []wire.OffsetCommitPartition{{Index: 0, Offset: 2, LeaderEpoch: -1}}}}}, &r)
			return r.Topics[0].Partitions[0].ErrorCode
		}},
		{"OffsetFetch, group", []wire.ACL{allowAlice(resGroup, "g", opDescribe), allowAlice(resTopic, "t", opDescribe)}, wire.ErrGroupAuthorizationFailed, func(h *harness) int16 {
			var r wire.OffsetFetchResponse
			h.call(user, wire.KeyOffsetFetch, 5, &wire.OffsetFetchRequest{GroupID: "g", Topics: []wire.OffsetFetchTopic{{Name: "t", Partitions: []int32{0}}}}, &r)
			if r.ErrorCode == 0 && r.Topics[0].Partitions[0].Offset != 1 {
				h.t.Errorf("allowed offset fetch read %d", r.Topics[0].Partitions[0].Offset)
			}
			return r.ErrorCode
		}},
		{"OffsetFetch, topic", []wire.ACL{allowAlice(resTopic, "t", opDescribe)}, wire.ErrTopicAuthorizationFailed, func(h *harness) int16 {
			h.createACLs(admin, allowAlice(resGroup, "g", opDescribe))
			var r wire.OffsetFetchResponse
			h.call(user, wire.KeyOffsetFetch, 5, &wire.OffsetFetchRequest{GroupID: "g", Topics: []wire.OffsetFetchTopic{{Name: "t", Partitions: []int32{0}}}}, &r)
			return r.Topics[0].Partitions[0].ErrorCode
		}},
		{"FindCoordinator", []wire.ACL{allowAlice(resGroup, "g", opDescribe)}, wire.ErrGroupAuthorizationFailed, func(h *harness) int16 {
			var r wire.FindCoordinatorResponse
			h.call(user, wire.KeyFindCoord, 2, &wire.FindCoordinatorRequest{Key: "g"}, &r)
			return r.ErrorCode
		}},
		{"JoinGroup", []wire.ACL{allowAlice(resGroup, "g", opRead)}, wire.ErrGroupAuthorizationFailed, func(h *harness) int16 {
			var r wire.JoinGroupResponse
			h.call(user, wire.KeyJoinGroup, 5, &wire.JoinGroupRequest{GroupID: "g", SessionTimeoutMs: 10000, RebalanceTimeoutMs: 10000,
				ProtocolType: "consumer", Protocols: []wire.GroupProtocol{{Name: "range"}}}, &r)
			return r.ErrorCode // allowed: MEMBER_ID_REQUIRED, the first step of a join
		}},
		{"SyncGroup", []wire.ACL{allowAlice(resGroup, "g", opRead)}, wire.ErrGroupAuthorizationFailed, func(h *harness) int16 {
			var r wire.SyncGroupResponse
			h.call(user, wire.KeySyncGroup, 3, &wire.SyncGroupRequest{GroupID: "g", GenerationID: 1, MemberID: "m"}, &r)
			return r.ErrorCode // allowed: UNKNOWN_MEMBER_ID
		}},
		{"Heartbeat", []wire.ACL{allowAlice(resGroup, "g", opRead)}, wire.ErrGroupAuthorizationFailed, func(h *harness) int16 {
			var r wire.HeartbeatResponse
			h.call(user, wire.KeyHeartbeat, 3, &wire.HeartbeatRequest{GroupID: "g", GenerationID: 1, MemberID: "m"}, &r)
			return r.ErrorCode
		}},
		{"LeaveGroup", []wire.ACL{allowAlice(resGroup, "g", opRead)}, wire.ErrGroupAuthorizationFailed, func(h *harness) int16 {
			var r wire.LeaveGroupResponse
			h.call(user, wire.KeyLeaveGroup, 3, &wire.LeaveGroupRequest{GroupID: "g", Members: []string{"m"}}, &r)
			return r.ErrorCode
		}},
		{"DescribeGroups", []wire.ACL{allowAlice(resGroup, "g", opDescribe)}, wire.ErrGroupAuthorizationFailed, func(h *harness) int16 {
			var r wire.DescribeGroupsResponse
			h.call(user, wire.KeyDescribeGroups, 5, &wire.DescribeGroupsRequest{Groups: []string{"g"}}, &r)
			return r.Groups[0].ErrorCode
		}},
		{"ListGroups", []wire.ACL{allowAlice(resGroup, "g", opDescribe)}, wire.ErrGroupAuthorizationFailed, func(h *harness) int16 {
			var r wire.ListGroupsResponse
			h.call(user, wire.KeyListGroups, 5, &wire.ListGroupsRequest{}, &r)
			if r.ErrorCode != 0 {
				h.t.Errorf("ListGroups answered %d; it never fails, it filters", r.ErrorCode)
			}
			if len(r.Groups) == 0 {
				return wire.ErrGroupAuthorizationFailed // stands for "not listed"
			}
			return 0
		}},
		{"DeleteTopics", []wire.ACL{allowAlice(resTopic, "gone", opDelete)}, wire.ErrTopicAuthorizationFailed, func(h *harness) int16 {
			var r wire.DeleteTopicsResponse
			h.call(user, wire.KeyDeleteTopics, 6, &wire.DeleteTopicsRequest{Topics: []wire.DeleteTopic{{Name: "gone"}}}, &r)
			return r.Topics[0].ErrorCode // allowed: deleted; denied again: refused before "unknown topic"
		}},
		{"DeleteRecords", []wire.ACL{allowAlice(resTopic, "t", opDelete)}, wire.ErrTopicAuthorizationFailed, func(h *harness) int16 {
			return h.deleteRecords(user, "t", 0, 0).ErrorCode
		}},
		{"CreatePartitions", []wire.ACL{allowAlice(resTopic, "t", opAlter)}, wire.ErrTopicAuthorizationFailed, func(h *harness) int16 {
			var r wire.CreatePartitionsResponse
			h.call(user, wire.KeyCreatePartitions, 3, &wire.CreatePartitionsRequest{Topics: []wire.CreatePartitionsTopic{{Name: "t", Count: 2}}, ValidateOnly: true}, &r)
			return r.Results[0].ErrorCode
		}},
		{"InitProducerId", []wire.ACL{allowAlice(resCluster, clusterName, opIdempotentWrite)}, wire.ErrClusterAuthorizationFailed, func(h *harness) int16 {
			var r wire.InitProducerIDResponse
			h.call(user, wire.KeyInitProducerID, 4, &wire.InitProducerIDRequest{}, &r)
			return r.ErrorCode
		}},
		{"InitProducerId, by a topic WRITE", []wire.ACL{allowAlice(resTopic, "t", opWrite)}, wire.ErrClusterAuthorizationFailed, func(h *harness) int16 {
			var r wire.InitProducerIDResponse
			h.call(user, wire.KeyInitProducerID, 4, &wire.InitProducerIDRequest{}, &r)
			return r.ErrorCode
		}},
		{"DescribeConfigs, topic", []wire.ACL{allowAlice(resTopic, "t", opDescribeConfigs)}, wire.ErrTopicAuthorizationFailed, func(h *harness) int16 {
			var r wire.DescribeConfigsResponse
			h.call(user, wire.KeyDescribeConfigs, 4, &wire.DescribeConfigsRequest{Resources: []wire.DescribeConfigsResource{{Type: wire.ResourceTopic, Name: "t"}}}, &r)
			return r.Results[0].ErrorCode
		}},
		{"DescribeConfigs, broker", []wire.ACL{allowAlice(resCluster, clusterName, opDescribeConfigs)}, wire.ErrClusterAuthorizationFailed, func(h *harness) int16 {
			var r wire.DescribeConfigsResponse
			h.call(user, wire.KeyDescribeConfigs, 4, &wire.DescribeConfigsRequest{Resources: []wire.DescribeConfigsResource{{Type: wire.ResourceBroker, Name: ""}}}, &r)
			return r.Results[0].ErrorCode
		}},
		{"DescribeAcls", []wire.ACL{allowAlice(resCluster, clusterName, opDescribe)}, wire.ErrClusterAuthorizationFailed, func(h *harness) int16 {
			return h.describeACLs(user, anyACL).ErrorCode
		}},
		{"CreateAcls", []wire.ACL{allowAlice(resCluster, clusterName, opAlter)}, wire.ErrClusterAuthorizationFailed, func(h *harness) int16 {
			return h.createACLs(user, allowAlice(resTopic, "x", opRead))[0].ErrorCode
		}},
		{"DeleteAcls", []wire.ACL{allowAlice(resCluster, clusterName, opAlter)}, wire.ErrClusterAuthorizationFailed, func(h *harness) int16 {
			x := "x"
			return h.deleteACLs(user, wire.ACLFilter{ResourceType: resTopic, ResourceName: &x, PatternType: patLiteral, Operation: opAny, Permission: permAny})[0].ErrorCode
		}},
	} {
		t.Run(c.api, func(t *testing.T) {
			h := aclHarness(t, &Authorization{})
			if got := c.do(h); got != c.denied {
				t.Fatalf("before the ACL: got %d, want %d", got, c.denied)
			}
			for _, r := range h.createACLs(admin, c.grant...) {
				if r.ErrorCode != 0 {
					t.Fatalf("create ACL: %+v", r)
				}
			}
			if got := c.do(h); got == c.denied || got == wire.ErrClusterAuthorizationFailed || got == wire.ErrGroupAuthorizationFailed || got == wire.ErrTopicAuthorizationFailed {
				t.Fatalf("with the ACL: refused, %d", got)
			}
			f := anyACL
			f.Principal = new(string)
			*f.Principal = "User:alice"
			if res := h.deleteACLs(admin, f); res[0].ErrorCode != 0 || len(res[0].Matching) == 0 {
				t.Fatalf("delete ACLs: %+v", res)
			}
			if got := c.do(h); got != c.denied {
				t.Fatalf("after the ACL was deleted: got %d, want %d", got, c.denied)
			}
		})
	}
}

// Requests that check nothing: ApiVersions, the SASL requests (exercised
// by the login) and DescribeCluster.
func TestUncheckedAPIs(t *testing.T) {
	h := aclHarness(t, &Authorization{})
	var r wire.DescribeClusterResponse
	h.call(user, wire.KeyDescribeCluster, 2, &wire.DescribeClusterRequest{EndpointType: wire.EndpointBrokers}, &r)
	if r.ErrorCode != 0 || len(r.Brokers) != 1 {
		t.Fatalf("DescribeCluster: %+v", r)
	}
	var v wire.APIVersionsResponse
	h.call(user, wire.KeyAPIVersions, 3, &wire.APIVersionsRequest{}, &v)
	if v.ErrorCode != 0 {
		t.Fatalf("ApiVersions: %d", v.ErrorCode)
	}
}

// An all-topics Metadata leaves out what alice may not describe, and
// discloses nothing about it.
func TestMetadataHidesTopics(t *testing.T) {
	h := aclHarness(t, &Authorization{})
	h.createACLs(admin, allowAlice(resTopic, "t", opRead))
	var r wire.MetadataResponse
	h.call(user, wire.KeyMetadata, 8, &wire.MetadataRequest{AllTopics: true}, &r)
	if len(r.Topics) != 1 || r.Topics[0].Name != "t" {
		t.Fatalf("all topics: %+v", r.Topics)
	}
	h.call(user, wire.KeyMetadata, 8, &wire.MetadataRequest{Topics: []string{"no-such-topic"}}, &r)
	if r.Topics[0].ErrorCode != wire.ErrTopicAuthorizationFailed {
		t.Fatalf("an unknown topic she may not describe: %d, want TOPIC_AUTHORIZATION_FAILED", r.Topics[0].ErrorCode)
	}
}

// AllowIfNoACL: a resource without ACLs is open; one with ACLs for someone
// else is not.
func TestAllowIfNoACL(t *testing.T) {
	h := aclHarness(t, &Authorization{AllowIfNoACL: true})
	if r := h.produce(user, "t", 0, "c"); r.ErrorCode != 0 {
		t.Fatalf("no ACL on t: %d", r.ErrorCode)
	}
	h.createACLs(admin, acl(resTopic, "t", patLiteral, "User:bob", "*", opWrite, permAllow))
	if r := h.produce(user, "t", 0, "c"); r.ErrorCode != wire.ErrTopicAuthorizationFailed {
		t.Fatalf("ACL for bob only: %d", r.ErrorCode)
	}
}

// Hosts are the client's address as the I/O layer reports it.
func TestHostACL(t *testing.T) {
	h := aclHarness(t, &Authorization{})
	h.createACLs(admin, acl(resTopic, "t", patLiteral, "User:alice", "10.1.0.0/16", opWrite, permAllow))
	if r := h.produce(user, "t", 0, "c"); r.ErrorCode != wire.ErrTopicAuthorizationFailed {
		t.Fatalf("from 127.0.0.1: %d", r.ErrorCode)
	}
	h.createACLs(admin, acl(resTopic, "t", patLiteral, "User:alice", "127.0.0.1", opWrite, permAllow))
	if r := h.produce(user, "t", 0, "c"); r.ErrorCode != 0 {
		t.Fatalf("from 127.0.0.1 with its ACL: %d", r.ErrorCode)
	}
}

func TestACLAPIs(t *testing.T) {
	h := aclHarness(t, &Authorization{})
	res := h.createACLs(admin,
		allowAlice(resTopic, "t", opRead),
		allowAlice(resTopic, "t", opRead), // the same: succeeds, stored once
		acl(resTopic, "or", patPrefixed, "User:*", "*", opWrite, permDeny),
		acl(resCluster, "other", patLiteral, "User:alice", "*", opAlter, permAllow),
		acl(resTopic, "t", patMatch, "User:alice", "*", opRead, permAllow),
		acl(resTopic, "t", patLiteral, "alice", "*", opRead, permAllow),
	)
	want := []int16{0, 0, 0, wire.ErrInvalidRequest, wire.ErrInvalidRequest, wire.ErrInvalidRequest}
	for i, r := range res {
		if r.ErrorCode != want[i] {
			t.Errorf("creation %d: %+v, want %d", i, r, want[i])
		}
	}
	d := h.describeACLs(admin, anyACL)
	if d.ErrorCode != 0 || len(d.Resources) != 2 || len(d.Resources[0].ACLs) != 1 || d.Resources[1].PatternType != patPrefixed {
		t.Fatalf("describe: %+v", d)
	}
	orders := "orders"
	f := wire.ACLFilter{ResourceType: resTopic, ResourceName: &orders, PatternType: patMatch, Operation: opAny, Permission: permAny}
	if d := h.describeACLs(admin, f); len(d.Resources) != 1 || d.Resources[0].ResourceName != "or" {
		t.Fatalf("MATCH orders: %+v", d)
	}
	if r := h.deleteACLs(admin, f); len(r[0].Matching) != 1 || r[0].Matching[0].ResourceName != "or" {
		t.Fatalf("delete MATCH orders: %+v", r)
	}
	if d := h.describeACLs(admin, anyACL); len(d.Resources) != 1 {
		t.Fatalf("after delete: %+v", d)
	}
	// The ACLs survive a restart, from the log and from a snapshot.
	h.start()
	if len(h.node.st.acls) != 1 {
		t.Fatalf("after restart: %+v", h.node.st.acls)
	}
}

// Without -acl, the ACL APIs answer SECURITY_DISABLED, as Kafka does
// without an authorizer, and everything else is allowed.
func TestACLsOff(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	if d := h.describeACLs(1, anyACL); d.ErrorCode != wire.ErrSecurityDisabled {
		t.Fatalf("describe: %d", d.ErrorCode)
	}
	if r := h.createACLs(1, allowAlice(resTopic, "t", opRead)); r[0].ErrorCode != wire.ErrSecurityDisabled {
		t.Fatalf("create: %d", r[0].ErrorCode)
	}
	if r := h.deleteACLs(1, anyACL); r[0].ErrorCode != wire.ErrSecurityDisabled {
		t.Fatalf("delete: %d", r[0].ErrorCode)
	}
}

// ACLs are commands in the log: created on the leader, they hold on every
// node, and the state hashes agree. A follower refuses ACL changes with
// NOT_CONTROLLER, as it refuses every write.
func TestACLsReplicate(t *testing.T) {
	tr := newTrio(t)
	for _, h := range tr.h {
		h.cfg.Authorization = &Authorization{AllowIfNoACL: true} // no SASL: everyone is User:ANONYMOUS
		h.start()
		h.open(1)
	}
	tr.elect(1, 1, 2, 3)
	lead, follower := tr.h[1], tr.h[2]
	corr := lead.send(1, wire.KeyCreateTopics, 4, &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{{Name: "t", NumPartitions: 1, ReplicationFactor: 3}}})
	tr.deliver(1, 2, 3)
	var ct wire.CreateTopicsResponse
	if !lead.recv(1, wire.KeyCreateTopics, 4, corr, &ct) || ct.Topics[0].ErrorCode != 0 {
		t.Fatalf("create topic: %+v", ct)
	}
	lead.must(lead.node.OnTimer(timerHeartbeat))
	tr.deliver(1, 2, 3)
	if md := follower.metadata(1); len(md.Topics) != 1 {
		t.Fatalf("follower before the ACL: %+v", md.Topics)
	}

	deny := acl(resTopic, "t", patLiteral, "User:ANONYMOUS", "*", opDescribe, permDeny)
	corr = lead.send(1, wire.KeyCreateAcls, 3, &wire.CreateAclsRequest{Creations: []wire.ACL{deny}})
	tr.deliver(1, 2, 3)
	var ca wire.CreateAclsResponse
	if !lead.recv(1, wire.KeyCreateAcls, 3, corr, &ca) || ca.Results[0].ErrorCode != 0 {
		t.Fatalf("create ACL on the leader: %+v", ca)
	}
	lead.must(lead.node.OnTimer(timerHeartbeat)) // carries the new commit index
	tr.deliver(1, 2, 3)
	la, lh := lead.node.StateHash()
	for id, h := range tr.h {
		if a, hash := h.node.StateHash(); a != la || hash != lh {
			t.Fatalf("node %d applied %d entries with hash %08x; the leader %d with %08x", id, a, hash, la, lh)
		}
	}
	if md := follower.metadata(1); len(md.Topics) != 0 {
		t.Fatalf("the follower still lists t after the DENY: %+v", md.Topics)
	}
	if r := follower.createACLs(1, deny); r[0].ErrorCode != wire.ErrNotController {
		t.Fatalf("CreateAcls on a follower: %+v", r)
	}
	if d := follower.describeACLs(1, anyACL); d.ErrorCode != 0 || len(d.Resources) != 1 {
		t.Fatalf("DescribeAcls on a follower: %+v", d)
	}
}
