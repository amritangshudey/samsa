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
		"CreateTopicsRequest": {KeyCreateTopics, &CreateTopicsRequest{Topics: []CreateTopic{{Name: "a", NumPartitions: 3, ReplicationFactor: 1}, {Name: "b", NumPartitions: 1, ReplicationFactor: 1, HasAssignments: true}}, TimeoutMs: 1000},
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

		"OffsetCommitRequest": {KeyOffsetCommit, &OffsetCommitRequest{GroupID: "g", GenerationID: 3, MemberID: "m", Topics: []OffsetCommitTopic{
			{Name: "a", Partitions: []OffsetCommitPartition{{Index: 0, Offset: 42, LeaderEpoch: 2, Metadata: "note"}, {Index: 1, Offset: 7, LeaderEpoch: -1}}}}},
			func() Message { return &OffsetCommitRequest{} }},
		"OffsetCommitResponse": {KeyOffsetCommit, &OffsetCommitResponse{Topics: []OffsetCommitTopicResponse{{Name: "a", Partitions: []PartitionError{{0, 0}, {1, 22}}}}},
			func() Message { return &OffsetCommitResponse{} }},
		"OffsetFetchRequest": {KeyOffsetFetch, &OffsetFetchRequest{GroupID: "g", Topics: []OffsetFetchTopic{{Name: "a", Partitions: []int32{0, 1}}}},
			func() Message { return &OffsetFetchRequest{} }},
		"OffsetFetchResponse": {KeyOffsetFetch, &OffsetFetchResponse{ErrorCode: 16, Topics: []OffsetFetchTopicResponse{{Name: "a", Partitions: []OffsetFetchPartition{
			{Index: 0, Offset: 42, LeaderEpoch: 2, Metadata: "note"}, {Index: 1, Offset: -1, LeaderEpoch: -1, ErrorCode: 3}}}}},
			func() Message { return &OffsetFetchResponse{} }},
		"FindCoordinatorRequest": {KeyFindCoord, &FindCoordinatorRequest{Key: "g"},
			func() Message { return &FindCoordinatorRequest{} }},
		"FindCoordinatorResponse": {KeyFindCoord, &FindCoordinatorResponse{ErrorCode: 15, NodeID: 1, Host: "localhost", Port: 9092},
			func() Message { return &FindCoordinatorResponse{} }},
		"JoinGroupRequest": {KeyJoinGroup, &JoinGroupRequest{GroupID: "g", SessionTimeoutMs: 10000, RebalanceTimeoutMs: 60000, MemberID: "m", ProtocolType: "consumer",
			Protocols: []GroupProtocol{{"range", []byte("sub")}, {"roundrobin", nil}}},
			func() Message { return &JoinGroupRequest{} }},
		"JoinGroupResponse": {KeyJoinGroup, &JoinGroupResponse{ErrorCode: 79, GenerationID: 4, ProtocolName: "range", Leader: "m", MemberID: "m",
			Members: []GroupMember{{"m", []byte("sub")}, {"n", nil}}},
			func() Message { return &JoinGroupResponse{} }},
		"HeartbeatRequest": {KeyHeartbeat, &HeartbeatRequest{GroupID: "g", GenerationID: 4, MemberID: "m"},
			func() Message { return &HeartbeatRequest{} }},
		"HeartbeatResponse": {KeyHeartbeat, &HeartbeatResponse{ErrorCode: 27},
			func() Message { return &HeartbeatResponse{} }},
		"LeaveGroupRequest": {KeyLeaveGroup, &LeaveGroupRequest{GroupID: "g", Members: []string{"m"}},
			func() Message { return &LeaveGroupRequest{} }},
		"LeaveGroupResponse": {KeyLeaveGroup, &LeaveGroupResponse{ErrorCode: 25, Members: []MemberError{{"m", 25}}},
			func() Message { return &LeaveGroupResponse{} }},
		"SyncGroupRequest": {KeySyncGroup, &SyncGroupRequest{GroupID: "g", GenerationID: 4, MemberID: "m", Assignments: []MemberAssignment{{"m", []byte("a")}, {"n", nil}}},
			func() Message { return &SyncGroupRequest{} }},
		"SyncGroupResponse": {KeySyncGroup, &SyncGroupResponse{ErrorCode: 22, Assignment: []byte("a")},
			func() Message { return &SyncGroupResponse{} }},
		"InitProducerIdRequest": {KeyInitProducerID, &InitProducerIDRequest{},
			func() Message { return &InitProducerIDRequest{} }},
		"InitProducerIdRequestTransactional": {KeyInitProducerID, &InitProducerIDRequest{Transactional: true},
			func() Message { return &InitProducerIDRequest{} }},
		"InitProducerIdResponse": {KeyInitProducerID, &InitProducerIDResponse{ErrorCode: 42, ProducerID: 1<<47 | 5, ProducerEpoch: 3},
			func() Message { return &InitProducerIDResponse{} }},

		"DeleteTopicsRequest": {KeyDeleteTopics, &DeleteTopicsRequest{Topics: []DeleteTopic{{Name: "a"}, {Name: "b"}}, TimeoutMs: 30000},
			func() Message { return &DeleteTopicsRequest{} }},
		"DeleteTopicsRequestByID": {KeyDeleteTopics, &DeleteTopicsRequest{Topics: []DeleteTopic{{TopicID: UUID{1, 2, 3}}}, TimeoutMs: 1},
			func() Message { return &DeleteTopicsRequest{} }},
		"DeleteTopicsResponse": {KeyDeleteTopics, &DeleteTopicsResponse{Topics: []DeleteTopicResult{{Name: "a"}, {Name: "b", ErrorCode: 3, ErrorMessage: "no such topic"}}},
			func() Message { return &DeleteTopicsResponse{} }},
		"DeleteRecordsRequest": {KeyDeleteRecords, &DeleteRecordsRequest{Topics: []DeleteRecordsTopic{{Name: "a", Partitions: []DeleteRecordsPartition{{0, 10}, {1, -1}}}}, TimeoutMs: 30000},
			func() Message { return &DeleteRecordsRequest{} }},
		"DeleteRecordsResponse": {KeyDeleteRecords, &DeleteRecordsResponse{Topics: []DeleteRecordsTopicResult{{Name: "a", Partitions: []DeleteRecordsPartitionResult{{0, 10, 0}, {1, -1, 1}}}}},
			func() Message { return &DeleteRecordsResponse{} }},
		"DescribeConfigsRequest": {KeyDescribeConfigs, &DescribeConfigsRequest{Resources: []DescribeConfigsResource{{Type: ResourceTopic, Name: "a"}, {Type: ResourceBroker, Name: "1", Keys: []string{"log.segment.bytes"}}}, IncludeSynonyms: true, IncludeDocumentation: true},
			func() Message { return &DescribeConfigsRequest{} }},
		"DescribeConfigsResponse": {KeyDescribeConfigs, &DescribeConfigsResponse{Results: []DescribeConfigsResult{
			{Type: ResourceTopic, Name: "a", Configs: []ConfigEntry{{Name: "cleanup.policy", Value: "delete", ReadOnly: true, Source: ConfigSourceDefault, Type: 2}, {Name: "retention.ms", Value: "-1", ReadOnly: true, Source: ConfigSourceStaticBroker}}},
			{ErrorCode: 3, ErrorMessage: "no such topic", Type: ResourceTopic, Name: "b"}}},
			func() Message { return &DescribeConfigsResponse{} }},
		"CreatePartitionsRequest": {KeyCreatePartitions, &CreatePartitionsRequest{Topics: []CreatePartitionsTopic{{Name: "a", Count: 3}, {Name: "b", Count: 2, HasAssignments: true}}, TimeoutMs: 30000, ValidateOnly: true},
			func() Message { return &CreatePartitionsRequest{} }},
		"CreatePartitionsResponse": {KeyCreatePartitions, &CreatePartitionsResponse{Results: []CreatePartitionsResult{{Name: "a"}, {Name: "b", ErrorCode: 37, ErrorMessage: "only growth"}}},
			func() Message { return &CreatePartitionsResponse{} }},
		"ListGroupsRequest": {KeyListGroups, &ListGroupsRequest{States: []string{"Stable"}, Types: []string{"classic"}},
			func() Message { return &ListGroupsRequest{} }},
		"ListGroupsResponse": {KeyListGroups, &ListGroupsResponse{Groups: []ListedGroup{{ID: "g", ProtocolType: "consumer", State: "Stable", Type: "classic"}}},
			func() Message { return &ListGroupsResponse{} }},
		"DescribeGroupsRequest": {KeyDescribeGroups, &DescribeGroupsRequest{Groups: []string{"g", "h"}, IncludeAuthorizedOperations: true},
			func() Message { return &DescribeGroupsRequest{} }},
		"DescribeGroupsResponse": {KeyDescribeGroups, &DescribeGroupsResponse{Groups: []DescribedGroup{
			{ID: "g", State: "Stable", ProtocolType: "consumer", ProtocolData: "range", Members: []DescribedMember{{ID: "m", ClientID: "c", ClientHost: "/127.0.0.1", Metadata: []byte("sub"), Assignment: []byte("a")}}},
			{ErrorCode: 16, ErrorMessage: "not coordinator", ID: "h", State: "Dead"}}},
			func() Message { return &DescribeGroupsResponse{} }},
		"DescribeClusterRequest": {KeyDescribeCluster, &DescribeClusterRequest{EndpointType: EndpointBrokers, IncludeFencedBrokers: true},
			func() Message { return &DescribeClusterRequest{} }},
		"DescribeClusterResponse": {KeyDescribeCluster, &DescribeClusterResponse{EndpointType: EndpointBrokers, ClusterID: "samsa", ControllerID: 2, Brokers: []Broker{{1, "localhost", 9092}, {2, "localhost", 9093}}},
			func() Message { return &DescribeClusterResponse{} }},
		"SaslHandshakeRequest": {KeySaslHandshake, &SaslHandshakeRequest{Mechanism: "SCRAM-SHA-256"},
			func() Message { return &SaslHandshakeRequest{} }},
		"SaslHandshakeResponse": {KeySaslHandshake, &SaslHandshakeResponse{ErrorCode: 33, Mechanisms: []string{"PLAIN", "SCRAM-SHA-512"}},
			func() Message { return &SaslHandshakeResponse{} }},
		"SaslAuthenticateRequest": {KeySaslAuthenticate, &SaslAuthenticateRequest{AuthBytes: []byte("n,,n=u,r=abc")},
			func() Message { return &SaslAuthenticateRequest{} }},
		"SaslAuthenticateResponse": {KeySaslAuthenticate, &SaslAuthenticateResponse{ErrorCode: 58, ErrorMessage: "no", AuthBytes: []byte("v=sig")},
			func() Message { return &SaslAuthenticateResponse{} }},
		"DescribeUserScramCredentialsRequest": {KeyDescribeUserScramCredentials, &DescribeUserScramCredentialsRequest{Users: []string{"alice", "bob"}},
			func() Message { return &DescribeUserScramCredentialsRequest{} }},
		"DescribeUserScramCredentialsResponse": {KeyDescribeUserScramCredentials, &DescribeUserScramCredentialsResponse{Results: []UserCredentials{
			{User: "alice", Infos: []CredentialInfo{{1, 4096}, {2, 8192}}}, {User: "x", ErrorCode: 91, ErrorMessage: "no"}}},
			func() Message { return &DescribeUserScramCredentialsResponse{} }},
		"AlterUserScramCredentialsRequest": {KeyAlterUserScramCredentials, &AlterUserScramCredentialsRequest{
			Deletions:  []CredentialDeletion{{"bob", 1}},
			Upsertions: []CredentialUpsertion{{"alice", 2, 4096, []byte("salt"), []byte("salted")}}},
			func() Message { return &AlterUserScramCredentialsRequest{} }},
		"AlterUserScramCredentialsResponse": {KeyAlterUserScramCredentials, &AlterUserScramCredentialsResponse{Results: []UserResult{{User: "alice"}, {"bob", 91, "gone"}}},
			func() Message { return &AlterUserScramCredentialsResponse{} }},
		"AddRaftVoterRequest": {KeyAddRaftVoter, &AddRaftVoterRequest{ClusterID: "samsa", TimeoutMs: 30000, VoterID: 4, Listeners: []Listener{{"CLIENT", "h4", 9092}, {"PEER", "h4", 65000}}, AckWhenCommitted: true},
			func() Message { return &AddRaftVoterRequest{} }},
		"VoterChangeResponse": {KeyAddRaftVoter, &VoterChangeResponse{ErrorCode: 126, ErrorMessage: "dup"},
			func() Message { return &VoterChangeResponse{} }},
		"RemoveRaftVoterRequest": {KeyRemoveRaftVoter, &RemoveRaftVoterRequest{VoterID: 3, VoterDirectoryID: UUID{1, 2}},
			func() Message { return &RemoveRaftVoterRequest{} }},
		"DescribeClientQuotasRequest": {KeyDescribeClientQuotas, &DescribeClientQuotasRequest{Components: []QuotaComponent{{EntityType: "user", Match: "carol"}, {EntityType: "client-id", MatchType: 2}}, Strict: true},
			func() Message { return &DescribeClientQuotasRequest{} }},
		"DescribeClientQuotasResponse": {KeyDescribeClientQuotas, &DescribeClientQuotasResponse{ErrorCode: 31, ErrorMessage: "no"},
			func() Message { return &DescribeClientQuotasResponse{} }},
		"CreateAclsRequest": {KeyCreateAcls, &CreateAclsRequest{Creations: []ACL{testACL}},
			func() Message { return &CreateAclsRequest{} }},
		"CreateAclsResponse": {KeyCreateAcls, &CreateAclsResponse{Results: []ACLResult{{}, {ErrorCode: 42, ErrorMessage: "bad"}}},
			func() Message { return &CreateAclsResponse{} }},
		"DeleteAclsRequest": {KeyDeleteAcls, &DeleteAclsRequest{Filters: []ACLFilter{testFilter, {ResourceType: 1, PatternType: 1, Operation: 1, Permission: 1}}},
			func() Message { return &DeleteAclsRequest{} }},
		"DeleteAclsResponse": {KeyDeleteAcls, &DeleteAclsResponse{Results: []DeleteAclsResult{{Matching: []DeleteAclsMatch{{ACL: testACL}}}, {ACLResult: ACLResult{ErrorCode: 31, ErrorMessage: "no"}}}},
			func() Message { return &DeleteAclsResponse{} }},
		"DescribeAclsRequest": {KeyDescribeAcls, &DescribeAclsRequest{testFilter},
			func() Message { return &DescribeAclsRequest{} }},
		"DescribeAclsResponse": {KeyDescribeAcls, &DescribeAclsResponse{Resources: []ACLResource{{ResourceType: 2, ResourceName: "t", PatternType: 4, ACLs: []ACLEntry{{"User:a", "*", 3, 3}}}}},
			func() Message { return &DescribeAclsResponse{} }},
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
		{KeyInitProducerID, 0, true},
		{KeyInitProducerID, 4, true},
		{KeyInitProducerID, 5, false}, // adds only an error for transactions
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

// A batch says which producer sent it in its header; the CRC covers those
// fields, so stamping them must leave a valid batch.
func TestBatchProducerFields(t *testing.T) {
	b := EncodeBatch([]Record{{Value: []byte("a")}, {Value: []byte("b")}, {Value: []byte("c")}})
	info, code := ParseBatch(b, 1<<20)
	if code != ErrNone || info.ProducerID != -1 || info.ProducerEpoch != -1 || info.BaseSequence != -1 {
		t.Fatalf("plain batch: %+v, error %d", info, code)
	}
	SetBatchProducer(b, 1<<40|7, 2, 40)
	info, code = ParseBatch(b, 1<<20)
	if code != ErrNone || info.ProducerID != 1<<40|7 || info.ProducerEpoch != 2 || info.BaseSequence != 40 || info.Count != 3 {
		t.Fatalf("stamped batch: %+v, error %d", info, code)
	}
	if peek, ok := PeekBatch(b); !ok || peek.ProducerID != info.ProducerID || peek.ProducerEpoch != 2 || peek.BaseSequence != 40 || peek.Count != 3 {
		t.Fatalf("PeekBatch = %+v, %v", peek, ok)
	}
	if _, records, err := DecodeBatch(b); err != nil || len(records) != 3 || string(records[2].Value) != "c" {
		t.Fatalf("records after stamping: %v, %v", records, err)
	}
	// LogValidator.java: a producer id needs a sequence.
	SetBatchProducer(b, 7, 0, -1)
	if _, code := ParseBatch(b, 1<<20); code != ErrInvalidRecord {
		t.Errorf("producer id without a sequence: error %d, want %d", code, ErrInvalidRecord)
	}
	// Without a producer id the other two fields mean nothing.
	SetBatchProducer(b, -1, 5, -9)
	if _, code := ParseBatch(b, 1<<20); code != ErrNone {
		t.Errorf("no producer id: error %d", code)
	}
}

// AddSequence is DefaultRecordBatch.incrementSequence: sequences run from 0
// to 2^31-1 and then start again at 0.
func TestAddSequence(t *testing.T) {
	const top = 1<<31 - 1
	for _, tt := range []struct{ seq, n, want int32 }{
		{0, 0, 0}, {5, 3, 8}, {top - 1, 1, top}, {top, 1, 0}, {top, 5, 4}, {top - 1, 3, 1}, {top, top, top - 1},
	} {
		if got := AddSequence(tt.seq, tt.n); got != tt.want {
			t.Errorf("AddSequence(%d, %d) = %d, want %d", tt.seq, tt.n, got, tt.want)
		}
	}
}

// AuthBytes is not nullable: an empty one must be encoded as empty, at
// every version, or the Java client fails to parse the response.
func TestSaslAuthBytesNeverNull(t *testing.T) {
	for v := int16(0); v <= 2; v++ {
		for _, m := range []Message{&SaslAuthenticateResponse{ErrorCode: 58}, &SaslAuthenticateRequest{}} {
			var w Writer
			m.Encode(&w, v)
			r := NewReader(w.B)
			if _, ok := m.(*SaslAuthenticateResponse); ok {
				r.Int16()
				r.NullableString(v >= 2)
			}
			if b := r.Bytes(v >= 2); b == nil || r.Err() != nil {
				t.Errorf("%T v%d: AuthBytes decoded as null (%v)", m, v, r.Err())
			}
		}
	}
}

var (
	testACL    = ACL{ResourceType: 2, ResourceName: "orders", PatternType: 3, Principal: "User:alice", Host: "*", Operation: 3, Permission: 3}
	anyName    = "orders"
	testFilter = ACLFilter{ResourceType: 2, ResourceName: &anyName, PatternType: 2, Operation: 1, Permission: 1}
)
