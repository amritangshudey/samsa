package wire

// Messages of the consumer group APIs, at their non-flexible versions.
// Layouts are from the message definitions in the Apache Kafka repository,
// read on 2026-11-10 (see docs/design/phase-4.md).

// nullableString reads a string that may be null, treating null as empty.
func (r *Reader) nullableString() string {
	s, _ := r.NullableString(false)
	return s
}

// bytesOrNil reads a byte sequence, treating null and empty alike as nil.
func (r *Reader) bytesOrNil() []byte {
	if b := r.Bytes(false); len(b) > 0 {
		return b
	}
	return nil
}

// nonNullBytes appends a byte sequence that may not be null.
func (w *Writer) nonNullBytes(p []byte) {
	w.Int32(int32(len(p)))
	w.B = append(w.B, p...)
}

// --- OffsetCommit (key 8), versions 2 to 7 ---

// OffsetCommitPartition is one partition's offset in an OffsetCommit request.
type OffsetCommitPartition struct {
	Index       int32
	Offset      int64
	LeaderEpoch int32 // v6+; -1 if unknown
	Metadata    string
}

// OffsetCommitTopic is one topic of an OffsetCommit request.
type OffsetCommitTopic struct {
	Name       string
	Partitions []OffsetCommitPartition
}

// OffsetCommitRequest is the body of an OffsetCommit request.
type OffsetCommitRequest struct {
	GroupID string
	// GenerationID is -1 and MemberID empty for a consumer that is not a
	// group member.
	GenerationID int32
	MemberID     string
	Topics       []OffsetCommitTopic
}

// Decode implements Message.
func (m *OffsetCommitRequest) Decode(r *Reader, v int16) {
	m.GroupID = r.String(false)
	m.GenerationID = r.Int32()
	m.MemberID = r.String(false)
	if v >= 7 {
		r.nullableString() // GroupInstanceId: static membership is not supported
	}
	if v <= 4 {
		r.Int64() // RetentionTimeMs
	}
	m.Topics = Array(r, false, func() OffsetCommitTopic {
		return OffsetCommitTopic{Name: r.String(false), Partitions: Array(r, false, func() OffsetCommitPartition {
			p := OffsetCommitPartition{Index: r.Int32(), Offset: r.Int64(), LeaderEpoch: -1}
			if v >= 6 {
				p.LeaderEpoch = r.Int32()
			}
			p.Metadata = r.nullableString()
			return p
		})}
	})
}

// Encode implements Message.
func (m *OffsetCommitRequest) Encode(w *Writer, v int16) {
	w.String(false, m.GroupID)
	w.Int32(m.GenerationID)
	w.String(false, m.MemberID)
	if v >= 7 {
		w.NullString(false)
	}
	if v <= 4 {
		w.Int64(-1)
	}
	w.ArrayLen(false, len(m.Topics))
	for _, t := range m.Topics {
		w.String(false, t.Name)
		w.ArrayLen(false, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Int64(p.Offset)
			if v >= 6 {
				w.Int32(p.LeaderEpoch)
			}
			w.String(false, p.Metadata)
		}
	}
}

// PartitionError is a partition index with an error code.
type PartitionError struct {
	Index     int32
	ErrorCode int16
}

// OffsetCommitTopicResponse is one topic of an OffsetCommit response.
type OffsetCommitTopicResponse struct {
	Name       string
	Partitions []PartitionError
}

// OffsetCommitResponse is the body of an OffsetCommit response.
type OffsetCommitResponse struct{ Topics []OffsetCommitTopicResponse }

// Decode implements Message.
func (m *OffsetCommitResponse) Decode(r *Reader, v int16) {
	if v >= 3 {
		r.Int32() // ThrottleTimeMs
	}
	m.Topics = Array(r, false, func() OffsetCommitTopicResponse {
		return OffsetCommitTopicResponse{Name: r.String(false), Partitions: Array(r, false, func() PartitionError {
			return PartitionError{Index: r.Int32(), ErrorCode: r.Int16()}
		})}
	})
}

// Encode implements Message.
func (m *OffsetCommitResponse) Encode(w *Writer, v int16) {
	if v >= 3 {
		w.Int32(0)
	}
	w.ArrayLen(false, len(m.Topics))
	for _, t := range m.Topics {
		w.String(false, t.Name)
		w.ArrayLen(false, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Int16(p.ErrorCode)
		}
	}
}

// --- OffsetFetch (key 9), versions 1 to 5 ---

// OffsetFetchTopic is one topic of an OffsetFetch request.
type OffsetFetchTopic struct {
	Name       string
	Partitions []int32
}

// OffsetFetchRequest is the body of an OffsetFetch request.
type OffsetFetchRequest struct {
	GroupID string
	// AllTopics is set when the topic array is null (v2+): every partition
	// the group has committed.
	AllTopics bool
	Topics    []OffsetFetchTopic
}

// Decode implements Message.
func (m *OffsetFetchRequest) Decode(r *Reader, v int16) {
	m.GroupID = r.String(false)
	n := r.ArrayLen(false)
	if n < 0 {
		if v < 2 {
			r.fail("null topic array before version 2")
		}
		m.AllTopics = r.Err() == nil
		return
	}
	for ; n > 0 && r.Err() == nil; n-- {
		m.Topics = append(m.Topics, OffsetFetchTopic{Name: r.String(false), Partitions: readInt32s(r)})
	}
}

// Encode implements Message.
func (m *OffsetFetchRequest) Encode(w *Writer, v int16) {
	w.String(false, m.GroupID)
	if m.AllTopics && v >= 2 {
		w.ArrayLen(false, -1)
		return
	}
	w.ArrayLen(false, len(m.Topics))
	for _, t := range m.Topics {
		w.String(false, t.Name)
		writeInt32s(w, t.Partitions)
	}
}

// OffsetFetchPartition is one partition's committed offset.
type OffsetFetchPartition struct {
	Index       int32
	Offset      int64 // -1: nothing committed
	LeaderEpoch int32 // v5+
	Metadata    string
	ErrorCode   int16
}

// OffsetFetchTopicResponse is one topic of an OffsetFetch response.
type OffsetFetchTopicResponse struct {
	Name       string
	Partitions []OffsetFetchPartition
}

// OffsetFetchResponse is the body of an OffsetFetch response.
type OffsetFetchResponse struct {
	Topics    []OffsetFetchTopicResponse
	ErrorCode int16 // v2+
}

// Decode implements Message.
func (m *OffsetFetchResponse) Decode(r *Reader, v int16) {
	if v >= 3 {
		r.Int32() // ThrottleTimeMs
	}
	m.Topics = Array(r, false, func() OffsetFetchTopicResponse {
		return OffsetFetchTopicResponse{Name: r.String(false), Partitions: Array(r, false, func() OffsetFetchPartition {
			p := OffsetFetchPartition{Index: r.Int32(), Offset: r.Int64(), LeaderEpoch: -1}
			if v >= 5 {
				p.LeaderEpoch = r.Int32()
			}
			p.Metadata = r.nullableString()
			p.ErrorCode = r.Int16()
			return p
		})}
	})
	if v >= 2 {
		m.ErrorCode = r.Int16()
	}
}

// Encode implements Message.
func (m *OffsetFetchResponse) Encode(w *Writer, v int16) {
	if v >= 3 {
		w.Int32(0)
	}
	w.ArrayLen(false, len(m.Topics))
	for _, t := range m.Topics {
		w.String(false, t.Name)
		w.ArrayLen(false, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Int64(p.Offset)
			if v >= 5 {
				w.Int32(p.LeaderEpoch)
			}
			w.String(false, p.Metadata)
			w.Int16(p.ErrorCode)
		}
	}
	if v >= 2 {
		w.Int16(m.ErrorCode)
	}
}

// --- FindCoordinator (key 10), versions 0 to 2 ---

// FindCoordinatorRequest is the body of a FindCoordinator request.
type FindCoordinatorRequest struct {
	Key     string
	KeyType int8 // v1+; 0 is a consumer group
}

// Decode implements Message.
func (m *FindCoordinatorRequest) Decode(r *Reader, v int16) {
	m.Key = r.String(false)
	if v >= 1 {
		m.KeyType = r.Int8()
	}
}

// Encode implements Message.
func (m *FindCoordinatorRequest) Encode(w *Writer, v int16) {
	w.String(false, m.Key)
	if v >= 1 {
		w.Int8(m.KeyType)
	}
}

// FindCoordinatorResponse is the body of a FindCoordinator response.
type FindCoordinatorResponse struct {
	ErrorCode int16
	NodeID    int32
	Host      string
	Port      int32
}

// Decode implements Message.
func (m *FindCoordinatorResponse) Decode(r *Reader, v int16) {
	if v >= 1 {
		r.Int32() // ThrottleTimeMs
	}
	m.ErrorCode = r.Int16()
	if v >= 1 {
		r.nullableString() // ErrorMessage
	}
	m.NodeID = r.Int32()
	m.Host = r.String(false)
	m.Port = r.Int32()
}

// Encode implements Message.
func (m *FindCoordinatorResponse) Encode(w *Writer, v int16) {
	if v >= 1 {
		w.Int32(0)
	}
	w.Int16(m.ErrorCode)
	if v >= 1 {
		w.NullString(false)
	}
	w.Int32(m.NodeID)
	w.String(false, m.Host)
	w.Int32(m.Port)
}

// --- JoinGroup (key 11), versions 0 to 5 ---

// GroupProtocol is an assignment strategy a member supports, with the
// metadata (for consumers: the subscription) that goes with it. The broker
// never looks inside Metadata.
type GroupProtocol struct {
	Name     string
	Metadata []byte
}

// JoinGroupRequest is the body of a JoinGroup request.
type JoinGroupRequest struct {
	GroupID          string
	SessionTimeoutMs int32
	// RebalanceTimeoutMs is -1 in version 0, which has no such field: the
	// session timeout is used instead.
	RebalanceTimeoutMs int32
	MemberID           string // empty on a first join
	ProtocolType       string
	Protocols          []GroupProtocol
}

// Decode implements Message.
func (m *JoinGroupRequest) Decode(r *Reader, v int16) {
	m.GroupID = r.String(false)
	m.SessionTimeoutMs = r.Int32()
	m.RebalanceTimeoutMs = -1
	if v >= 1 {
		m.RebalanceTimeoutMs = r.Int32()
	}
	m.MemberID = r.String(false)
	if v >= 5 {
		r.nullableString() // GroupInstanceId
	}
	m.ProtocolType = r.String(false)
	m.Protocols = Array(r, false, func() GroupProtocol {
		return GroupProtocol{Name: r.String(false), Metadata: r.bytesOrNil()}
	})
}

// Encode implements Message.
func (m *JoinGroupRequest) Encode(w *Writer, v int16) {
	w.String(false, m.GroupID)
	w.Int32(m.SessionTimeoutMs)
	if v >= 1 {
		w.Int32(m.RebalanceTimeoutMs)
	}
	w.String(false, m.MemberID)
	if v >= 5 {
		w.NullString(false)
	}
	w.String(false, m.ProtocolType)
	w.ArrayLen(false, len(m.Protocols))
	for _, p := range m.Protocols {
		w.String(false, p.Name)
		w.nonNullBytes(p.Metadata)
	}
}

// GroupMember is a member in a JoinGroup response: its id and the metadata
// of the chosen protocol.
type GroupMember struct {
	MemberID string
	Metadata []byte
}

// JoinGroupResponse is the body of a JoinGroup response.
type JoinGroupResponse struct {
	ErrorCode    int16
	GenerationID int32
	ProtocolName string
	Leader       string
	MemberID     string
	Members      []GroupMember // only the leader receives the list
}

// Decode implements Message.
func (m *JoinGroupResponse) Decode(r *Reader, v int16) {
	if v >= 2 {
		r.Int32() // ThrottleTimeMs
	}
	m.ErrorCode = r.Int16()
	m.GenerationID = r.Int32()
	m.ProtocolName = r.String(false)
	m.Leader = r.String(false)
	m.MemberID = r.String(false)
	m.Members = Array(r, false, func() GroupMember {
		g := GroupMember{MemberID: r.String(false)}
		if v >= 5 {
			r.nullableString() // GroupInstanceId
		}
		g.Metadata = r.bytesOrNil()
		return g
	})
}

// Encode implements Message.
func (m *JoinGroupResponse) Encode(w *Writer, v int16) {
	if v >= 2 {
		w.Int32(0)
	}
	w.Int16(m.ErrorCode)
	w.Int32(m.GenerationID)
	w.String(false, m.ProtocolName)
	w.String(false, m.Leader)
	w.String(false, m.MemberID)
	w.ArrayLen(false, len(m.Members))
	for _, g := range m.Members {
		w.String(false, g.MemberID)
		if v >= 5 {
			w.NullString(false)
		}
		w.nonNullBytes(g.Metadata)
	}
}

// --- Heartbeat (key 12), versions 0 to 3 ---

// HeartbeatRequest is the body of a Heartbeat request.
type HeartbeatRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
}

// Decode implements Message.
func (m *HeartbeatRequest) Decode(r *Reader, v int16) {
	m.GroupID = r.String(false)
	m.GenerationID = r.Int32()
	m.MemberID = r.String(false)
	if v >= 3 {
		r.nullableString() // GroupInstanceId
	}
}

// Encode implements Message.
func (m *HeartbeatRequest) Encode(w *Writer, v int16) {
	w.String(false, m.GroupID)
	w.Int32(m.GenerationID)
	w.String(false, m.MemberID)
	if v >= 3 {
		w.NullString(false)
	}
}

// HeartbeatResponse is the body of a Heartbeat response.
type HeartbeatResponse struct{ ErrorCode int16 }

// Decode implements Message.
func (m *HeartbeatResponse) Decode(r *Reader, v int16) {
	if v >= 1 {
		r.Int32() // ThrottleTimeMs
	}
	m.ErrorCode = r.Int16()
}

// Encode implements Message.
func (m *HeartbeatResponse) Encode(w *Writer, v int16) {
	if v >= 1 {
		w.Int32(0)
	}
	w.Int16(m.ErrorCode)
}

// --- LeaveGroup (key 13), versions 0 to 3 ---

// LeaveGroupRequest is the body of a LeaveGroup request. Versions 0 to 2
// carry exactly one member id.
type LeaveGroupRequest struct {
	GroupID string
	Members []string
}

// Decode implements Message.
func (m *LeaveGroupRequest) Decode(r *Reader, v int16) {
	m.GroupID = r.String(false)
	if v <= 2 {
		m.Members = []string{r.String(false)}
		return
	}
	m.Members = Array(r, false, func() string {
		id := r.String(false)
		r.nullableString() // GroupInstanceId
		return id
	})
}

// Encode implements Message.
func (m *LeaveGroupRequest) Encode(w *Writer, v int16) {
	w.String(false, m.GroupID)
	if v <= 2 {
		id := ""
		if len(m.Members) > 0 {
			id = m.Members[0]
		}
		w.String(false, id)
		return
	}
	w.ArrayLen(false, len(m.Members))
	for _, id := range m.Members {
		w.String(false, id)
		w.NullString(false)
	}
}

// MemberError is a member id with an error code.
type MemberError struct {
	MemberID  string
	ErrorCode int16
}

// LeaveGroupResponse is the body of a LeaveGroup response.
type LeaveGroupResponse struct {
	ErrorCode int16
	Members   []MemberError // v3+
}

// Decode implements Message.
func (m *LeaveGroupResponse) Decode(r *Reader, v int16) {
	if v >= 1 {
		r.Int32() // ThrottleTimeMs
	}
	m.ErrorCode = r.Int16()
	if v >= 3 {
		m.Members = Array(r, false, func() MemberError {
			e := MemberError{MemberID: r.String(false)}
			r.nullableString() // GroupInstanceId
			e.ErrorCode = r.Int16()
			return e
		})
	}
}

// Encode implements Message.
func (m *LeaveGroupResponse) Encode(w *Writer, v int16) {
	if v >= 1 {
		w.Int32(0)
	}
	w.Int16(m.ErrorCode)
	if v >= 3 {
		w.ArrayLen(false, len(m.Members))
		for _, e := range m.Members {
			w.String(false, e.MemberID)
			w.NullString(false)
			w.Int16(e.ErrorCode)
		}
	}
}

// --- SyncGroup (key 14), versions 0 to 3 ---

// MemberAssignment is what the group leader assigned to one member. The
// broker never looks inside Assignment.
type MemberAssignment struct {
	MemberID   string
	Assignment []byte
}

// SyncGroupRequest is the body of a SyncGroup request. Only the leader's
// carries assignments.
type SyncGroupRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
	Assignments  []MemberAssignment
}

// Decode implements Message.
func (m *SyncGroupRequest) Decode(r *Reader, v int16) {
	m.GroupID = r.String(false)
	m.GenerationID = r.Int32()
	m.MemberID = r.String(false)
	if v >= 3 {
		r.nullableString() // GroupInstanceId
	}
	m.Assignments = Array(r, false, func() MemberAssignment {
		return MemberAssignment{MemberID: r.String(false), Assignment: r.bytesOrNil()}
	})
}

// Encode implements Message.
func (m *SyncGroupRequest) Encode(w *Writer, v int16) {
	w.String(false, m.GroupID)
	w.Int32(m.GenerationID)
	w.String(false, m.MemberID)
	if v >= 3 {
		w.NullString(false)
	}
	w.ArrayLen(false, len(m.Assignments))
	for _, a := range m.Assignments {
		w.String(false, a.MemberID)
		w.nonNullBytes(a.Assignment)
	}
}

// SyncGroupResponse is the body of a SyncGroup response.
type SyncGroupResponse struct {
	ErrorCode  int16
	Assignment []byte
}

// Decode implements Message.
func (m *SyncGroupResponse) Decode(r *Reader, v int16) {
	if v >= 1 {
		r.Int32() // ThrottleTimeMs
	}
	m.ErrorCode = r.Int16()
	m.Assignment = r.bytesOrNil()
}

// Encode implements Message.
func (m *SyncGroupResponse) Encode(w *Writer, v int16) {
	if v >= 1 {
		w.Int32(0)
	}
	w.Int16(m.ErrorCode)
	w.nonNullBytes(m.Assignment)
}
