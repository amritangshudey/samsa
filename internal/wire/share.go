package wire

// Share groups (KIP-932), from the message schemas of kafka-clients 4.4.0
// (docs/design/c22-share-groups.md). Every version is flexible.

// Share acknowledgement types.
const (
	AckGap     int8 = 0
	AckAccept  int8 = 1
	AckRelease int8 = 2
	AckReject  int8 = 3
	AckRenew   int8 = 4
)

func readInt8s(r *Reader, flex bool) []int8 { return Array(r, flex, r.Int8) }

func writeInt8s(w *Writer, flex bool, vs []int8) {
	w.ArrayLen(flex, len(vs))
	for _, v := range vs {
		w.Int8(v)
	}
}

// --- ShareGroupHeartbeat (76) ----------------------------------------------

// ShareGroupHeartbeatRequest is the body of a ShareGroupHeartbeat request
// (key 76).
type ShareGroupHeartbeatRequest struct {
	GroupID          string
	MemberID         string
	MemberEpoch      int32
	RackID           string // empty: null
	SubscribedTopics []string
}

// Decode implements Message.
func (m *ShareGroupHeartbeatRequest) Decode(r *Reader, _ int16) {
	m.GroupID = r.String(true)
	m.MemberID = r.String(true)
	m.MemberEpoch = r.Int32()
	m.RackID, _ = r.NullableString(true)
	m.SubscribedTopics = r.nullableStrings(true)
	r.Tags(true)
}

// Encode implements Message.
func (m *ShareGroupHeartbeatRequest) Encode(w *Writer, _ int16) {
	w.String(true, m.GroupID)
	w.String(true, m.MemberID)
	w.Int32(m.MemberEpoch)
	w.optString(true, m.RackID)
	w.nullableStrings(true, m.SubscribedTopics)
	w.Tags(true)
}

// ShareGroupHeartbeatResponse is the body of a ShareGroupHeartbeat
// response.
type ShareGroupHeartbeatResponse struct {
	ErrorCode           int16
	ErrorMessage        string // empty: null
	MemberID            string // empty: null
	MemberEpoch         int32
	HeartbeatIntervalMs int32
	Assignment          []TopicPartitionIDs
	HasAssignment       bool
}

// Decode implements Message.
func (m *ShareGroupHeartbeatResponse) Decode(r *Reader, _ int16) {
	r.Int32() // ThrottleTimeMs
	m.ErrorCode = r.Int16()
	m.ErrorMessage, _ = r.NullableString(true)
	m.MemberID, _ = r.NullableString(true)
	m.MemberEpoch = r.Int32()
	m.HeartbeatIntervalMs = r.Int32()
	if r.Int8() >= 0 {
		m.HasAssignment = true
		m.Assignment = Array(r, true, func() TopicPartitionIDs { return readTopicPartitionIDs(r, true) })
		r.Tags(true)
	}
	r.Tags(true)
}

// Encode implements Message.
func (m *ShareGroupHeartbeatResponse) Encode(w *Writer, _ int16) {
	w.Int32(0)
	w.Int16(m.ErrorCode)
	w.optString(true, m.ErrorMessage)
	w.optString(true, m.MemberID)
	w.Int32(m.MemberEpoch)
	w.Int32(m.HeartbeatIntervalMs)
	if m.HasAssignment {
		w.Int8(1)
		w.ArrayLen(true, len(m.Assignment))
		for _, t := range m.Assignment {
			writeTopicPartitionIDs(w, true, t)
		}
		w.Tags(true)
	} else {
		w.Int8(-1)
	}
	w.Tags(true)
}

// --- ShareGroupDescribe (77) -----------------------------------------------

// ShareGroupDescribeRequest is the body of a ShareGroupDescribe request
// (key 77).
type ShareGroupDescribeRequest struct {
	Groups                      []string
	IncludeAuthorizedOperations bool
}

// Decode implements Message.
func (m *ShareGroupDescribeRequest) Decode(r *Reader, _ int16) {
	m.Groups = readStrings(r, true)
	m.IncludeAuthorizedOperations = r.Bool()
	r.Tags(true)
}

// Encode implements Message.
func (m *ShareGroupDescribeRequest) Encode(w *Writer, _ int16) {
	writeStrings(w, true, m.Groups)
	w.Bool(m.IncludeAuthorizedOperations)
	w.Tags(true)
}

// ShareGroupDescribeMember is one member of a described share group.
type ShareGroupDescribeMember struct {
	MemberID         string
	RackID           string // empty: null
	MemberEpoch      int32
	ClientID         string
	ClientHost       string
	SubscribedTopics []string
	Assignment       []NamedTopicPartitions
}

// ShareGroupDescribeGroup is one group of a ShareGroupDescribe response.
type ShareGroupDescribeGroup struct {
	ErrorCode       int16
	ErrorMessage    string // empty: null
	GroupID         string
	State           string
	GroupEpoch      int32
	AssignmentEpoch int32
	AssignorName    string
	Members         []ShareGroupDescribeMember
}

// ShareGroupDescribeResponse is the body of a ShareGroupDescribe response.
type ShareGroupDescribeResponse struct{ Groups []ShareGroupDescribeGroup }

// Decode implements Message.
func (m *ShareGroupDescribeResponse) Decode(r *Reader, _ int16) {
	r.Int32() // ThrottleTimeMs
	m.Groups = Array(r, true, func() ShareGroupDescribeGroup {
		var g ShareGroupDescribeGroup
		g.ErrorCode = r.Int16()
		g.ErrorMessage, _ = r.NullableString(true)
		g.GroupID = r.String(true)
		g.State = r.String(true)
		g.GroupEpoch = r.Int32()
		g.AssignmentEpoch = r.Int32()
		g.AssignorName = r.String(true)
		g.Members = Array(r, true, func() ShareGroupDescribeMember {
			var mem ShareGroupDescribeMember
			mem.MemberID = r.String(true)
			mem.RackID, _ = r.NullableString(true)
			mem.MemberEpoch = r.Int32()
			mem.ClientID = r.String(true)
			mem.ClientHost = r.String(true)
			mem.SubscribedTopics = readStrings(r, true)
			mem.Assignment = readNamed(r)
			r.Tags(true)
			return mem
		})
		r.Int32() // AuthorizedOperations
		r.Tags(true)
		return g
	})
	r.Tags(true)
}

// Encode implements Message.
func (m *ShareGroupDescribeResponse) Encode(w *Writer, _ int16) {
	w.Int32(0)
	w.ArrayLen(true, len(m.Groups))
	for _, g := range m.Groups {
		w.Int16(g.ErrorCode)
		w.optString(true, g.ErrorMessage)
		w.String(true, g.GroupID)
		w.String(true, g.State)
		w.Int32(g.GroupEpoch)
		w.Int32(g.AssignmentEpoch)
		w.String(true, g.AssignorName)
		w.ArrayLen(true, len(g.Members))
		for _, mem := range g.Members {
			w.String(true, mem.MemberID)
			w.optString(true, mem.RackID)
			w.Int32(mem.MemberEpoch)
			w.String(true, mem.ClientID)
			w.String(true, mem.ClientHost)
			writeStrings(w, true, mem.SubscribedTopics)
			writeNamed(w, mem.Assignment)
			w.Tags(true)
		}
		w.Int32(authorizedOperationsOmitted)
		w.Tags(true)
	}
	w.Tags(true)
}

// --- ShareFetch (78) & ShareAcknowledge (79) -------------------------------

// ShareAckBatch is one contiguous range of offsets to acknowledge.
type ShareAckBatch struct {
	FirstOffset      int64
	LastOffset       int64
	AcknowledgeTypes []int8
}

func readAckBatches(r *Reader) []ShareAckBatch {
	return Array(r, true, func() ShareAckBatch {
		b := ShareAckBatch{FirstOffset: r.Int64(), LastOffset: r.Int64(), AcknowledgeTypes: readInt8s(r, true)}
		r.Tags(true)
		return b
	})
}

func writeAckBatches(w *Writer, batches []ShareAckBatch) {
	w.ArrayLen(true, len(batches))
	for _, b := range batches {
		w.Int64(b.FirstOffset)
		w.Int64(b.LastOffset)
		writeInt8s(w, true, b.AcknowledgeTypes)
		w.Tags(true)
	}
}

// ShareFetchPartition is one partition of a ShareFetch request.
type ShareFetchPartition struct {
	Index      int32
	AckBatches []ShareAckBatch
}

// ShareFetchTopic is one topic of a ShareFetch request.
type ShareFetchTopic struct {
	TopicID    UUID
	Partitions []ShareFetchPartition
}

// ShareFetchRequest is the body of a ShareFetch request (key 78, v1-2).
type ShareFetchRequest struct {
	GroupID           string
	MemberID          string
	ShareSessionEpoch int32
	MaxWaitMs         int32
	MinBytes          int32
	MaxBytes          int32
	MaxRecords        int32
	BatchSize         int32
	ShareAcquireMode  int8 // v2+
	IsRenewAck        bool // v2+
	Topics            []ShareFetchTopic
	ForgottenTopics   []TopicPartitionIDs
}

// Decode implements Message.
func (m *ShareFetchRequest) Decode(r *Reader, v int16) {
	m.GroupID, _ = r.NullableString(true)
	m.MemberID, _ = r.NullableString(true)
	m.ShareSessionEpoch = r.Int32()
	m.MaxWaitMs = r.Int32()
	m.MinBytes = r.Int32()
	m.MaxBytes = r.Int32()
	m.MaxRecords = r.Int32()
	m.BatchSize = r.Int32()
	if v >= 2 {
		m.ShareAcquireMode = r.Int8()
		m.IsRenewAck = r.Bool()
	}
	m.Topics = Array(r, true, func() ShareFetchTopic {
		t := ShareFetchTopic{TopicID: r.uuid()}
		t.Partitions = Array(r, true, func() ShareFetchPartition {
			p := ShareFetchPartition{Index: r.Int32(), AckBatches: readAckBatches(r)}
			r.Tags(true)
			return p
		})
		r.Tags(true)
		return t
	})
	m.ForgottenTopics = Array(r, true, func() TopicPartitionIDs { return readTopicPartitionIDs(r, true) })
	r.Tags(true)
}

// Encode implements Message.
func (m *ShareFetchRequest) Encode(w *Writer, v int16) {
	w.optString(true, m.GroupID)
	w.optString(true, m.MemberID)
	w.Int32(m.ShareSessionEpoch)
	w.Int32(m.MaxWaitMs)
	w.Int32(m.MinBytes)
	w.Int32(m.MaxBytes)
	w.Int32(m.MaxRecords)
	w.Int32(m.BatchSize)
	if v >= 2 {
		w.Int8(m.ShareAcquireMode)
		w.Bool(m.IsRenewAck)
	}
	w.ArrayLen(true, len(m.Topics))
	for _, t := range m.Topics {
		w.uuid(t.TopicID)
		w.ArrayLen(true, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			writeAckBatches(w, p.AckBatches)
			w.Tags(true)
		}
		w.Tags(true)
	}
	w.ArrayLen(true, len(m.ForgottenTopics))
	for _, f := range m.ForgottenTopics {
		writeTopicPartitionIDs(w, true, f)
	}
	w.Tags(true)
}

// AcquiredRange is one contiguous range of acquired offsets.
type AcquiredRange struct {
	FirstOffset   int64
	LastOffset    int64
	DeliveryCount int16
}

// ShareFetchPartitionResponse is one partition of a ShareFetch response.
type ShareFetchPartitionResponse struct {
	Index           int32
	ErrorCode       int16
	ErrorMessage    string
	AckErrorCode    int16
	AckErrorMessage string
	LeaderID        int32
	LeaderEpoch     int32
	Records         []byte
	Acquired        []AcquiredRange
}

// ShareFetchTopicResponse is one topic of a ShareFetch response.
type ShareFetchTopicResponse struct {
	TopicID    UUID
	Partitions []ShareFetchPartitionResponse
}

// ShareFetchResponse is the body of a ShareFetch response (key 78, v1-2).
type ShareFetchResponse struct {
	ErrorCode                int16
	ErrorMessage             string
	AcquisitionLockTimeoutMs int32
	Responses                []ShareFetchTopicResponse
}

// Decode implements Message.
func (m *ShareFetchResponse) Decode(r *Reader, _ int16) {
	r.Int32() // ThrottleTimeMs
	m.ErrorCode = r.Int16()
	m.ErrorMessage, _ = r.NullableString(true)
	m.AcquisitionLockTimeoutMs = r.Int32()
	m.Responses = Array(r, true, func() ShareFetchTopicResponse {
		t := ShareFetchTopicResponse{TopicID: r.uuid()}
		t.Partitions = Array(r, true, func() ShareFetchPartitionResponse {
			var p ShareFetchPartitionResponse
			p.Index = r.Int32()
			p.ErrorCode = r.Int16()
			p.ErrorMessage, _ = r.NullableString(true)
			p.AckErrorCode = r.Int16()
			p.AckErrorMessage, _ = r.NullableString(true)
			p.LeaderID = r.Int32()
			p.LeaderEpoch = r.Int32()
			r.Tags(true) // CurrentLeader tags
			p.Records = r.Bytes(true)
			p.Acquired = Array(r, true, func() AcquiredRange {
				a := AcquiredRange{FirstOffset: r.Int64(), LastOffset: r.Int64(), DeliveryCount: r.Int16()}
				r.Tags(true)
				return a
			})
			r.Tags(true)
			return p
		})
		r.Tags(true)
		return t
	})
	Array(r, true, func() struct{} { // NodeEndpoints
		r.Int32()
		r.String(true)
		r.Int32()
		r.NullableString(true)
		r.Tags(true)
		return struct{}{}
	})
	r.Tags(true)
}

// Encode implements Message.
func (m *ShareFetchResponse) Encode(w *Writer, _ int16) {
	w.Int32(0)
	w.Int16(m.ErrorCode)
	w.optString(true, m.ErrorMessage)
	w.Int32(m.AcquisitionLockTimeoutMs)
	w.ArrayLen(true, len(m.Responses))
	for _, t := range m.Responses {
		w.uuid(t.TopicID)
		w.ArrayLen(true, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Int16(p.ErrorCode)
			w.optString(true, p.ErrorMessage)
			w.Int16(p.AckErrorCode)
			w.optString(true, p.AckErrorMessage)
			w.Int32(p.LeaderID)
			w.Int32(p.LeaderEpoch)
			w.Tags(true)
			if p.Records == nil {
				w.Bytes(true, []byte{}) // non-null in v1+
			} else {
				w.Bytes(true, p.Records)
			}
			w.ArrayLen(true, len(p.Acquired))
			for _, a := range p.Acquired {
				w.Int64(a.FirstOffset)
				w.Int64(a.LastOffset)
				w.Int16(a.DeliveryCount)
				w.Tags(true)
			}
			w.Tags(true)
		}
		w.Tags(true)
	}
	w.ArrayLen(true, 0) // NodeEndpoints
	w.Tags(true)
}

// ShareAcknowledgeRequest is the body of a ShareAcknowledge request (key 79,
// v1-2).
type ShareAcknowledgeRequest struct {
	GroupID           string
	MemberID          string
	ShareSessionEpoch int32
	IsRenewAck        bool // v2+
	Topics            []ShareFetchTopic
}

// Decode implements Message.
func (m *ShareAcknowledgeRequest) Decode(r *Reader, v int16) {
	m.GroupID, _ = r.NullableString(true)
	m.MemberID, _ = r.NullableString(true)
	m.ShareSessionEpoch = r.Int32()
	if v >= 2 {
		m.IsRenewAck = r.Bool()
	}
	m.Topics = Array(r, true, func() ShareFetchTopic {
		t := ShareFetchTopic{TopicID: r.uuid()}
		t.Partitions = Array(r, true, func() ShareFetchPartition {
			p := ShareFetchPartition{Index: r.Int32(), AckBatches: readAckBatches(r)}
			r.Tags(true)
			return p
		})
		r.Tags(true)
		return t
	})
	r.Tags(true)
}

// Encode implements Message.
func (m *ShareAcknowledgeRequest) Encode(w *Writer, v int16) {
	w.optString(true, m.GroupID)
	w.optString(true, m.MemberID)
	w.Int32(m.ShareSessionEpoch)
	if v >= 2 {
		w.Bool(m.IsRenewAck)
	}
	w.ArrayLen(true, len(m.Topics))
	for _, t := range m.Topics {
		w.uuid(t.TopicID)
		w.ArrayLen(true, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			writeAckBatches(w, p.AckBatches)
			w.Tags(true)
		}
		w.Tags(true)
	}
	w.Tags(true)
}

// ShareAckPartitionResponse is one partition of a ShareAcknowledge response.
type ShareAckPartitionResponse struct {
	Index        int32
	ErrorCode    int16
	ErrorMessage string
	LeaderID     int32
	LeaderEpoch  int32
}

// ShareAckTopicResponse is one topic of a ShareAcknowledge response.
type ShareAckTopicResponse struct {
	TopicID    UUID
	Partitions []ShareAckPartitionResponse
}

// ShareAcknowledgeResponse is the body of a ShareAcknowledge response
// (key 79, v1-2).
type ShareAcknowledgeResponse struct {
	ErrorCode                int16
	ErrorMessage             string
	AcquisitionLockTimeoutMs int32 // v2+
	Responses                []ShareAckTopicResponse
}

// Decode implements Message.
func (m *ShareAcknowledgeResponse) Decode(r *Reader, v int16) {
	r.Int32() // ThrottleTimeMs
	m.ErrorCode = r.Int16()
	m.ErrorMessage, _ = r.NullableString(true)
	if v >= 2 {
		m.AcquisitionLockTimeoutMs = r.Int32()
	}
	m.Responses = Array(r, true, func() ShareAckTopicResponse {
		t := ShareAckTopicResponse{TopicID: r.uuid()}
		t.Partitions = Array(r, true, func() ShareAckPartitionResponse {
			p := ShareAckPartitionResponse{Index: r.Int32(), ErrorCode: r.Int16()}
			p.ErrorMessage, _ = r.NullableString(true)
			p.LeaderID = r.Int32()
			p.LeaderEpoch = r.Int32()
			r.Tags(true) // CurrentLeader tags
			r.Tags(true)
			return p
		})
		r.Tags(true)
		return t
	})
	Array(r, true, func() struct{} { // NodeEndpoints
		r.Int32()
		r.String(true)
		r.Int32()
		r.NullableString(true)
		r.Tags(true)
		return struct{}{}
	})
	r.Tags(true)
}

// Encode implements Message.
func (m *ShareAcknowledgeResponse) Encode(w *Writer, v int16) {
	w.Int32(0)
	w.Int16(m.ErrorCode)
	w.optString(true, m.ErrorMessage)
	if v >= 2 {
		w.Int32(m.AcquisitionLockTimeoutMs)
	}
	w.ArrayLen(true, len(m.Responses))
	for _, t := range m.Responses {
		w.uuid(t.TopicID)
		w.ArrayLen(true, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Int16(p.ErrorCode)
			w.optString(true, p.ErrorMessage)
			w.Int32(p.LeaderID)
			w.Int32(p.LeaderEpoch)
			w.Tags(true)
			w.Tags(true)
		}
		w.Tags(true)
	}
	w.ArrayLen(true, 0) // NodeEndpoints
	w.Tags(true)
}
