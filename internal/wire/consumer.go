package wire

// The consumer group protocol of KIP-848 and the API that tools use to
// read topic metadata a page at a time, from the message schemas of
// kafka-clients 4.4.0 (ConsumerGroupHeartbeat, ConsumerGroupDescribe and
// DescribeTopicPartitions; docs/design/c18-kip848.md). Every version is
// flexible.
//
// Where a schema field is a nullable struct that is not tagged, the
// encoding is one byte, -1 for null and 1 for present, followed by the
// struct's fields and its tagged fields (MessageDataGenerator.java).

// Member epochs with a meaning in a ConsumerGroupHeartbeat request.
const (
	EpochJoin      int32 = 0  // join the group
	EpochLeave     int32 = -1 // leave the group
	EpochRejoin    int32 = -2 // a static member will rejoin
	MemberClassic  int8  = 0  // ConsumerGroupDescribe member types
	MemberConsumer int8  = 1
)

// readNullableArray reads an array and tells a null one from an empty one:
// the first result is nil only for null.
func readNullableArray[T any](r *Reader, flex bool, elem func() T) []T {
	n := r.ArrayLen(flex)
	if n < 0 {
		return nil
	}
	out := make([]T, 0, min(n, 256))
	for ; n > 0 && r.Err() == nil; n-- {
		out = append(out, elem())
	}
	return out
}

// writeNullableArray writes nil as null and anything else as an array.
func writeNullableArray[T any](w *Writer, flex bool, items []T, elem func(T)) {
	if items == nil {
		w.ArrayLen(flex, -1)
		return
	}
	w.ArrayLen(flex, len(items))
	for _, it := range items {
		elem(it)
	}
}

func (r *Reader) nullableStrings(flex bool) []string {
	return readNullableArray(r, flex, func() string { return r.String(flex) })
}

func (w *Writer) nullableStrings(flex bool, ss []string) {
	writeNullableArray(w, flex, ss, func(s string) { w.String(flex, s) })
}

// TopicPartitionIDs is a topic, by id, and some of its partitions.
type TopicPartitionIDs struct {
	TopicID    UUID
	Partitions []int32
}

func readTopicPartitionIDs(r *Reader, flex bool) TopicPartitionIDs {
	t := TopicPartitionIDs{TopicID: r.uuid(), Partitions: readInt32s(r, flex)}
	r.Tags(flex)
	return t
}

func writeTopicPartitionIDs(w *Writer, flex bool, t TopicPartitionIDs) {
	w.uuid(t.TopicID)
	writeInt32s(w, flex, t.Partitions)
	w.Tags(flex)
}

// --- ConsumerGroupHeartbeat (68) -------------------------------------------

// ConsumerGroupHeartbeatRequest is the body of a ConsumerGroupHeartbeat
// request (key 68). Where the schema says null means "unchanged since the
// last heartbeat", the field is nil, or empty for a string.
type ConsumerGroupHeartbeatRequest struct {
	GroupID            string
	MemberID           string
	MemberEpoch        int32
	InstanceID         string // empty: null
	RackID             string // empty: null
	RebalanceTimeoutMs int32  // -1: unchanged
	// SubscribedTopics is nil if it did not change, else the whole
	// subscription.
	SubscribedTopics []string
	// SubscribedRegex (v1+) is empty if null.
	SubscribedRegex string
	// ServerAssignor is empty if null.
	ServerAssignor string
	// Owned is nil if it did not change, else every partition the member
	// still owns.
	Owned []TopicPartitionIDs
}

// Decode implements Message.
func (m *ConsumerGroupHeartbeatRequest) Decode(r *Reader, v int16) {
	m.GroupID = r.String(true)
	m.MemberID = r.String(true)
	m.MemberEpoch = r.Int32()
	m.InstanceID, _ = r.NullableString(true)
	m.RackID, _ = r.NullableString(true)
	m.RebalanceTimeoutMs = r.Int32()
	m.SubscribedTopics = r.nullableStrings(true)
	if v >= 1 {
		m.SubscribedRegex, _ = r.NullableString(true)
	}
	m.ServerAssignor, _ = r.NullableString(true)
	m.Owned = readNullableArray(r, true, func() TopicPartitionIDs { return readTopicPartitionIDs(r, true) })
	r.Tags(true)
}

// Encode implements Message.
func (m *ConsumerGroupHeartbeatRequest) Encode(w *Writer, v int16) {
	w.String(true, m.GroupID)
	w.String(true, m.MemberID)
	w.Int32(m.MemberEpoch)
	w.optString(true, m.InstanceID)
	w.optString(true, m.RackID)
	w.Int32(m.RebalanceTimeoutMs)
	w.nullableStrings(true, m.SubscribedTopics)
	if v >= 1 {
		w.optString(true, m.SubscribedRegex)
	}
	w.optString(true, m.ServerAssignor)
	writeNullableArray(w, true, m.Owned, func(t TopicPartitionIDs) { writeTopicPartitionIDs(w, true, t) })
	w.Tags(true)
}

// ConsumerGroupHeartbeatResponse is the body of a ConsumerGroupHeartbeat
// response.
type ConsumerGroupHeartbeatResponse struct {
	ErrorCode           int16
	ErrorMessage        string // empty: null
	MemberID            string // empty: null
	MemberEpoch         int32
	HeartbeatIntervalMs int32
	// Assignment is nil if the member is to keep what it has, else the
	// partitions it may use now.
	Assignment []TopicPartitionIDs
	// HasAssignment distinguishes an empty assignment from none.
	HasAssignment bool
}

// Decode implements Message.
func (m *ConsumerGroupHeartbeatResponse) Decode(r *Reader, _ int16) {
	r.Int32() // ThrottleTimeMs
	m.ErrorCode = r.Int16()
	m.ErrorMessage, _ = r.NullableString(true)
	m.MemberID, _ = r.NullableString(true)
	m.MemberEpoch = r.Int32()
	m.HeartbeatIntervalMs = r.Int32()
	if r.Int8() >= 0 { // Assignment: present
		m.HasAssignment = true
		m.Assignment = Array(r, true, func() TopicPartitionIDs { return readTopicPartitionIDs(r, true) })
		r.Tags(true)
	}
	r.Tags(true)
}

// Encode implements Message.
func (m *ConsumerGroupHeartbeatResponse) Encode(w *Writer, _ int16) {
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

// --- ConsumerGroupDescribe (69) --------------------------------------------

// ConsumerGroupDescribeRequest is the body of a ConsumerGroupDescribe
// request (key 69).
type ConsumerGroupDescribeRequest struct {
	Groups                      []string
	IncludeAuthorizedOperations bool
}

// Decode implements Message.
func (m *ConsumerGroupDescribeRequest) Decode(r *Reader, _ int16) {
	m.Groups = readStrings(r, true)
	m.IncludeAuthorizedOperations = r.Bool()
	r.Tags(true)
}

// Encode implements Message.
func (m *ConsumerGroupDescribeRequest) Encode(w *Writer, _ int16) {
	writeStrings(w, true, m.Groups)
	w.Bool(m.IncludeAuthorizedOperations)
	w.Tags(true)
}

// NamedTopicPartitions is a topic with its id and name, and some of its
// partitions.
type NamedTopicPartitions struct {
	TopicID    UUID
	TopicName  string
	Partitions []int32
}

func readNamed(r *Reader) []NamedTopicPartitions {
	// Assignment is a struct holding the array of TopicPartitions.
	ts := Array(r, true, func() NamedTopicPartitions {
		t := NamedTopicPartitions{TopicID: r.uuid(), TopicName: r.String(true), Partitions: readInt32s(r, true)}
		r.Tags(true)
		return t
	})
	r.Tags(true)
	return ts
}

func writeNamed(w *Writer, ts []NamedTopicPartitions) {
	w.ArrayLen(true, len(ts))
	for _, t := range ts {
		w.uuid(t.TopicID)
		w.String(true, t.TopicName)
		writeInt32s(w, true, t.Partitions)
		w.Tags(true)
	}
	w.Tags(true)
}

// ConsumerGroupDescribeMember is one member of a described group.
type ConsumerGroupDescribeMember struct {
	MemberID         string
	InstanceID       string // empty: null
	RackID           string // empty: null
	MemberEpoch      int32
	ClientID         string
	ClientHost       string
	SubscribedTopics []string
	SubscribedRegex  string // empty: null
	Assignment       []NamedTopicPartitions
	Target           []NamedTopicPartitions
	MemberType       int8 // v1+
}

// ConsumerGroupDescribeGroup is one group of a ConsumerGroupDescribe
// response.
type ConsumerGroupDescribeGroup struct {
	ErrorCode       int16
	ErrorMessage    string // empty: null
	GroupID         string
	State           string
	GroupEpoch      int32
	AssignmentEpoch int32
	AssignorName    string
	Members         []ConsumerGroupDescribeMember
}

// ConsumerGroupDescribeResponse is the body of a ConsumerGroupDescribe
// response.
type ConsumerGroupDescribeResponse struct{ Groups []ConsumerGroupDescribeGroup }

// Decode implements Message.
func (m *ConsumerGroupDescribeResponse) Decode(r *Reader, v int16) {
	r.Int32() // ThrottleTimeMs
	m.Groups = Array(r, true, func() ConsumerGroupDescribeGroup {
		var g ConsumerGroupDescribeGroup
		g.ErrorCode = r.Int16()
		g.ErrorMessage, _ = r.NullableString(true)
		g.GroupID = r.String(true)
		g.State = r.String(true)
		g.GroupEpoch = r.Int32()
		g.AssignmentEpoch = r.Int32()
		g.AssignorName = r.String(true)
		g.Members = Array(r, true, func() ConsumerGroupDescribeMember {
			var mem ConsumerGroupDescribeMember
			mem.MemberID = r.String(true)
			mem.InstanceID, _ = r.NullableString(true)
			mem.RackID, _ = r.NullableString(true)
			mem.MemberEpoch = r.Int32()
			mem.ClientID = r.String(true)
			mem.ClientHost = r.String(true)
			mem.SubscribedTopics = readStrings(r, true)
			mem.SubscribedRegex, _ = r.NullableString(true)
			mem.Assignment = readNamed(r)
			mem.Target = readNamed(r)
			if v >= 1 {
				mem.MemberType = r.Int8()
			}
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
func (m *ConsumerGroupDescribeResponse) Encode(w *Writer, v int16) {
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
			w.optString(true, mem.InstanceID)
			w.optString(true, mem.RackID)
			w.Int32(mem.MemberEpoch)
			w.String(true, mem.ClientID)
			w.String(true, mem.ClientHost)
			writeStrings(w, true, mem.SubscribedTopics)
			w.optString(true, mem.SubscribedRegex)
			writeNamed(w, mem.Assignment)
			writeNamed(w, mem.Target)
			if v >= 1 {
				w.Int8(mem.MemberType)
			}
			w.Tags(true)
		}
		w.Int32(authorizedOperationsOmitted)
		w.Tags(true)
	}
	w.Tags(true)
}

// --- DescribeTopicPartitions (75) ------------------------------------------

// TopicCursor is where a paged DescribeTopicPartitions goes on.
type TopicCursor struct {
	TopicName      string
	PartitionIndex int32
}

func readCursor(r *Reader) *TopicCursor {
	if r.Int8() < 0 {
		return nil
	}
	c := &TopicCursor{TopicName: r.String(true), PartitionIndex: r.Int32()}
	r.Tags(true)
	return c
}

func writeCursor(w *Writer, c *TopicCursor) {
	if c == nil {
		w.Int8(-1)
		return
	}
	w.Int8(1)
	w.String(true, c.TopicName)
	w.Int32(c.PartitionIndex)
	w.Tags(true)
}

// DescribeTopicPartitionsRequest is the body of a DescribeTopicPartitions
// request (key 75).
type DescribeTopicPartitionsRequest struct {
	Topics                 []string
	ResponsePartitionLimit int32
	Cursor                 *TopicCursor
}

// Decode implements Message.
func (m *DescribeTopicPartitionsRequest) Decode(r *Reader, _ int16) {
	m.Topics = Array(r, true, func() string {
		name := r.String(true)
		r.Tags(true)
		return name
	})
	m.ResponsePartitionLimit = r.Int32()
	m.Cursor = readCursor(r)
	r.Tags(true)
}

// Encode implements Message.
func (m *DescribeTopicPartitionsRequest) Encode(w *Writer, _ int16) {
	w.ArrayLen(true, len(m.Topics))
	for _, name := range m.Topics {
		w.String(true, name)
		w.Tags(true)
	}
	w.Int32(m.ResponsePartitionLimit)
	writeCursor(w, m.Cursor)
	w.Tags(true)
}

// DescribedPartition is one partition of a DescribeTopicPartitions answer.
// Samsa has no eligible leader replicas and no offline replicas, so those
// are never sent.
type DescribedPartition struct {
	ErrorCode   int16
	Index       int32
	Leader      int32
	LeaderEpoch int32
	Replicas    []int32
	ISR         []int32
}

// DescribedTopic is one topic of a DescribeTopicPartitions answer.
type DescribedTopic struct {
	ErrorCode  int16
	Name       string // empty: null
	TopicID    UUID
	IsInternal bool
	Partitions []DescribedPartition
}

// DescribeTopicPartitionsResponse is the body of a
// DescribeTopicPartitions response.
type DescribeTopicPartitionsResponse struct {
	Topics     []DescribedTopic
	NextCursor *TopicCursor
}

// Decode implements Message.
func (m *DescribeTopicPartitionsResponse) Decode(r *Reader, _ int16) {
	r.Int32() // ThrottleTimeMs
	m.Topics = Array(r, true, func() DescribedTopic {
		var t DescribedTopic
		t.ErrorCode = r.Int16()
		t.Name, _ = r.NullableString(true)
		t.TopicID = r.uuid()
		t.IsInternal = r.Bool()
		t.Partitions = Array(r, true, func() DescribedPartition {
			p := DescribedPartition{ErrorCode: r.Int16(), Index: r.Int32(), Leader: r.Int32(), LeaderEpoch: r.Int32()}
			p.Replicas = readInt32s(r, true)
			p.ISR = readInt32s(r, true)
			readNullableArray(r, true, r.Int32) // EligibleLeaderReplicas
			readNullableArray(r, true, r.Int32) // LastKnownElr
			readInt32s(r, true)                 // OfflineReplicas
			r.Tags(true)
			return p
		})
		r.Int32() // TopicAuthorizedOperations
		r.Tags(true)
		return t
	})
	m.NextCursor = readCursor(r)
	r.Tags(true)
}

// Encode implements Message.
func (m *DescribeTopicPartitionsResponse) Encode(w *Writer, _ int16) {
	w.Int32(0)
	w.ArrayLen(true, len(m.Topics))
	for _, t := range m.Topics {
		w.Int16(t.ErrorCode)
		w.optString(true, t.Name)
		w.uuid(t.TopicID)
		w.Bool(t.IsInternal)
		w.ArrayLen(true, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int16(p.ErrorCode)
			w.Int32(p.Index)
			w.Int32(p.Leader)
			w.Int32(p.LeaderEpoch)
			writeInt32s(w, true, p.Replicas)
			writeInt32s(w, true, p.ISR)
			w.ArrayLen(true, 0) // EligibleLeaderReplicas: Kafka 4.4 calls .stream() without a null check
			w.ArrayLen(true, 0) // LastKnownElr
			w.ArrayLen(true, 0) // OfflineReplicas
			w.Tags(true)
		}
		w.Int32(authorizedOperationsOmitted)
		w.Tags(true)
	}
	writeCursor(w, m.NextCursor)
	w.Tags(true)
}
