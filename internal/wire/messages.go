package wire

// The structs in this file hold only the fields the broker uses. Fields it
// ignores are skipped when decoding and written with their documented
// defaults when encoding. Version conditions are copied from the message
// definitions; a comment such as "v5+" names the first version with a field.

// APIVersionsRequest is the body of an ApiVersions request (key 18).
type APIVersionsRequest struct {
	ClientSoftwareName, ClientSoftwareVersion string // v3+
}

// Decode implements Message.
func (m *APIVersionsRequest) Decode(r *Reader, v int16) {
	if v >= 3 {
		m.ClientSoftwareName = r.String(true)
		m.ClientSoftwareVersion = r.String(true)
		r.Tags(true)
	}
}

// Encode implements Message.
func (m *APIVersionsRequest) Encode(w *Writer, v int16) {
	if v >= 3 {
		w.String(true, m.ClientSoftwareName)
		w.String(true, m.ClientSoftwareVersion)
		w.Tags(true)
	}
}

// APIRange is one entry of an ApiVersions response.
type APIRange struct{ Key, Min, Max int16 }

// APIVersionsResponse is the body of an ApiVersions response.
type APIVersionsResponse struct {
	ErrorCode int16
	APIs      []APIRange
}

// Decode implements Message.
func (m *APIVersionsResponse) Decode(r *Reader, v int16) {
	flex := v >= 3
	m.ErrorCode = r.Int16()
	m.APIs = Array(r, flex, func() APIRange {
		a := APIRange{r.Int16(), r.Int16(), r.Int16()}
		r.Tags(flex)
		return a
	})
	if v >= 1 {
		r.Int32() // ThrottleTimeMs
	}
	r.Tags(flex)
}

// Encode implements Message.
func (m *APIVersionsResponse) Encode(w *Writer, v int16) {
	flex := v >= 3
	w.Int16(m.ErrorCode)
	w.ArrayLen(flex, len(m.APIs))
	for _, a := range m.APIs {
		w.Int16(a.Key)
		w.Int16(a.Min)
		w.Int16(a.Max)
		w.Tags(flex)
	}
	if v >= 1 {
		w.Int32(0) // ThrottleTimeMs
	}
	w.Tags(flex)
}

// MetadataRequest is the body of a Metadata request (key 3).
type MetadataRequest struct {
	// AllTopics is set when the topic array is null, which from version 1
	// means "every topic". An empty array means "no topics".
	AllTopics bool
	Topics    []string
}

// Decode implements Message.
func (m *MetadataRequest) Decode(r *Reader, v int16) {
	n := r.ArrayLen(false)
	m.AllTopics = n < 0 && r.Err() == nil
	m.Topics = nil
	for ; n > 0 && r.Err() == nil; n-- {
		m.Topics = append(m.Topics, r.String(false))
	}
	if v >= 4 {
		r.Bool() // AllowAutoTopicCreation: topics are never auto-created
	}
	if v >= 8 {
		r.Bool() // IncludeClusterAuthorizedOperations
		r.Bool() // IncludeTopicAuthorizedOperations
	}
}

// Encode implements Message.
func (m *MetadataRequest) Encode(w *Writer, v int16) {
	if m.AllTopics {
		w.ArrayLen(false, -1)
	} else {
		w.ArrayLen(false, len(m.Topics))
		for _, t := range m.Topics {
			w.String(false, t)
		}
	}
	if v >= 4 {
		w.Bool(false)
	}
	if v >= 8 {
		w.Bool(false)
		w.Bool(false)
	}
}

// Broker is one broker in a Metadata response.
type Broker struct {
	NodeID int32
	Host   string
	Port   int32
}

// PartitionMetadata is one partition in a Metadata response.
type PartitionMetadata struct {
	ErrorCode     int16
	Index, Leader int32
	Replicas, ISR []int32
}

// TopicMetadata is one topic in a Metadata response.
type TopicMetadata struct {
	ErrorCode  int16
	Name       string
	Partitions []PartitionMetadata
}

// MetadataResponse is the body of a Metadata response.
type MetadataResponse struct {
	Brokers      []Broker
	ClusterID    string // v2+
	ControllerID int32
	Topics       []TopicMetadata
}

// noAuthorizedOperations is the documented default of the
// *AuthorizedOperations fields, meaning "not requested".
const noAuthorizedOperations = -2147483648

func readInt32s(r *Reader) []int32 { return Array(r, false, r.Int32) }

func writeInt32s(w *Writer, vs []int32) {
	w.ArrayLen(false, len(vs))
	for _, v := range vs {
		w.Int32(v)
	}
}

// Decode implements Message.
func (m *MetadataResponse) Decode(r *Reader, v int16) {
	if v >= 3 {
		r.Int32() // ThrottleTimeMs
	}
	m.Brokers = Array(r, false, func() Broker {
		b := Broker{NodeID: r.Int32(), Host: r.String(false), Port: r.Int32()}
		r.NullableString(false) // Rack, v1+
		return b
	})
	if v >= 2 {
		m.ClusterID, _ = r.NullableString(false)
	}
	m.ControllerID = r.Int32()
	m.Topics = Array(r, false, func() TopicMetadata {
		t := TopicMetadata{ErrorCode: r.Int16(), Name: r.String(false)}
		r.Bool() // IsInternal, v1+
		t.Partitions = Array(r, false, func() PartitionMetadata {
			p := PartitionMetadata{ErrorCode: r.Int16(), Index: r.Int32(), Leader: r.Int32()}
			if v >= 7 {
				r.Int32() // LeaderEpoch
			}
			p.Replicas, p.ISR = readInt32s(r), readInt32s(r)
			if v >= 5 {
				readInt32s(r) // OfflineReplicas
			}
			return p
		})
		if v >= 8 {
			r.Int32() // TopicAuthorizedOperations
		}
		return t
	})
	if v >= 8 {
		r.Int32() // ClusterAuthorizedOperations, v8-10
	}
}

// Encode implements Message.
func (m *MetadataResponse) Encode(w *Writer, v int16) {
	if v >= 3 {
		w.Int32(0)
	}
	w.ArrayLen(false, len(m.Brokers))
	for _, b := range m.Brokers {
		w.Int32(b.NodeID)
		w.String(false, b.Host)
		w.Int32(b.Port)
		w.NullString(false) // Rack
	}
	if v >= 2 {
		w.String(false, m.ClusterID)
	}
	w.Int32(m.ControllerID)
	w.ArrayLen(false, len(m.Topics))
	for _, t := range m.Topics {
		w.Int16(t.ErrorCode)
		w.String(false, t.Name)
		w.Bool(false) // IsInternal
		w.ArrayLen(false, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int16(p.ErrorCode)
			w.Int32(p.Index)
			w.Int32(p.Leader)
			if v >= 7 {
				w.Int32(0) // LeaderEpoch: constant until there is replication
			}
			writeInt32s(w, p.Replicas)
			writeInt32s(w, p.ISR)
			if v >= 5 {
				w.ArrayLen(false, 0) // OfflineReplicas
			}
		}
		if v >= 8 {
			w.Int32(noAuthorizedOperations)
		}
	}
	if v >= 8 {
		w.Int32(noAuthorizedOperations)
	}
}

// CreateTopic is one topic of a CreateTopics request.
type CreateTopic struct {
	Name              string
	NumPartitions     int32 // -1 asks for the broker default (v4+)
	ReplicationFactor int16 // -1 asks for the broker default (v4+)
	// HasAssignments is set if the request carried a manual replica
	// assignment, which the broker does not support.
	HasAssignments bool
}

// CreateTopicsRequest is the body of a CreateTopics request (key 19).
type CreateTopicsRequest struct {
	Topics       []CreateTopic
	TimeoutMs    int32
	ValidateOnly bool
}

// Decode implements Message.
func (m *CreateTopicsRequest) Decode(r *Reader, _ int16) {
	m.Topics = Array(r, false, func() CreateTopic {
		t := CreateTopic{Name: r.String(false), NumPartitions: r.Int32(), ReplicationFactor: r.Int16()}
		assignments := Array(r, false, func() struct{} {
			r.Int32()     // PartitionIndex
			readInt32s(r) // BrokerIds
			return struct{}{}
		})
		t.HasAssignments = len(assignments) > 0
		Array(r, false, func() struct{} { // Configs: accepted and ignored
			r.String(false)
			r.NullableString(false)
			return struct{}{}
		})
		return t
	})
	m.TimeoutMs = r.Int32()
	m.ValidateOnly = r.Bool() // v1+
}

// Encode implements Message. It never writes configs; HasAssignments is
// written as one assignment of partition 0 to no brokers, so that it
// survives a round trip.
func (m *CreateTopicsRequest) Encode(w *Writer, _ int16) {
	w.ArrayLen(false, len(m.Topics))
	for _, t := range m.Topics {
		w.String(false, t.Name)
		w.Int32(t.NumPartitions)
		w.Int16(t.ReplicationFactor)
		if t.HasAssignments {
			w.ArrayLen(false, 1)
			w.Int32(0)           // PartitionIndex
			w.ArrayLen(false, 0) // BrokerIds
		} else {
			w.ArrayLen(false, 0)
		}
		w.ArrayLen(false, 0)
	}
	w.Int32(m.TimeoutMs)
	w.Bool(m.ValidateOnly)
}

// CreateTopicResult is one topic of a CreateTopics response.
type CreateTopicResult struct {
	Name      string
	ErrorCode int16
	// ErrorMessage is sent as null when empty.
	ErrorMessage string
}

// CreateTopicsResponse is the body of a CreateTopics response.
type CreateTopicsResponse struct{ Topics []CreateTopicResult }

// Decode implements Message.
func (m *CreateTopicsResponse) Decode(r *Reader, _ int16) {
	r.Int32() // ThrottleTimeMs, v2+
	m.Topics = Array(r, false, func() CreateTopicResult {
		t := CreateTopicResult{Name: r.String(false), ErrorCode: r.Int16()}
		t.ErrorMessage, _ = r.NullableString(false) // v1+
		return t
	})
}

// Encode implements Message.
func (m *CreateTopicsResponse) Encode(w *Writer, _ int16) {
	w.Int32(0)
	w.ArrayLen(false, len(m.Topics))
	for _, t := range m.Topics {
		w.String(false, t.Name)
		w.Int16(t.ErrorCode)
		if t.ErrorMessage == "" {
			w.NullString(false)
		} else {
			w.String(false, t.ErrorMessage)
		}
	}
}

// ProducePartition is one partition's records in a Produce request.
type ProducePartition struct {
	Index   int32
	Records []byte // one or more record batches; aliases the request
}

// ProduceTopic is one topic of a Produce request.
type ProduceTopic struct {
	Name       string
	Partitions []ProducePartition
}

// ProduceRequest is the body of a Produce request (key 0).
type ProduceRequest struct {
	Acks      int16
	TimeoutMs int32
	Topics    []ProduceTopic
}

// Decode implements Message.
func (m *ProduceRequest) Decode(r *Reader, _ int16) {
	r.NullableString(false) // TransactionalId, v3+
	m.Acks = r.Int16()
	m.TimeoutMs = r.Int32()
	m.Topics = Array(r, false, func() ProduceTopic {
		return ProduceTopic{Name: r.String(false), Partitions: Array(r, false, func() ProducePartition {
			return ProducePartition{Index: r.Int32(), Records: r.Bytes(false)}
		})}
	})
}

// Encode implements Message.
func (m *ProduceRequest) Encode(w *Writer, _ int16) {
	w.NullString(false)
	w.Int16(m.Acks)
	w.Int32(m.TimeoutMs)
	w.ArrayLen(false, len(m.Topics))
	for _, t := range m.Topics {
		w.String(false, t.Name)
		w.ArrayLen(false, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Bytes(false, p.Records)
		}
	}
}

// ProducePartitionResponse is one partition's result in a Produce response.
type ProducePartitionResponse struct {
	Index      int32
	ErrorCode  int16
	BaseOffset int64
}

// ProduceTopicResponse is one topic of a Produce response.
type ProduceTopicResponse struct {
	Name       string
	Partitions []ProducePartitionResponse
}

// ProduceResponse is the body of a Produce response.
type ProduceResponse struct{ Topics []ProduceTopicResponse }

// Decode implements Message.
func (m *ProduceResponse) Decode(r *Reader, v int16) {
	m.Topics = Array(r, false, func() ProduceTopicResponse {
		return ProduceTopicResponse{Name: r.String(false), Partitions: Array(r, false, func() ProducePartitionResponse {
			p := ProducePartitionResponse{Index: r.Int32(), ErrorCode: r.Int16(), BaseOffset: r.Int64()}
			r.Int64() // LogAppendTimeMs, v2+
			if v >= 5 {
				r.Int64() // LogStartOffset
			}
			if v >= 8 {
				Array(r, false, func() struct{} { // RecordErrors
					r.Int32()
					r.NullableString(false)
					return struct{}{}
				})
				r.NullableString(false) // ErrorMessage
			}
			return p
		})}
	})
	r.Int32() // ThrottleTimeMs, v1+
}

// Encode implements Message.
func (m *ProduceResponse) Encode(w *Writer, v int16) {
	w.ArrayLen(false, len(m.Topics))
	for _, t := range m.Topics {
		w.String(false, t.Name)
		w.ArrayLen(false, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Int16(p.ErrorCode)
			w.Int64(p.BaseOffset)
			w.Int64(-1) // LogAppendTimeMs: record timestamps are the producer's
			if v >= 5 {
				w.Int64(0) // LogStartOffset: nothing is ever deleted
			}
			if v >= 8 {
				w.ArrayLen(false, 0)
				w.NullString(false)
			}
		}
	}
	w.Int32(0)
}

// FetchPartition is one partition of a Fetch request.
type FetchPartition struct {
	Index    int32
	Offset   int64
	MaxBytes int32
}

// FetchTopic is one topic of a Fetch request.
type FetchTopic struct {
	Name       string
	Partitions []FetchPartition
}

// FetchRequest is the body of a Fetch request (key 1).
type FetchRequest struct {
	MaxWaitMs, MinBytes, MaxBytes int32
	Topics                        []FetchTopic
}

// Decode implements Message.
func (m *FetchRequest) Decode(r *Reader, v int16) {
	r.Int32() // ReplicaId
	m.MaxWaitMs = r.Int32()
	m.MinBytes = r.Int32()
	m.MaxBytes = r.Int32() // v3+
	r.Int8()               // IsolationLevel, v4+
	if v >= 7 {
		r.Int32() // SessionId: sessions are declined
		r.Int32() // SessionEpoch
	}
	m.Topics = Array(r, false, func() FetchTopic {
		return FetchTopic{Name: r.String(false), Partitions: Array(r, false, func() FetchPartition {
			p := FetchPartition{Index: r.Int32()}
			if v >= 9 {
				r.Int32() // CurrentLeaderEpoch
			}
			p.Offset = r.Int64()
			if v >= 5 {
				r.Int64() // LogStartOffset
			}
			p.MaxBytes = r.Int32()
			return p
		})}
	})
	if v >= 7 {
		Array(r, false, func() struct{} { // ForgottenTopicsData
			r.String(false)
			readInt32s(r)
			return struct{}{}
		})
	}
	if v >= 11 {
		r.String(false) // RackId
	}
}

// Encode implements Message.
func (m *FetchRequest) Encode(w *Writer, v int16) {
	w.Int32(-1)
	w.Int32(m.MaxWaitMs)
	w.Int32(m.MinBytes)
	w.Int32(m.MaxBytes)
	w.Int8(0)
	if v >= 7 {
		w.Int32(0)
		w.Int32(-1)
	}
	w.ArrayLen(false, len(m.Topics))
	for _, t := range m.Topics {
		w.String(false, t.Name)
		w.ArrayLen(false, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			if v >= 9 {
				w.Int32(-1)
			}
			w.Int64(p.Offset)
			if v >= 5 {
				w.Int64(-1)
			}
			w.Int32(p.MaxBytes)
		}
	}
	if v >= 7 {
		w.ArrayLen(false, 0)
	}
	if v >= 11 {
		w.String(false, "")
	}
}

// FetchPartitionResponse is one partition's result in a Fetch response.
type FetchPartitionResponse struct {
	Index         int32
	ErrorCode     int16
	HighWatermark int64
	Records       []byte // whole record batches, or nil
}

// FetchTopicResponse is one topic of a Fetch response.
type FetchTopicResponse struct {
	Name       string
	Partitions []FetchPartitionResponse
}

// FetchResponse is the body of a Fetch response.
type FetchResponse struct{ Topics []FetchTopicResponse }

// Decode implements Message.
func (m *FetchResponse) Decode(r *Reader, v int16) {
	r.Int32() // ThrottleTimeMs, v1+
	if v >= 7 {
		r.Int16() // ErrorCode
		r.Int32() // SessionId
	}
	m.Topics = Array(r, false, func() FetchTopicResponse {
		return FetchTopicResponse{Name: r.String(false), Partitions: Array(r, false, func() FetchPartitionResponse {
			p := FetchPartitionResponse{Index: r.Int32(), ErrorCode: r.Int16(), HighWatermark: r.Int64()}
			r.Int64() // LastStableOffset, v4+
			if v >= 5 {
				r.Int64() // LogStartOffset
			}
			Array(r, false, func() struct{} { // AbortedTransactions, v4+
				r.Int64()
				r.Int64()
				return struct{}{}
			})
			if v >= 11 {
				r.Int32() // PreferredReadReplica
			}
			if p.Records = r.Bytes(false); len(p.Records) == 0 {
				p.Records = nil
			}
			return p
		})}
	})
}

// Encode implements Message.
func (m *FetchResponse) Encode(w *Writer, v int16) {
	w.Int32(0)
	if v >= 7 {
		w.Int16(ErrNone)
		w.Int32(0) // SessionId 0: no session, keep sending full requests
	}
	w.ArrayLen(false, len(m.Topics))
	for _, t := range m.Topics {
		w.String(false, t.Name)
		w.ArrayLen(false, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Int16(p.ErrorCode)
			w.Int64(p.HighWatermark)
			w.Int64(p.HighWatermark) // LastStableOffset: no transactions
			if v >= 5 {
				w.Int64(0) // LogStartOffset
			}
			w.ArrayLen(false, -1) // AbortedTransactions: null
			if v >= 11 {
				w.Int32(-1)
			}
			// "No records" is sent as an empty set, never as null. The
			// definition allows null, and the Java client and franz-go
			// accept it, but librdkafka rejects it ("invalid
			// MessageSetSize -1"). Found by the kcat integration test.
			w.Int32(int32(len(p.Records)))
			w.B = append(w.B, p.Records...)
		}
	}
}

// Special timestamps in a ListOffsets request.
const (
	TimestampLatest   int64 = -1
	TimestampEarliest int64 = -2
)

// ListOffsetsPartition is one partition of a ListOffsets request.
type ListOffsetsPartition struct {
	Index     int32
	Timestamp int64
}

// ListOffsetsTopic is one topic of a ListOffsets request.
type ListOffsetsTopic struct {
	Name       string
	Partitions []ListOffsetsPartition
}

// ListOffsetsRequest is the body of a ListOffsets request (key 2).
type ListOffsetsRequest struct{ Topics []ListOffsetsTopic }

// Decode implements Message.
func (m *ListOffsetsRequest) Decode(r *Reader, v int16) {
	r.Int32() // ReplicaId
	if v >= 2 {
		r.Int8() // IsolationLevel
	}
	m.Topics = Array(r, false, func() ListOffsetsTopic {
		return ListOffsetsTopic{Name: r.String(false), Partitions: Array(r, false, func() ListOffsetsPartition {
			p := ListOffsetsPartition{Index: r.Int32()}
			if v >= 4 {
				r.Int32() // CurrentLeaderEpoch
			}
			p.Timestamp = r.Int64()
			return p
		})}
	})
}

// Encode implements Message.
func (m *ListOffsetsRequest) Encode(w *Writer, v int16) {
	w.Int32(-1)
	if v >= 2 {
		w.Int8(0)
	}
	w.ArrayLen(false, len(m.Topics))
	for _, t := range m.Topics {
		w.String(false, t.Name)
		w.ArrayLen(false, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			if v >= 4 {
				w.Int32(-1)
			}
			w.Int64(p.Timestamp)
		}
	}
}

// ListOffsetsPartitionResponse is one partition's result in a ListOffsets
// response.
type ListOffsetsPartitionResponse struct {
	Index     int32
	ErrorCode int16
	Timestamp int64
	Offset    int64
}

// ListOffsetsTopicResponse is one topic of a ListOffsets response.
type ListOffsetsTopicResponse struct {
	Name       string
	Partitions []ListOffsetsPartitionResponse
}

// ListOffsetsResponse is the body of a ListOffsets response.
type ListOffsetsResponse struct{ Topics []ListOffsetsTopicResponse }

// Decode implements Message.
func (m *ListOffsetsResponse) Decode(r *Reader, v int16) {
	if v >= 2 {
		r.Int32() // ThrottleTimeMs
	}
	m.Topics = Array(r, false, func() ListOffsetsTopicResponse {
		return ListOffsetsTopicResponse{Name: r.String(false), Partitions: Array(r, false, func() ListOffsetsPartitionResponse {
			p := ListOffsetsPartitionResponse{Index: r.Int32(), ErrorCode: r.Int16(), Timestamp: r.Int64(), Offset: r.Int64()}
			if v >= 4 {
				r.Int32() // LeaderEpoch
			}
			return p
		})}
	})
}

// Encode implements Message.
func (m *ListOffsetsResponse) Encode(w *Writer, v int16) {
	if v >= 2 {
		w.Int32(0)
	}
	w.ArrayLen(false, len(m.Topics))
	for _, t := range m.Topics {
		w.String(false, t.Name)
		w.ArrayLen(false, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Int16(p.ErrorCode)
			w.Int64(p.Timestamp)
			w.Int64(p.Offset)
			if v >= 4 {
				w.Int32(0) // LeaderEpoch
			}
		}
	}
}
