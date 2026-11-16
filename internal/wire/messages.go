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
		if v >= 5 {
			r.NullableString(true) // ClusterId, v5+ (KIP-1242)
			r.Int32()              // NodeId, v5+
		}
		r.Tags(true)
	}
}

// Encode implements Message.
func (m *APIVersionsRequest) Encode(w *Writer, v int16) {
	if v >= 3 {
		w.String(true, m.ClientSoftwareName)
		w.String(true, m.ClientSoftwareVersion)
		if v >= 5 {
			w.optString(true, "")
			w.Int32(-1)
		}
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
	// TopicIDs, when non-empty, is parallel to Topics (v10+). An entry
	// with a non-zero UUID queries by topic ID; Topics[i] may then be "".
	TopicIDs []UUID
}

// Decode implements Message.
func (m *MetadataRequest) Decode(r *Reader, v int16) {
	flex := v >= 9
	n := r.ArrayLen(flex)
	m.AllTopics = n < 0 && r.Err() == nil
	m.Topics = nil
	m.TopicIDs = nil
	var hasIDs bool
	for ; n > 0 && r.Err() == nil; n-- {
		var id UUID
		if v >= 10 {
			id = r.uuid()
			if id != (UUID{}) {
				hasIDs = true
			}
		}
		var name string
		if v >= 10 {
			name, _ = r.NullableString(flex)
		} else {
			name = r.String(flex)
		}
		r.Tags(flex)
		m.Topics = append(m.Topics, name)
		if v >= 10 {
			m.TopicIDs = append(m.TopicIDs, id)
		}
	}
	if !hasIDs {
		m.TopicIDs = nil
	}
	if v >= 4 {
		r.Bool() // AllowAutoTopicCreation: topics are never auto-created
	}
	if v >= 8 && v <= 10 {
		r.Bool() // IncludeClusterAuthorizedOperations, v8-10
	}
	if v >= 8 {
		r.Bool() // IncludeTopicAuthorizedOperations
	}
	r.Tags(flex)
}

// Encode implements Message.
func (m *MetadataRequest) Encode(w *Writer, v int16) {
	flex := v >= 9
	if m.AllTopics {
		w.ArrayLen(flex, -1)
	} else {
		n := max(len(m.Topics), len(m.TopicIDs))
		w.ArrayLen(flex, n)
		for i := range n {
			var name string
			if i < len(m.Topics) {
				name = m.Topics[i]
			}
			var id UUID
			if i < len(m.TopicIDs) {
				id = m.TopicIDs[i]
			}
			if v >= 10 {
				w.uuid(id)
				w.optString(flex, name)
			} else {
				w.String(flex, name)
			}
			w.Tags(flex)
		}
	}
	if v >= 4 {
		w.Bool(false)
	}
	if v >= 8 && v <= 10 {
		w.Bool(false)
	}
	if v >= 8 {
		w.Bool(false)
	}
	w.Tags(flex)
}

// Broker is one broker in a Metadata response.
type Broker struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   string // "" is null
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
	TopicID    UUID // v10+
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

func readInt32s(r *Reader, flex bool) []int32 { return Array(r, flex, r.Int32) }

func writeInt32s(w *Writer, flex bool, vs []int32) {
	w.ArrayLen(flex, len(vs))
	for _, v := range vs {
		w.Int32(v)
	}
}

// Decode implements Message.
func (m *MetadataResponse) Decode(r *Reader, v int16) {
	flex := v >= 9
	if v >= 3 {
		r.Int32() // ThrottleTimeMs
	}
	m.Brokers = Array(r, flex, func() Broker {
		b := Broker{NodeID: r.Int32(), Host: r.String(flex), Port: r.Int32()}
		b.Rack, _ = r.NullableString(flex) // v1+
		r.Tags(flex)
		return b
	})
	if v >= 2 {
		m.ClusterID, _ = r.NullableString(flex)
	}
	m.ControllerID = r.Int32()
	m.Topics = Array(r, flex, func() TopicMetadata {
		t := TopicMetadata{ErrorCode: r.Int16()}
		if v >= 12 {
			t.Name, _ = r.NullableString(flex)
		} else {
			t.Name = r.String(flex)
		}
		if v >= 10 {
			t.TopicID = r.uuid()
		}
		r.Bool() // IsInternal, v1+
		t.Partitions = Array(r, flex, func() PartitionMetadata {
			p := PartitionMetadata{ErrorCode: r.Int16(), Index: r.Int32(), Leader: r.Int32()}
			if v >= 7 {
				r.Int32() // LeaderEpoch
			}
			p.Replicas, p.ISR = readInt32s(r, flex), readInt32s(r, flex)
			if v >= 5 {
				readInt32s(r, flex) // OfflineReplicas
			}
			r.Tags(flex)
			return p
		})
		if v >= 8 {
			r.Int32() // TopicAuthorizedOperations
		}
		r.Tags(flex)
		return t
	})
	if v >= 8 && v <= 10 {
		r.Int32() // ClusterAuthorizedOperations, v8-10
	}
	r.Tags(flex)
}

// Encode implements Message.
func (m *MetadataResponse) Encode(w *Writer, v int16) {
	flex := v >= 9
	if v >= 3 {
		w.Int32(0)
	}
	w.ArrayLen(flex, len(m.Brokers))
	for _, b := range m.Brokers {
		w.Int32(b.NodeID)
		w.String(flex, b.Host)
		w.Int32(b.Port)
		w.optString(flex, b.Rack)
		w.Tags(flex)
	}
	if v >= 2 {
		w.String(flex, m.ClusterID)
	}
	w.Int32(m.ControllerID)
	w.ArrayLen(flex, len(m.Topics))
	for _, t := range m.Topics {
		w.Int16(t.ErrorCode)
		if v >= 12 {
			w.optString(flex, t.Name)
		} else {
			w.String(flex, t.Name)
		}
		if v >= 10 {
			w.uuid(t.TopicID)
		}
		w.Bool(false) // IsInternal
		w.ArrayLen(flex, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int16(p.ErrorCode)
			w.Int32(p.Index)
			w.Int32(p.Leader)
			if v >= 7 {
				w.Int32(0) // LeaderEpoch: constant until there is replication
			}
			writeInt32s(w, flex, p.Replicas)
			writeInt32s(w, flex, p.ISR)
			if v >= 5 {
				w.ArrayLen(flex, 0) // OfflineReplicas
			}
			w.Tags(flex)
		}
		if v >= 8 {
			w.Int32(noAuthorizedOperations)
		}
		w.Tags(flex)
	}
	if v >= 8 && v <= 10 {
		w.Int32(noAuthorizedOperations)
	}
	w.Tags(flex)
}

// CreateTopic is one topic of a CreateTopics request.
type CreateTopic struct {
	Name              string
	NumPartitions     int32 // -1 asks for the broker default (v4+)
	ReplicationFactor int16 // -1 asks for the broker default (v4+)
	// HasAssignments is set if the request carried a manual replica
	// assignment, which the broker does not support.
	HasAssignments bool
	Configs        []TopicConfig
}

// TopicConfig is one configuration of a topic being created: a name and a
// value, which may be null.
type TopicConfig struct {
	Name  string
	Value string
	Null  bool
}

// CreateTopicsRequest is the body of a CreateTopics request (key 19).
type CreateTopicsRequest struct {
	Topics       []CreateTopic
	TimeoutMs    int32
	ValidateOnly bool
}

// Decode implements Message.
func (m *CreateTopicsRequest) Decode(r *Reader, v int16) {
	flex := v >= 5
	m.Topics = Array(r, flex, func() CreateTopic {
		t := CreateTopic{Name: r.String(flex), NumPartitions: r.Int32(), ReplicationFactor: r.Int16()}
		assignments := Array(r, flex, func() struct{} {
			r.Int32()           // PartitionIndex
			readInt32s(r, flex) // BrokerIds
			r.Tags(flex)
			return struct{}{}
		})
		t.HasAssignments = len(assignments) > 0
		t.Configs = Array(r, flex, func() TopicConfig {
			c := TopicConfig{Name: r.String(flex)}
			c.Value, c.Null = r.NullableString(flex)
			r.Tags(flex)
			return c
		})
		r.Tags(flex)
		return t
	})
	m.TimeoutMs = r.Int32()
	m.ValidateOnly = r.Bool() // v1+
	r.Tags(flex)
}

// Encode implements Message. HasAssignments is
// written as one assignment of partition 0 to no brokers, so that it
// survives a round trip.
func (m *CreateTopicsRequest) Encode(w *Writer, v int16) {
	flex := v >= 5
	w.ArrayLen(flex, len(m.Topics))
	for _, t := range m.Topics {
		w.String(flex, t.Name)
		w.Int32(t.NumPartitions)
		w.Int16(t.ReplicationFactor)
		if t.HasAssignments {
			w.ArrayLen(flex, 1)
			w.Int32(0)          // PartitionIndex
			w.ArrayLen(flex, 0) // BrokerIds
			w.Tags(flex)
		} else {
			w.ArrayLen(flex, 0)
		}
		w.ArrayLen(flex, len(t.Configs))
		for _, c := range t.Configs {
			w.String(flex, c.Name)
			if c.Null {
				w.NullString(flex)
			} else {
				w.String(flex, c.Value)
			}
			w.Tags(flex)
		}
		w.Tags(flex)
	}
	w.Int32(m.TimeoutMs)
	w.Bool(m.ValidateOnly)
	w.Tags(flex)
}

// CreateTopicConfig is one configuration entry of a CreateTopics v5+ response.
type CreateTopicConfig struct {
	Name        string
	Value       string
	Null        bool
	ReadOnly    bool
	Source      int8
	IsSensitive bool
}

// CreateTopicResult is one topic of a CreateTopics response.
type CreateTopicResult struct {
	Name      string
	TopicID   UUID // v7+
	ErrorCode int16
	// ErrorMessage is sent as null when empty.
	ErrorMessage      string
	NumPartitions     int32               // v5+; -1 when unset/error
	ReplicationFactor int16               // v5+; -1 when unset/error
	Configs           []CreateTopicConfig // v5+; nil is sent as null
}

// CreateTopicsResponse is the body of a CreateTopics response.
type CreateTopicsResponse struct{ Topics []CreateTopicResult }

// Decode implements Message.
func (m *CreateTopicsResponse) Decode(r *Reader, v int16) {
	flex := v >= 5
	r.Int32() // ThrottleTimeMs, v2+
	m.Topics = Array(r, flex, func() CreateTopicResult {
		t := CreateTopicResult{Name: r.String(flex)}
		if v >= 7 {
			t.TopicID = r.uuid()
		}
		t.ErrorCode = r.Int16()
		t.ErrorMessage, _ = r.NullableString(flex) // v1+
		if v >= 5 {
			t.NumPartitions = r.Int32()
			t.ReplicationFactor = r.Int16()
			t.Configs = Array(r, flex, func() CreateTopicConfig {
				c := CreateTopicConfig{Name: r.String(flex)}
				c.Value, c.Null = r.NullableString(flex)
				c.ReadOnly = r.Bool()
				c.Source = r.Int8()
				c.IsSensitive = r.Bool()
				r.Tags(flex)
				return c
			})
		}
		r.Tags(flex)
		return t
	})
	r.Tags(flex)
}

// Encode implements Message.
func (m *CreateTopicsResponse) Encode(w *Writer, v int16) {
	flex := v >= 5
	w.Int32(0)
	w.ArrayLen(flex, len(m.Topics))
	for _, t := range m.Topics {
		w.String(flex, t.Name)
		if v >= 7 {
			w.uuid(t.TopicID)
		}
		w.Int16(t.ErrorCode)
		w.optString(flex, t.ErrorMessage)
		if v >= 5 {
			w.Int32(t.NumPartitions)
			w.Int16(t.ReplicationFactor)
			if t.Configs == nil {
				w.ArrayLen(flex, -1)
			} else {
				w.ArrayLen(flex, len(t.Configs))
				for _, c := range t.Configs {
					w.String(flex, c.Name)
					if c.Null {
						w.NullString(flex)
					} else {
						w.String(flex, c.Value)
					}
					w.Bool(c.ReadOnly)
					w.Int8(c.Source)
					w.Bool(c.IsSensitive)
					w.Tags(flex)
				}
			}
		}
		w.Tags(flex)
	}
	w.Tags(flex)
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
	TransactionalID    string
	HasTransactionalID bool
	Acks               int16
	TimeoutMs          int32
	Topics             []ProduceTopic
}

// Decode implements Message.
func (m *ProduceRequest) Decode(r *Reader, v int16) {
	flex := v >= 9
	tid, null := r.NullableString(flex) // TransactionalId, v3+
	m.TransactionalID, m.HasTransactionalID = tid, !null
	m.Acks = r.Int16()
	m.TimeoutMs = r.Int32()
	m.Topics = Array(r, flex, func() ProduceTopic {
		t := ProduceTopic{Name: r.String(flex)}
		t.Partitions = Array(r, flex, func() ProducePartition {
			p := ProducePartition{Index: r.Int32(), Records: r.Bytes(flex)}
			r.Tags(flex)
			return p
		})
		r.Tags(flex)
		return t
	})
	r.Tags(flex)
}

// Encode implements Message.
func (m *ProduceRequest) Encode(w *Writer, v int16) {
	flex := v >= 9
	if m.HasTransactionalID || m.TransactionalID != "" {
		w.String(flex, m.TransactionalID)
	} else {
		w.NullString(flex)
	}
	w.Int16(m.Acks)
	w.Int32(m.TimeoutMs)
	w.ArrayLen(flex, len(m.Topics))
	for _, t := range m.Topics {
		w.String(flex, t.Name)
		w.ArrayLen(flex, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Bytes(flex, p.Records)
			w.Tags(flex)
		}
		w.Tags(flex)
	}
	w.Tags(flex)
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
	flex := v >= 9
	m.Topics = Array(r, flex, func() ProduceTopicResponse {
		t := ProduceTopicResponse{Name: r.String(flex)}
		t.Partitions = Array(r, flex, func() ProducePartitionResponse {
			p := ProducePartitionResponse{Index: r.Int32(), ErrorCode: r.Int16(), BaseOffset: r.Int64()}
			r.Int64() // LogAppendTimeMs, v2+
			if v >= 5 {
				r.Int64() // LogStartOffset
			}
			if v >= 8 {
				Array(r, flex, func() struct{} { // RecordErrors
					r.Int32()
					r.NullableString(flex)
					r.Tags(flex)
					return struct{}{}
				})
				r.NullableString(flex) // ErrorMessage
			}
			r.Tags(flex)
			return p
		})
		r.Tags(flex)
		return t
	})
	r.Int32() // ThrottleTimeMs, v1+
	r.Tags(flex)
}

// Encode implements Message.
func (m *ProduceResponse) Encode(w *Writer, v int16) {
	flex := v >= 9
	w.ArrayLen(flex, len(m.Topics))
	for _, t := range m.Topics {
		w.String(flex, t.Name)
		w.ArrayLen(flex, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Int16(p.ErrorCode)
			w.Int64(p.BaseOffset)
			w.Int64(-1) // LogAppendTimeMs: record timestamps are the producer's
			if v >= 5 {
				w.Int64(-1) // LogStartOffset: the schema's default, "unknown"; Fetch carries it
			}
			if v >= 8 {
				w.ArrayLen(flex, 0)
				w.NullString(flex)
			}
			w.Tags(flex)
		}
		w.Tags(flex)
	}
	w.Int32(0)
	w.Tags(flex)
}

// FetchPartition is one partition of a Fetch request.
type FetchPartition struct {
	Index    int32
	Offset   int64
	MaxBytes int32
}

// FetchTopic is one topic of a Fetch request.
type FetchTopic struct {
	Name       string // v4-12
	TopicID    UUID   // v13+
	Partitions []FetchPartition
}

// FetchRequest is the body of a Fetch request (key 1).
type FetchRequest struct {
	MaxWaitMs, MinBytes, MaxBytes int32
	IsolationLevel                int8 // v4+: 0=READ_UNCOMMITTED, 1=READ_COMMITTED
	Topics                        []FetchTopic
	// RackID is the consumer's rack (v11+): the broker may name a replica in
	// it to fetch from (KIP-392).
	RackID string
}

// Decode implements Message.
func (m *FetchRequest) Decode(r *Reader, v int16) {
	flex := v >= 12
	r.Int32() // ReplicaId
	m.MaxWaitMs = r.Int32()
	m.MinBytes = r.Int32()
	m.MaxBytes = r.Int32() // v3+
	m.IsolationLevel = r.Int8() // v4+
	if v >= 7 {
		r.Int32() // SessionId: sessions are declined
		r.Int32() // SessionEpoch
	}
	m.Topics = Array(r, flex, func() FetchTopic {
		var t FetchTopic
		if v >= 13 {
			t.TopicID = r.uuid()
		} else {
			t.Name = r.String(flex)
		}
		t.Partitions = Array(r, flex, func() FetchPartition {
			p := FetchPartition{Index: r.Int32()}
			if v >= 9 {
				r.Int32() // CurrentLeaderEpoch
			}
			p.Offset = r.Int64()
			if v >= 12 {
				r.Int32() // LastFetchedEpoch
			}
			if v >= 5 {
				r.Int64() // LogStartOffset
			}
			p.MaxBytes = r.Int32()
			r.Tags(flex)
			return p
		})
		r.Tags(flex)
		return t
	})
	if v >= 7 {
		Array(r, flex, func() struct{} { // ForgottenTopicsData
			if v >= 13 {
				r.uuid()
			} else {
				r.String(flex)
			}
			readInt32s(r, flex)
			r.Tags(flex)
			return struct{}{}
		})
	}
	if v >= 11 {
		m.RackID = r.String(flex)
	}
	r.Tags(flex)
}

// Encode implements Message.
func (m *FetchRequest) Encode(w *Writer, v int16) {
	flex := v >= 12
	w.Int32(-1)
	w.Int32(m.MaxWaitMs)
	w.Int32(m.MinBytes)
	w.Int32(m.MaxBytes)
	w.Int8(m.IsolationLevel)
	if v >= 7 {
		w.Int32(0)
		w.Int32(-1)
	}
	w.ArrayLen(flex, len(m.Topics))
	for _, t := range m.Topics {
		if v >= 13 {
			w.uuid(t.TopicID)
		} else {
			w.String(flex, t.Name)
		}
		w.ArrayLen(flex, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			if v >= 9 {
				w.Int32(-1)
			}
			w.Int64(p.Offset)
			if v >= 12 {
				w.Int32(-1)
			}
			if v >= 5 {
				w.Int64(-1)
			}
			w.Int32(p.MaxBytes)
			w.Tags(flex)
		}
		w.Tags(flex)
	}
	if v >= 7 {
		w.ArrayLen(flex, 0)
	}
	if v >= 11 {
		w.String(flex, m.RackID)
	}
	w.Tags(flex)
}

// AbortedTransaction is one aborted transaction range in a Fetch response.
type AbortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
}

// FetchPartitionResponse is one partition's result in a Fetch response.
type FetchPartitionResponse struct {
	Index               int32
	ErrorCode           int16
	HighWatermark       int64
	LastStableOffset    int64 // v4+ (when HasLastStableOffset is false, HighWatermark is used)
	HasLastStableOffset bool
	LogStartOffset      int64 // v5+: the first offset still available
	AbortedTransactions []AbortedTransaction
	Records             []byte // whole record batches, or nil
	// PreferredReadReplica, if HasPreferred (v11+), is the node the consumer
	// should fetch from next, in its own rack (KIP-392).
	PreferredReadReplica int32
	HasPreferred         bool
}

// FetchTopicResponse is one topic of a Fetch response.
type FetchTopicResponse struct {
	Name       string // v4-12
	TopicID    UUID   // v13+
	Partitions []FetchPartitionResponse
}

// FetchResponse is the body of a Fetch response.
type FetchResponse struct{ Topics []FetchTopicResponse }

// Decode implements Message.
func (m *FetchResponse) Decode(r *Reader, v int16) {
	flex := v >= 12
	r.Int32() // ThrottleTimeMs, v1+
	if v >= 7 {
		r.Int16() // ErrorCode
		r.Int32() // SessionId
	}
	m.Topics = Array(r, flex, func() FetchTopicResponse {
		var t FetchTopicResponse
		if v >= 13 {
			t.TopicID = r.uuid()
		} else {
			t.Name = r.String(flex)
		}
		t.Partitions = Array(r, flex, func() FetchPartitionResponse {
			p := FetchPartitionResponse{Index: r.Int32(), ErrorCode: r.Int16(), HighWatermark: r.Int64()}
			p.LastStableOffset = r.Int64() // LastStableOffset, v4+
			p.HasLastStableOffset = true
			if v >= 5 {
				p.LogStartOffset = r.Int64()
			}
			p.AbortedTransactions = Array(r, flex, func() AbortedTransaction { // AbortedTransactions, v4+
				a := AbortedTransaction{ProducerID: r.Int64(), FirstOffset: r.Int64()}
				r.Tags(flex)
				return a
			})
			if v >= 11 {
				if id := r.Int32(); id >= 0 {
					p.PreferredReadReplica, p.HasPreferred = id, true
				}
			}
			if p.Records = r.Bytes(flex); len(p.Records) == 0 {
				p.Records = nil
			}
			r.Tags(flex)
			return p
		})
		r.Tags(flex)
		return t
	})
	r.Tags(flex)
}

// Encode implements Message.
func (m *FetchResponse) Encode(w *Writer, v int16) {
	flex := v >= 12
	w.Int32(0)
	if v >= 7 {
		w.Int16(ErrNone)
		w.Int32(0) // SessionId 0: no session, keep sending full requests
	}
	w.ArrayLen(flex, len(m.Topics))
	for _, t := range m.Topics {
		if v >= 13 {
			w.uuid(t.TopicID)
		} else {
			w.String(flex, t.Name)
		}
		w.ArrayLen(flex, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Int16(p.ErrorCode)
			w.Int64(p.HighWatermark)
			lso := p.HighWatermark
			if p.HasLastStableOffset {
				lso = p.LastStableOffset
			}
			w.Int64(lso)
			if v >= 5 {
				w.Int64(p.LogStartOffset)
			}
			if len(p.AbortedTransactions) == 0 {
				w.ArrayLen(flex, -1) // null when empty
			} else {
				w.ArrayLen(flex, len(p.AbortedTransactions))
				for _, a := range p.AbortedTransactions {
					w.Int64(a.ProducerID)
					w.Int64(a.FirstOffset)
					w.Tags(flex)
				}
			}
			if v >= 11 {
				if p.HasPreferred {
					w.Int32(p.PreferredReadReplica)
				} else {
					w.Int32(-1)
				}
			}
			// "No records" is sent as an empty set, never as null. The
			// definition allows null, and the Java client and franz-go
			// accept it, but librdkafka rejects it ("invalid
			// MessageSetSize -1"). Found by the kcat integration test.
			recs := p.Records
			if recs == nil {
				recs = []byte{}
			}
			w.Bytes(flex, recs)
			w.Tags(flex)
		}
		w.Tags(flex)
	}
	w.Tags(flex)
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
type ListOffsetsRequest struct {
	IsolationLevel int8
	Topics         []ListOffsetsTopic
}

// Decode implements Message.
func (m *ListOffsetsRequest) Decode(r *Reader, v int16) {
	r.Int32() // ReplicaId
	if v >= 2 {
		m.IsolationLevel = r.Int8()
	}
	flex := v >= 6
	m.Topics = Array(r, flex, func() ListOffsetsTopic {
		t := ListOffsetsTopic{Name: r.String(flex)}
		t.Partitions = Array(r, flex, func() ListOffsetsPartition {
			p := ListOffsetsPartition{Index: r.Int32()}
			if v >= 4 {
				r.Int32() // CurrentLeaderEpoch
			}
			p.Timestamp = r.Int64()
			r.Tags(flex)
			return p
		})
		r.Tags(flex)
		return t
	})
	r.Tags(flex)
}

// Encode implements Message.
func (m *ListOffsetsRequest) Encode(w *Writer, v int16) {
	w.Int32(-1)
	if v >= 2 {
		w.Int8(m.IsolationLevel)
	}
	flex := v >= 6
	w.ArrayLen(flex, len(m.Topics))
	for _, t := range m.Topics {
		w.String(flex, t.Name)
		w.ArrayLen(flex, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			if v >= 4 {
				w.Int32(-1)
			}
			w.Int64(p.Timestamp)
			w.Tags(flex)
		}
		w.Tags(flex)
	}
	w.Tags(flex)
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
	flex := v >= 6
	m.Topics = Array(r, flex, func() ListOffsetsTopicResponse {
		t := ListOffsetsTopicResponse{Name: r.String(flex)}
		t.Partitions = Array(r, flex, func() ListOffsetsPartitionResponse {
			p := ListOffsetsPartitionResponse{Index: r.Int32(), ErrorCode: r.Int16(), Timestamp: r.Int64(), Offset: r.Int64()}
			if v >= 4 {
				r.Int32() // LeaderEpoch
			}
			r.Tags(flex)
			return p
		})
		r.Tags(flex)
		return t
	})
	r.Tags(flex)
}

// Encode implements Message.
func (m *ListOffsetsResponse) Encode(w *Writer, v int16) {
	if v >= 2 {
		w.Int32(0)
	}
	flex := v >= 6
	w.ArrayLen(flex, len(m.Topics))
	for _, t := range m.Topics {
		w.String(flex, t.Name)
		w.ArrayLen(flex, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Int16(p.ErrorCode)
			w.Int64(p.Timestamp)
			w.Int64(p.Offset)
			if v >= 4 {
				w.Int32(0) // LeaderEpoch
			}
			w.Tags(flex)
		}
		w.Tags(flex)
	}
	w.Tags(flex)
}

// InitProducerIDRequest is the body of an InitProducerId request (key 22).
type InitProducerIDRequest struct {
	Transactional        bool
	TransactionalID      string
	TransactionTimeoutMs int32
	ProducerID           int64 // v3+ (-1 if unset)
	ProducerEpoch        int16 // v3+ (-1 if unset)
}

// Decode implements Message.
func (m *InitProducerIDRequest) Decode(r *Reader, v int16) {
	flex := v >= 2
	tid, null := r.NullableString(flex)
	m.Transactional = !null
	m.TransactionalID = tid
	m.TransactionTimeoutMs = r.Int32()
	m.ProducerID, m.ProducerEpoch = -1, -1
	if v >= 3 {
		m.ProducerID = r.Int64()
		m.ProducerEpoch = r.Int16()
	}
	r.Tags(flex)
}

// Encode implements Message.
func (m *InitProducerIDRequest) Encode(w *Writer, v int16) {
	flex := v >= 2
	if m.Transactional || m.TransactionalID != "" {
		w.String(flex, m.TransactionalID)
	} else {
		w.NullString(flex)
	}
	timeout := m.TransactionTimeoutMs
	if timeout == 0 && m.Transactional {
		timeout = 1<<31 - 1
	} else if timeout == 0 {
		timeout = 1<<31 - 1
	}
	w.Int32(timeout)
	if v >= 3 {
		pid := m.ProducerID
		ep := m.ProducerEpoch
		if pid == 0 && ep == 0 && m.TransactionalID == "" {
			pid, ep = -1, -1
		}
		w.Int64(pid)
		w.Int16(ep)
	}
	w.Tags(flex)
}

// InitProducerIDResponse is the body of an InitProducerId response.
type InitProducerIDResponse struct {
	ErrorCode     int16
	ProducerID    int64
	ProducerEpoch int16
}

// Decode implements Message.
func (m *InitProducerIDResponse) Decode(r *Reader, v int16) {
	r.Int32() // ThrottleTimeMs
	m.ErrorCode = r.Int16()
	m.ProducerID = r.Int64()
	m.ProducerEpoch = r.Int16()
	r.Tags(v >= 2)
}

// Encode implements Message.
func (m *InitProducerIDResponse) Encode(w *Writer, v int16) {
	w.Int32(0) // ThrottleTimeMs
	w.Int16(m.ErrorCode)
	w.Int64(m.ProducerID)
	w.Int16(m.ProducerEpoch)
	w.Tags(v >= 2)
}
