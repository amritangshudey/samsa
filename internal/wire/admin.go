package wire

// The admin APIs of docs/design/b4-operations.md, from the message schemas
// of kafka-clients 4.4.0 (ADR 0016). Every structure of a flexible version
// ends with tagged fields, which are skipped.

// UUID is a Kafka uuid: 16 bytes. Samsa has no topic ids and sends zero.
type UUID [16]byte

func (r *Reader) uuid() (u UUID) {
	copy(u[:], r.take(16))
	return u
}

func (w *Writer) uuid(u UUID) { w.B = append(w.B, u[:]...) }

// optString writes s, or null if it is empty: for fields where Samsa never
// means an empty string.
func (w *Writer) optString(flex bool, s string) {
	if s == "" {
		w.NullString(flex)
	} else {
		w.String(flex, s)
	}
}

func readStrings(r *Reader, flex bool) []string {
	return Array(r, flex, func() string { return r.String(flex) })
}

func writeStrings(w *Writer, flex bool, ss []string) {
	w.ArrayLen(flex, len(ss))
	for _, s := range ss {
		w.String(flex, s)
	}
}

// --- DeleteTopics (20) -----------------------------------------------------

// DeleteTopic names one topic to delete: by name, or from v6 by id alone
// (Name empty), which Samsa answers UNKNOWN_TOPIC_ID.
type DeleteTopic struct {
	Name    string
	TopicID UUID // v6+
}

// DeleteTopicsRequest is the body of a DeleteTopics request (key 20).
type DeleteTopicsRequest struct {
	Topics    []DeleteTopic
	TimeoutMs int32
}

// Decode implements Message.
func (m *DeleteTopicsRequest) Decode(r *Reader, v int16) {
	flex := v >= 4
	if v >= 6 {
		m.Topics = Array(r, flex, func() DeleteTopic {
			name, _ := r.NullableString(flex)
			t := DeleteTopic{Name: name, TopicID: r.uuid()}
			r.Tags(flex)
			return t
		})
	} else {
		for _, name := range readStrings(r, flex) {
			m.Topics = append(m.Topics, DeleteTopic{Name: name})
		}
	}
	m.TimeoutMs = r.Int32()
	r.Tags(flex)
}

// Encode implements Message.
func (m *DeleteTopicsRequest) Encode(w *Writer, v int16) {
	flex := v >= 4
	w.ArrayLen(flex, len(m.Topics))
	for _, t := range m.Topics {
		if v >= 6 {
			w.optString(flex, t.Name)
			w.uuid(t.TopicID)
			w.Tags(flex)
		} else {
			w.String(flex, t.Name)
		}
	}
	w.Int32(m.TimeoutMs)
	w.Tags(flex)
}

// DeleteTopicResult is one topic of a DeleteTopics response.
type DeleteTopicResult struct {
	Name         string
	TopicID      UUID // v6+
	ErrorCode    int16
	ErrorMessage string // v5+; null when empty
}

// DeleteTopicsResponse is the body of a DeleteTopics response.
type DeleteTopicsResponse struct{ Topics []DeleteTopicResult }

// Decode implements Message.
func (m *DeleteTopicsResponse) Decode(r *Reader, v int16) {
	flex := v >= 4
	r.Int32() // ThrottleTimeMs
	m.Topics = Array(r, flex, func() DeleteTopicResult {
		var t DeleteTopicResult
		t.Name, _ = r.NullableString(flex)
		if v >= 6 {
			t.TopicID = r.uuid()
		}
		t.ErrorCode = r.Int16()
		if v >= 5 {
			t.ErrorMessage, _ = r.NullableString(flex)
		}
		r.Tags(flex)
		return t
	})
	r.Tags(flex)
}

// Encode implements Message.
func (m *DeleteTopicsResponse) Encode(w *Writer, v int16) {
	flex := v >= 4
	w.Int32(0)
	w.ArrayLen(flex, len(m.Topics))
	for _, t := range m.Topics {
		if v >= 6 {
			w.optString(flex, t.Name)
			w.uuid(t.TopicID)
		} else {
			w.String(flex, t.Name)
		}
		w.Int16(t.ErrorCode)
		if v >= 5 {
			w.optString(flex, t.ErrorMessage)
		}
		w.Tags(flex)
	}
	w.Tags(flex)
}

// --- DeleteRecords (21) ----------------------------------------------------

// DeleteRecordsPartition asks to delete a partition's records below Offset;
// -1 means the high watermark.
type DeleteRecordsPartition struct {
	Index  int32
	Offset int64
}

// DeleteRecordsTopic is one topic of a DeleteRecords request.
type DeleteRecordsTopic struct {
	Name       string
	Partitions []DeleteRecordsPartition
}

// DeleteRecordsRequest is the body of a DeleteRecords request (key 21).
type DeleteRecordsRequest struct {
	Topics    []DeleteRecordsTopic
	TimeoutMs int32
}

// Decode implements Message.
func (m *DeleteRecordsRequest) Decode(r *Reader, v int16) {
	flex := v >= 2
	m.Topics = Array(r, flex, func() DeleteRecordsTopic {
		t := DeleteRecordsTopic{Name: r.String(flex)}
		t.Partitions = Array(r, flex, func() DeleteRecordsPartition {
			p := DeleteRecordsPartition{Index: r.Int32(), Offset: r.Int64()}
			r.Tags(flex)
			return p
		})
		r.Tags(flex)
		return t
	})
	m.TimeoutMs = r.Int32()
	r.Tags(flex)
}

// Encode implements Message.
func (m *DeleteRecordsRequest) Encode(w *Writer, v int16) {
	flex := v >= 2
	w.ArrayLen(flex, len(m.Topics))
	for _, t := range m.Topics {
		w.String(flex, t.Name)
		w.ArrayLen(flex, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Int64(p.Offset)
			w.Tags(flex)
		}
		w.Tags(flex)
	}
	w.Int32(m.TimeoutMs)
	w.Tags(flex)
}

// DeleteRecordsPartitionResult is one partition of a DeleteRecords response.
type DeleteRecordsPartitionResult struct {
	Index        int32
	LowWatermark int64
	ErrorCode    int16
}

// DeleteRecordsTopicResult is one topic of a DeleteRecords response.
type DeleteRecordsTopicResult struct {
	Name       string
	Partitions []DeleteRecordsPartitionResult
}

// DeleteRecordsResponse is the body of a DeleteRecords response.
type DeleteRecordsResponse struct{ Topics []DeleteRecordsTopicResult }

// Decode implements Message.
func (m *DeleteRecordsResponse) Decode(r *Reader, v int16) {
	flex := v >= 2
	r.Int32() // ThrottleTimeMs
	m.Topics = Array(r, flex, func() DeleteRecordsTopicResult {
		t := DeleteRecordsTopicResult{Name: r.String(flex)}
		t.Partitions = Array(r, flex, func() DeleteRecordsPartitionResult {
			p := DeleteRecordsPartitionResult{Index: r.Int32(), LowWatermark: r.Int64(), ErrorCode: r.Int16()}
			r.Tags(flex)
			return p
		})
		r.Tags(flex)
		return t
	})
	r.Tags(flex)
}

// Encode implements Message.
func (m *DeleteRecordsResponse) Encode(w *Writer, v int16) {
	flex := v >= 2
	w.Int32(0)
	w.ArrayLen(flex, len(m.Topics))
	for _, t := range m.Topics {
		w.String(flex, t.Name)
		w.ArrayLen(flex, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Int64(p.LowWatermark)
			w.Int16(p.ErrorCode)
			w.Tags(flex)
		}
		w.Tags(flex)
	}
	w.Tags(flex)
}

// --- DescribeConfigs (32) --------------------------------------------------

// Config resource types.
const (
	ResourceTopic  int8 = 2
	ResourceBroker int8 = 4
)

// Config sources, as DescribeConfigs reports them.
const (
	ConfigSourceStaticBroker int8 = 4
	ConfigSourceDefault      int8 = 5
)

// DescribeConfigsResource names one resource to describe; Keys nil means
// every config.
type DescribeConfigsResource struct {
	Type int8
	Name string
	Keys []string
}

// DescribeConfigsRequest is the body of a DescribeConfigs request (key 32).
type DescribeConfigsRequest struct {
	Resources            []DescribeConfigsResource
	IncludeSynonyms      bool
	IncludeDocumentation bool // v3+
}

// Decode implements Message.
func (m *DescribeConfigsRequest) Decode(r *Reader, v int16) {
	flex := v >= 4
	m.Resources = Array(r, flex, func() DescribeConfigsResource {
		res := DescribeConfigsResource{Type: r.Int8(), Name: r.String(flex), Keys: readStrings(r, flex)}
		r.Tags(flex)
		return res
	})
	m.IncludeSynonyms = r.Bool()
	if v >= 3 {
		m.IncludeDocumentation = r.Bool()
	}
	r.Tags(flex)
}

// Encode implements Message. Keys nil is written as null.
func (m *DescribeConfigsRequest) Encode(w *Writer, v int16) {
	flex := v >= 4
	w.ArrayLen(flex, len(m.Resources))
	for _, res := range m.Resources {
		w.Int8(res.Type)
		w.String(flex, res.Name)
		if res.Keys == nil {
			w.ArrayLen(flex, -1)
		} else {
			writeStrings(w, flex, res.Keys)
		}
		w.Tags(flex)
	}
	w.Bool(m.IncludeSynonyms)
	if v >= 3 {
		w.Bool(m.IncludeDocumentation)
	}
	w.Tags(flex)
}

// ConfigEntry is one config of a described resource. Samsa sends no
// synonyms and no documentation; Value is null when empty.
type ConfigEntry struct {
	Name        string
	Value       string
	ReadOnly    bool
	Source      int8
	IsSensitive bool
	Type        int8 // v3+
}

// DescribeConfigsResult is one resource of a DescribeConfigs response.
type DescribeConfigsResult struct {
	ErrorCode    int16
	ErrorMessage string // null when empty
	Type         int8
	Name         string
	Configs      []ConfigEntry
}

// DescribeConfigsResponse is the body of a DescribeConfigs response.
type DescribeConfigsResponse struct{ Results []DescribeConfigsResult }

// Decode implements Message.
func (m *DescribeConfigsResponse) Decode(r *Reader, v int16) {
	flex := v >= 4
	r.Int32() // ThrottleTimeMs
	m.Results = Array(r, flex, func() DescribeConfigsResult {
		var res DescribeConfigsResult
		res.ErrorCode = r.Int16()
		res.ErrorMessage, _ = r.NullableString(flex)
		res.Type = r.Int8()
		res.Name = r.String(flex)
		res.Configs = Array(r, flex, func() ConfigEntry {
			var c ConfigEntry
			c.Name = r.String(flex)
			c.Value, _ = r.NullableString(flex)
			c.ReadOnly = r.Bool()
			c.Source = r.Int8()
			c.IsSensitive = r.Bool()
			Array(r, flex, func() struct{} { // Synonyms
				r.String(flex)
				r.NullableString(flex)
				r.Int8()
				r.Tags(flex)
				return struct{}{}
			})
			if v >= 3 {
				c.Type = r.Int8()
				r.NullableString(flex) // Documentation
			}
			r.Tags(flex)
			return c
		})
		r.Tags(flex)
		return res
	})
	r.Tags(flex)
}

// Encode implements Message.
func (m *DescribeConfigsResponse) Encode(w *Writer, v int16) {
	flex := v >= 4
	w.Int32(0)
	w.ArrayLen(flex, len(m.Results))
	for _, res := range m.Results {
		w.Int16(res.ErrorCode)
		w.optString(flex, res.ErrorMessage)
		w.Int8(res.Type)
		w.String(flex, res.Name)
		w.ArrayLen(flex, len(res.Configs))
		for _, c := range res.Configs {
			w.String(flex, c.Name)
			w.optString(flex, c.Value)
			w.Bool(c.ReadOnly)
			w.Int8(c.Source)
			w.Bool(c.IsSensitive)
			w.ArrayLen(flex, 0) // Synonyms
			if v >= 3 {
				w.Int8(c.Type)
				w.NullString(flex) // Documentation
			}
			w.Tags(flex)
		}
		w.Tags(flex)
	}
	w.Tags(flex)
}

// --- CreatePartitions (37) -------------------------------------------------

// CreatePartitionsTopic asks for a topic to grow to Count partitions.
type CreatePartitionsTopic struct {
	Name  string
	Count int32
	// HasAssignments is set if the request placed the new partitions,
	// which Samsa refuses: every node holds every partition.
	HasAssignments bool
}

// CreatePartitionsRequest is the body of a CreatePartitions request (key 37).
type CreatePartitionsRequest struct {
	Topics       []CreatePartitionsTopic
	TimeoutMs    int32
	ValidateOnly bool
}

// Decode implements Message.
func (m *CreatePartitionsRequest) Decode(r *Reader, v int16) {
	flex := v >= 2
	m.Topics = Array(r, flex, func() CreatePartitionsTopic {
		t := CreatePartitionsTopic{Name: r.String(flex), Count: r.Int32()}
		n := r.ArrayLen(flex) // Assignments, nullable
		t.HasAssignments = n >= 0
		for ; n > 0 && r.Err() == nil; n-- {
			Array(r, flex, r.Int32) // BrokerIds
			r.Tags(flex)
		}
		r.Tags(flex)
		return t
	})
	m.TimeoutMs = r.Int32()
	m.ValidateOnly = r.Bool()
	r.Tags(flex)
}

// Encode implements Message. HasAssignments is written as an empty array,
// which survives a round trip.
func (m *CreatePartitionsRequest) Encode(w *Writer, v int16) {
	flex := v >= 2
	w.ArrayLen(flex, len(m.Topics))
	for _, t := range m.Topics {
		w.String(flex, t.Name)
		w.Int32(t.Count)
		if t.HasAssignments {
			w.ArrayLen(flex, 0)
		} else {
			w.ArrayLen(flex, -1)
		}
		w.Tags(flex)
	}
	w.Int32(m.TimeoutMs)
	w.Bool(m.ValidateOnly)
	w.Tags(flex)
}

// CreatePartitionsResult is one topic of a CreatePartitions response.
type CreatePartitionsResult struct {
	Name         string
	ErrorCode    int16
	ErrorMessage string // null when empty
}

// CreatePartitionsResponse is the body of a CreatePartitions response.
type CreatePartitionsResponse struct{ Results []CreatePartitionsResult }

// Decode implements Message.
func (m *CreatePartitionsResponse) Decode(r *Reader, v int16) {
	flex := v >= 2
	r.Int32() // ThrottleTimeMs
	m.Results = Array(r, flex, func() CreatePartitionsResult {
		res := CreatePartitionsResult{Name: r.String(flex), ErrorCode: r.Int16()}
		res.ErrorMessage, _ = r.NullableString(flex)
		r.Tags(flex)
		return res
	})
	r.Tags(flex)
}

// Encode implements Message.
func (m *CreatePartitionsResponse) Encode(w *Writer, v int16) {
	flex := v >= 2
	w.Int32(0)
	w.ArrayLen(flex, len(m.Results))
	for _, res := range m.Results {
		w.String(flex, res.Name)
		w.Int16(res.ErrorCode)
		w.optString(flex, res.ErrorMessage)
		w.Tags(flex)
	}
	w.Tags(flex)
}

// --- ListGroups (16) -------------------------------------------------------

// ListGroupsRequest is the body of a ListGroups request (key 16). Empty
// filters match everything.
type ListGroupsRequest struct {
	States []string // v4+
	Types  []string // v5+
}

// Decode implements Message.
func (m *ListGroupsRequest) Decode(r *Reader, v int16) {
	flex := v >= 3
	if v >= 4 {
		m.States = readStrings(r, flex)
	}
	if v >= 5 {
		m.Types = readStrings(r, flex)
	}
	r.Tags(flex)
}

// Encode implements Message.
func (m *ListGroupsRequest) Encode(w *Writer, v int16) {
	flex := v >= 3
	if v >= 4 {
		writeStrings(w, flex, m.States)
	}
	if v >= 5 {
		writeStrings(w, flex, m.Types)
	}
	w.Tags(flex)
}

// ListedGroup is one group of a ListGroups response.
type ListedGroup struct {
	ID           string
	ProtocolType string
	State        string // v4+
	Type         string // v5+
}

// ListGroupsResponse is the body of a ListGroups response.
type ListGroupsResponse struct {
	ErrorCode int16
	Groups    []ListedGroup
}

// Decode implements Message.
func (m *ListGroupsResponse) Decode(r *Reader, v int16) {
	flex := v >= 3
	if v >= 1 {
		r.Int32() // ThrottleTimeMs
	}
	m.ErrorCode = r.Int16()
	m.Groups = Array(r, flex, func() ListedGroup {
		g := ListedGroup{ID: r.String(flex), ProtocolType: r.String(flex)}
		if v >= 4 {
			g.State = r.String(flex)
		}
		if v >= 5 {
			g.Type = r.String(flex)
		}
		r.Tags(flex)
		return g
	})
	r.Tags(flex)
}

// Encode implements Message.
func (m *ListGroupsResponse) Encode(w *Writer, v int16) {
	flex := v >= 3
	if v >= 1 {
		w.Int32(0)
	}
	w.Int16(m.ErrorCode)
	w.ArrayLen(flex, len(m.Groups))
	for _, g := range m.Groups {
		w.String(flex, g.ID)
		w.String(flex, g.ProtocolType)
		if v >= 4 {
			w.String(flex, g.State)
		}
		if v >= 5 {
			w.String(flex, g.Type)
		}
		w.Tags(flex)
	}
	w.Tags(flex)
}

// --- DescribeGroups (15) ---------------------------------------------------

// DescribeGroupsRequest is the body of a DescribeGroups request (key 15).
type DescribeGroupsRequest struct {
	Groups                      []string
	IncludeAuthorizedOperations bool // v3+
}

// Decode implements Message.
func (m *DescribeGroupsRequest) Decode(r *Reader, v int16) {
	flex := v >= 5
	m.Groups = readStrings(r, flex)
	if v >= 3 {
		m.IncludeAuthorizedOperations = r.Bool()
	}
	r.Tags(flex)
}

// Encode implements Message.
func (m *DescribeGroupsRequest) Encode(w *Writer, v int16) {
	flex := v >= 5
	writeStrings(w, flex, m.Groups)
	if v >= 3 {
		w.Bool(m.IncludeAuthorizedOperations)
	}
	w.Tags(flex)
}

// DescribedMember is one member of a described group. Samsa has no static
// membership, so GroupInstanceId is always null.
type DescribedMember struct {
	ID         string
	ClientID   string
	ClientHost string
	Metadata   []byte
	Assignment []byte
}

// DescribedGroup is one group of a DescribeGroups response.
type DescribedGroup struct {
	ErrorCode    int16
	ErrorMessage string // v6+; null when empty
	ID           string
	State        string
	ProtocolType string
	ProtocolData string
	Members      []DescribedMember
}

// authorizedOperationsOmitted is INT32_MIN: "not requested".
const authorizedOperationsOmitted int32 = -1 << 31

// DescribeGroupsResponse is the body of a DescribeGroups response.
type DescribeGroupsResponse struct{ Groups []DescribedGroup }

// Decode implements Message.
func (m *DescribeGroupsResponse) Decode(r *Reader, v int16) {
	flex := v >= 5
	if v >= 1 {
		r.Int32() // ThrottleTimeMs
	}
	m.Groups = Array(r, flex, func() DescribedGroup {
		var g DescribedGroup
		g.ErrorCode = r.Int16()
		if v >= 6 {
			g.ErrorMessage, _ = r.NullableString(flex)
		}
		g.ID = r.String(flex)
		g.State = r.String(flex)
		g.ProtocolType = r.String(flex)
		g.ProtocolData = r.String(flex)
		g.Members = Array(r, flex, func() DescribedMember {
			mem := DescribedMember{ID: r.String(flex)}
			if v >= 4 {
				r.NullableString(flex) // GroupInstanceId
			}
			mem.ClientID = r.String(flex)
			mem.ClientHost = r.String(flex)
			mem.Metadata = r.Bytes(flex)
			mem.Assignment = r.Bytes(flex)
			r.Tags(flex)
			return mem
		})
		if v >= 3 {
			r.Int32() // AuthorizedOperations
		}
		r.Tags(flex)
		return g
	})
	r.Tags(flex)
}

// Encode implements Message.
func (m *DescribeGroupsResponse) Encode(w *Writer, v int16) {
	flex := v >= 5
	if v >= 1 {
		w.Int32(0)
	}
	w.ArrayLen(flex, len(m.Groups))
	for _, g := range m.Groups {
		w.Int16(g.ErrorCode)
		if v >= 6 {
			w.optString(flex, g.ErrorMessage)
		}
		w.String(flex, g.ID)
		w.String(flex, g.State)
		w.String(flex, g.ProtocolType)
		w.String(flex, g.ProtocolData)
		w.ArrayLen(flex, len(g.Members))
		for _, mem := range g.Members {
			w.String(flex, mem.ID)
			if v >= 4 {
				w.NullString(flex)
			}
			w.String(flex, mem.ClientID)
			w.String(flex, mem.ClientHost)
			w.Bytes(flex, mem.Metadata)
			w.Bytes(flex, mem.Assignment)
			w.Tags(flex)
		}
		if v >= 3 {
			w.Int32(authorizedOperationsOmitted)
		}
		w.Tags(flex)
	}
	w.Tags(flex)
}

// --- DescribeCluster (60) --------------------------------------------------

// Endpoint types of DescribeCluster.
const (
	EndpointBrokers     int8 = 1
	EndpointControllers int8 = 2
)

// DescribeClusterRequest is the body of a DescribeCluster request (key 60).
type DescribeClusterRequest struct {
	IncludeAuthorizedOperations bool
	EndpointType                int8 // v1+; 1 before
	IncludeFencedBrokers        bool // v2+
}

// Decode implements Message.
func (m *DescribeClusterRequest) Decode(r *Reader, v int16) {
	m.IncludeAuthorizedOperations = r.Bool()
	m.EndpointType = EndpointBrokers
	if v >= 1 {
		m.EndpointType = r.Int8()
	}
	if v >= 2 {
		m.IncludeFencedBrokers = r.Bool()
	}
	r.Tags(true)
}

// Encode implements Message.
func (m *DescribeClusterRequest) Encode(w *Writer, v int16) {
	w.Bool(m.IncludeAuthorizedOperations)
	if v >= 1 {
		w.Int8(m.EndpointType)
	}
	if v >= 2 {
		w.Bool(m.IncludeFencedBrokers)
	}
	w.Tags(true)
}

// DescribeClusterResponse is the body of a DescribeCluster response. No
// broker has a rack or is fenced.
type DescribeClusterResponse struct {
	ErrorCode    int16
	ErrorMessage string // null when empty
	EndpointType int8   // v1+
	ClusterID    string
	ControllerID int32
	Brokers      []Broker
}

// Decode implements Message.
func (m *DescribeClusterResponse) Decode(r *Reader, v int16) {
	r.Int32() // ThrottleTimeMs
	m.ErrorCode = r.Int16()
	m.ErrorMessage, _ = r.NullableString(true)
	if v >= 1 {
		m.EndpointType = r.Int8()
	}
	m.ClusterID = r.String(true)
	m.ControllerID = r.Int32()
	m.Brokers = Array(r, true, func() Broker {
		b := Broker{NodeID: r.Int32(), Host: r.String(true), Port: r.Int32()}
		r.NullableString(true) // Rack
		if v >= 2 {
			r.Bool() // IsFenced
		}
		r.Tags(true)
		return b
	})
	r.Int32() // ClusterAuthorizedOperations
	r.Tags(true)
}

// Encode implements Message.
func (m *DescribeClusterResponse) Encode(w *Writer, v int16) {
	w.Int32(0)
	w.Int16(m.ErrorCode)
	w.optString(true, m.ErrorMessage)
	if v >= 1 {
		w.Int8(m.EndpointType)
	}
	w.String(true, m.ClusterID)
	w.Int32(m.ControllerID)
	w.ArrayLen(true, len(m.Brokers))
	for _, b := range m.Brokers {
		w.Int32(b.NodeID)
		w.String(true, b.Host)
		w.Int32(b.Port)
		w.NullString(true)
		if v >= 2 {
			w.Bool(false)
		}
		w.Tags(true)
	}
	w.Int32(authorizedOperationsOmitted)
	w.Tags(true)
}
