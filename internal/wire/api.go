package wire

// API keys of the supported requests.
const (
	KeyProduce      int16 = 0
	KeyFetch        int16 = 1
	KeyListOffsets  int16 = 2
	KeyMetadata     int16 = 3
	KeyOffsetCommit int16 = 8
	KeyOffsetFetch  int16 = 9
	KeyFindCoord    int16 = 10
	KeyJoinGroup    int16 = 11
	KeyHeartbeat    int16 = 12
	KeyLeaveGroup   int16 = 13
	KeySyncGroup    int16 = 14
	// KeyDescribeGroups and the keys below are the admin APIs of B4.
	KeyDescribeGroups   int16 = 15
	KeyListGroups       int16 = 16
	KeyDeleteTopics     int16 = 20
	KeyDeleteRecords    int16 = 21
	KeyDescribeConfigs  int16 = 32
	KeyCreatePartitions int16 = 37
	KeyDescribeCluster  int16 = 60
	KeyAPIVersions      int16 = 18
	KeyCreateTopics     int16 = 19
	// KeySaslHandshake and KeySaslAuthenticate authenticate a connection
	// (ADR 0019).
	KeySaslHandshake    int16 = 17
	KeySaslAuthenticate int16 = 36
	// KeyInitProducerID is how an idempotent producer gets its id.
	KeyInitProducerID int16 = 22
)

// Kafka error codes used by the broker, from Errors.java.
const (
	ErrUnknownServerError          int16 = -1
	ErrNone                        int16 = 0
	ErrOffsetOutOfRange            int16 = 1
	ErrCorruptMessage              int16 = 2
	ErrUnknownTopicOrPartition     int16 = 3
	ErrLeaderNotAvailable          int16 = 5
	ErrNotLeaderOrFollower         int16 = 6
	ErrMessageTooLarge             int16 = 10
	ErrOffsetMetadataTooLarge      int16 = 12
	ErrCoordinatorNotAvailable     int16 = 15
	ErrNotCoordinator              int16 = 16
	ErrInvalidTopic                int16 = 17
	ErrInvalidRequiredAcks         int16 = 21
	ErrIllegalGeneration           int16 = 22
	ErrInconsistentGroupProtocol   int16 = 23
	ErrInvalidGroupID              int16 = 24
	ErrUnknownMemberID             int16 = 25
	ErrInvalidSessionTimeout       int16 = 26
	ErrRebalanceInProgress         int16 = 27
	ErrUnsupportedSaslMechanism    int16 = 33
	ErrIllegalSaslState            int16 = 34
	ErrUnsupportedVersion          int16 = 35
	ErrTopicAlreadyExists          int16 = 36
	ErrInvalidPartitions           int16 = 37
	ErrInvalidReplicationFactor    int16 = 38
	ErrInvalidReplicaAssignment    int16 = 39
	ErrNotController               int16 = 41
	ErrInvalidRequest              int16 = 42
	ErrUnsupportedForMessageFormat int16 = 43
	ErrOutOfOrderSequenceNumber    int16 = 45
	ErrInvalidProducerEpoch        int16 = 47
	ErrMemberIDRequired            int16 = 79
	ErrGroupMaxSizeReached         int16 = 81
	ErrSaslAuthenticationFailed    int16 = 58
	ErrInvalidRecord               int16 = 87
	ErrUnknownTopicID              int16 = 100
	ErrUnsupportedEndpointType     int16 = 115
)

// API describes the versions of one API that the broker speaks.
type API struct {
	Key int16
	// Min and Max bound the versions the broker accepts.
	Min, Max int16
	// AdvertisedMin is the minimum reported in ApiVersions. It differs from
	// Min only for Produce, which Kafka advertises from version 0 because
	// librdkafka derives feature support from the advertised range
	// (KAFKA-18659).
	AdvertisedMin int16
	// FlexFrom is the first flexible version.
	FlexFrom int16
}

// APIs lists every supported API in ascending key order. ADR 0006 explains
// the ranges.
var APIs = []API{
	{Key: KeyProduce, Min: 3, Max: 8, AdvertisedMin: 0, FlexFrom: 9},
	{Key: KeyFetch, Min: 4, Max: 11, AdvertisedMin: 4, FlexFrom: 12},
	{Key: KeyListOffsets, Min: 1, Max: 5, AdvertisedMin: 1, FlexFrom: 6},
	{Key: KeyMetadata, Min: 1, Max: 8, AdvertisedMin: 1, FlexFrom: 9},
	{Key: KeyOffsetCommit, Min: 2, Max: 7, AdvertisedMin: 2, FlexFrom: 8},
	{Key: KeyOffsetFetch, Min: 1, Max: 5, AdvertisedMin: 1, FlexFrom: 6},
	{Key: KeyFindCoord, Min: 0, Max: 2, AdvertisedMin: 0, FlexFrom: 3},
	{Key: KeyJoinGroup, Min: 0, Max: 5, AdvertisedMin: 0, FlexFrom: 6},
	{Key: KeyHeartbeat, Min: 0, Max: 3, AdvertisedMin: 0, FlexFrom: 4},
	{Key: KeyLeaveGroup, Min: 0, Max: 3, AdvertisedMin: 0, FlexFrom: 4},
	{Key: KeySyncGroup, Min: 0, Max: 3, AdvertisedMin: 0, FlexFrom: 4},
	{Key: KeyDescribeGroups, Min: 0, Max: 6, AdvertisedMin: 0, FlexFrom: 5},
	{Key: KeyListGroups, Min: 0, Max: 5, AdvertisedMin: 0, FlexFrom: 3},
	{Key: KeySaslHandshake, Min: 1, Max: 1, AdvertisedMin: 1, FlexFrom: 2},
	{Key: KeyAPIVersions, Min: 0, Max: 4, AdvertisedMin: 0, FlexFrom: 3},
	{Key: KeyCreateTopics, Min: 2, Max: 4, AdvertisedMin: 2, FlexFrom: 5},
	{Key: KeyDeleteTopics, Min: 1, Max: 6, AdvertisedMin: 1, FlexFrom: 4},
	{Key: KeyDeleteRecords, Min: 0, Max: 2, AdvertisedMin: 0, FlexFrom: 2},
	{Key: KeyInitProducerID, Min: 0, Max: 4, AdvertisedMin: 0, FlexFrom: 2},
	{Key: KeyDescribeConfigs, Min: 1, Max: 4, AdvertisedMin: 1, FlexFrom: 4},
	{Key: KeySaslAuthenticate, Min: 0, Max: 2, AdvertisedMin: 0, FlexFrom: 2},
	{Key: KeyCreatePartitions, Min: 0, Max: 3, AdvertisedMin: 0, FlexFrom: 2},
	{Key: KeyDescribeCluster, Min: 0, Max: 2, AdvertisedMin: 0, FlexFrom: 0},
}

// LookupAPI returns the entry for key.
func LookupAPI(key int16) (API, bool) {
	for _, a := range APIs {
		if a.Key == key {
			return a, true
		}
	}
	return API{}, false
}

// Supported reports whether the broker accepts this version of this API.
func Supported(key, version int16) bool {
	a, ok := LookupAPI(key)
	return ok && version >= a.Min && version <= a.Max
}

// flexible reports whether a version of an API uses the compact encoding
// with tagged fields. Unknown APIs are treated as not flexible.
func flexible(key, version int16) bool {
	a, ok := LookupAPI(key)
	return ok && version >= a.FlexFrom
}

// RequestHeader precedes every request.
type RequestHeader struct {
	Key, Version  int16
	CorrelationID int32
	ClientID      string // empty if the client sent null
}

// Decode reads a request header. The first three fields and the client id
// have the same layout in every header version; flexible requests add
// tagged fields after them.
func (h *RequestHeader) Decode(r *Reader) {
	h.Key = r.Int16()
	h.Version = r.Int16()
	h.CorrelationID = r.Int32()
	h.ClientID, _ = r.NullableString(false) // never compact, even in flexible headers
	r.Tags(flexible(h.Key, h.Version))
}

// Encode appends the header.
func (h *RequestHeader) Encode(w *Writer) {
	w.Int16(h.Key)
	w.Int16(h.Version)
	w.Int32(h.CorrelationID)
	w.String(false, h.ClientID)
	w.Tags(flexible(h.Key, h.Version))
}

// responseHeaderFlexible reports whether the response header carries tagged
// fields. ApiVersions never does, so that a client can read the response
// before it knows what the broker supports.
func responseHeaderFlexible(key, version int16) bool {
	return key != KeyAPIVersions && flexible(key, version)
}

// EncodeResponseHeader appends the response header for a request.
func EncodeResponseHeader(w *Writer, key, version int16, correlationID int32) {
	w.Int32(correlationID)
	w.Tags(responseHeaderFlexible(key, version))
}

// DecodeResponseHeader reads a response header and returns its correlation id.
func DecodeResponseHeader(r *Reader, key, version int16) int32 {
	id := r.Int32()
	r.Tags(responseHeaderFlexible(key, version))
	return id
}

// Message is a request or response body that can be encoded and decoded at
// a given API version.
type Message interface {
	Decode(r *Reader, version int16)
	Encode(w *Writer, version int16)
}
