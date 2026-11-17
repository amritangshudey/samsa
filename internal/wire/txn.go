package wire

// AddPartitionsToTxnTopic is one topic's partitions in AddPartitionsToTxn.
type AddPartitionsToTxnTopic struct {
	Name       string
	Partitions []int32
}

// AddPartitionsToTxnTransaction is one transaction in AddPartitionsToTxn (v4+).
type AddPartitionsToTxnTransaction struct {
	TransactionalID string
	ProducerID      int64
	ProducerEpoch   int16
	VerifyOnly      bool
	Topics          []AddPartitionsToTxnTopic
}

// AddPartitionsToTxnRequest is the body of AddPartitionsToTxn (key 24, v0-5).
// Versions 0-3 carry one transaction; versions 4-5 batch multiple transactions.
type AddPartitionsToTxnRequest struct {
	Transactions []AddPartitionsToTxnTransaction
}

// Decode implements Message.
func (m *AddPartitionsToTxnRequest) Decode(r *Reader, v int16) {
	flex := v >= 3
	decodeTopics := func() []AddPartitionsToTxnTopic {
		return Array(r, flex, func() AddPartitionsToTxnTopic {
			t := AddPartitionsToTxnTopic{
				Name:       r.String(flex),
				Partitions: Array(r, flex, func() int32 { return r.Int32() }),
			}
			r.Tags(flex)
			return t
		})
	}
	if v >= 4 {
		m.Transactions = Array(r, flex, func() AddPartitionsToTxnTransaction {
			tx := AddPartitionsToTxnTransaction{
				TransactionalID: r.String(flex),
				ProducerID:      r.Int64(),
				ProducerEpoch:   r.Int16(),
				VerifyOnly:      r.Bool(),
				Topics:          decodeTopics(),
			}
			r.Tags(flex)
			return tx
		})
	} else {
		tx := AddPartitionsToTxnTransaction{
			TransactionalID: r.String(flex),
			ProducerID:      r.Int64(),
			ProducerEpoch:   r.Int16(),
			Topics:          decodeTopics(),
		}
		m.Transactions = []AddPartitionsToTxnTransaction{tx}
	}
	r.Tags(flex)
}

// Encode implements Message.
func (m *AddPartitionsToTxnRequest) Encode(w *Writer, v int16) {
	flex := v >= 3
	encodeTopics := func(topics []AddPartitionsToTxnTopic) {
		w.ArrayLen(flex, len(topics))
		for _, t := range topics {
			w.String(flex, t.Name)
			w.ArrayLen(flex, len(t.Partitions))
			for _, p := range t.Partitions {
				w.Int32(p)
			}
			w.Tags(flex)
		}
	}
	if v >= 4 {
		w.ArrayLen(flex, len(m.Transactions))
		for _, tx := range m.Transactions {
			w.String(flex, tx.TransactionalID)
			w.Int64(tx.ProducerID)
			w.Int16(tx.ProducerEpoch)
			w.Bool(tx.VerifyOnly)
			encodeTopics(tx.Topics)
			w.Tags(flex)
		}
	} else {
		var tx AddPartitionsToTxnTransaction
		if len(m.Transactions) > 0 {
			tx = m.Transactions[0]
		}
		w.String(flex, tx.TransactionalID)
		w.Int64(tx.ProducerID)
		w.Int16(tx.ProducerEpoch)
		encodeTopics(tx.Topics)
	}
	w.Tags(flex)
}

// AddPartitionsToTxnPartitionResult is one partition's result in AddPartitionsToTxn.
type AddPartitionsToTxnPartitionResult struct {
	PartitionIndex int32
	ErrorCode      int16
}

// AddPartitionsToTxnTopicResult is one topic's result in AddPartitionsToTxn.
type AddPartitionsToTxnTopicResult struct {
	Name       string
	Partitions []AddPartitionsToTxnPartitionResult
}

// AddPartitionsToTxnResult is one transaction's result in AddPartitionsToTxn (v4+).
type AddPartitionsToTxnResult struct {
	TransactionalID string
	TopicResults    []AddPartitionsToTxnTopicResult
}

// AddPartitionsToTxnResponse is the body of AddPartitionsToTxn response (key 24, v0-5).
type AddPartitionsToTxnResponse struct {
	ErrorCode            int16 // v4+
	ResultsByTransaction []AddPartitionsToTxnResult
}

// Decode implements Message.
func (m *AddPartitionsToTxnResponse) Decode(r *Reader, v int16) {
	flex := v >= 3
	r.Int32() // ThrottleTimeMs
	decodeTopicResults := func() []AddPartitionsToTxnTopicResult {
		return Array(r, flex, func() AddPartitionsToTxnTopicResult {
			tr := AddPartitionsToTxnTopicResult{Name: r.String(flex)}
			tr.Partitions = Array(r, flex, func() AddPartitionsToTxnPartitionResult {
				pr := AddPartitionsToTxnPartitionResult{
					PartitionIndex: r.Int32(),
					ErrorCode:      r.Int16(),
				}
				r.Tags(flex)
				return pr
			})
			r.Tags(flex)
			return tr
		})
	}
	if v >= 4 {
		m.ErrorCode = r.Int16()
		m.ResultsByTransaction = Array(r, flex, func() AddPartitionsToTxnResult {
			res := AddPartitionsToTxnResult{
				TransactionalID: r.String(flex),
				TopicResults:    decodeTopicResults(),
			}
			r.Tags(flex)
			return res
		})
	} else {
		topics := decodeTopicResults()
		if len(topics) > 0 {
			m.ResultsByTransaction = []AddPartitionsToTxnResult{{TopicResults: topics}}
		}
	}
	r.Tags(flex)
}

// Encode implements Message.
func (m *AddPartitionsToTxnResponse) Encode(w *Writer, v int16) {
	flex := v >= 3
	w.Int32(0) // ThrottleTimeMs
	encodeTopicResults := func(topics []AddPartitionsToTxnTopicResult) {
		w.ArrayLen(flex, len(topics))
		for _, tr := range topics {
			w.String(flex, tr.Name)
			w.ArrayLen(flex, len(tr.Partitions))
			for _, pr := range tr.Partitions {
				w.Int32(pr.PartitionIndex)
				w.Int16(pr.ErrorCode)
				w.Tags(flex)
			}
			w.Tags(flex)
		}
	}
	if v >= 4 {
		w.Int16(m.ErrorCode)
		w.ArrayLen(flex, len(m.ResultsByTransaction))
		for _, res := range m.ResultsByTransaction {
			w.String(flex, res.TransactionalID)
			encodeTopicResults(res.TopicResults)
			w.Tags(flex)
		}
	} else {
		var topics []AddPartitionsToTxnTopicResult
		if len(m.ResultsByTransaction) > 0 {
			topics = m.ResultsByTransaction[0].TopicResults
		}
		encodeTopicResults(topics)
	}
	w.Tags(flex)
}

// AddOffsetsToTxnRequest is the body of AddOffsetsToTxn (key 25, v0-4).
type AddOffsetsToTxnRequest struct {
	TransactionalID string
	ProducerID      int64
	ProducerEpoch   int16
	GroupID         string
}

// Decode implements Message.
func (m *AddOffsetsToTxnRequest) Decode(r *Reader, v int16) {
	flex := v >= 3
	m.TransactionalID = r.String(flex)
	m.ProducerID = r.Int64()
	m.ProducerEpoch = r.Int16()
	m.GroupID = r.String(flex)
	r.Tags(flex)
}

// Encode implements Message.
func (m *AddOffsetsToTxnRequest) Encode(w *Writer, v int16) {
	flex := v >= 3
	w.String(flex, m.TransactionalID)
	w.Int64(m.ProducerID)
	w.Int16(m.ProducerEpoch)
	w.String(flex, m.GroupID)
	w.Tags(flex)
}

// AddOffsetsToTxnResponse is the body of AddOffsetsToTxn response (key 25, v0-4).
type AddOffsetsToTxnResponse struct {
	ErrorCode int16
}

// Decode implements Message.
func (m *AddOffsetsToTxnResponse) Decode(r *Reader, v int16) {
	r.Int32() // ThrottleTimeMs
	m.ErrorCode = r.Int16()
	r.Tags(v >= 3)
}

// Encode implements Message.
func (m *AddOffsetsToTxnResponse) Encode(w *Writer, v int16) {
	w.Int32(0)
	w.Int16(m.ErrorCode)
	w.Tags(v >= 3)
}

// EndTxnRequest is the body of EndTxn (key 26, v0-5).
type EndTxnRequest struct {
	TransactionalID string
	ProducerID      int64
	ProducerEpoch   int16
	Committed       bool
}

// Decode implements Message.
func (m *EndTxnRequest) Decode(r *Reader, v int16) {
	flex := v >= 3
	m.TransactionalID = r.String(flex)
	m.ProducerID = r.Int64()
	m.ProducerEpoch = r.Int16()
	m.Committed = r.Bool()
	r.Tags(flex)
}

// Encode implements Message.
func (m *EndTxnRequest) Encode(w *Writer, v int16) {
	flex := v >= 3
	w.String(flex, m.TransactionalID)
	w.Int64(m.ProducerID)
	w.Int16(m.ProducerEpoch)
	w.Bool(m.Committed)
	w.Tags(flex)
}

// EndTxnResponse is the body of EndTxn response (key 26, v0-5).
type EndTxnResponse struct {
	ErrorCode     int16
	ProducerID    int64 // v5+ (KIP-890 Part 2)
	ProducerEpoch int16 // v5+
}

// Decode implements Message.
func (m *EndTxnResponse) Decode(r *Reader, v int16) {
	flex := v >= 3
	r.Int32() // ThrottleTimeMs
	m.ErrorCode = r.Int16()
	m.ProducerID, m.ProducerEpoch = -1, -1
	if v >= 5 {
		m.ProducerID = r.Int64()
		m.ProducerEpoch = r.Int16()
	}
	r.Tags(flex)
}

// Encode implements Message.
func (m *EndTxnResponse) Encode(w *Writer, v int16) {
	flex := v >= 3
	w.Int32(0)
	w.Int16(m.ErrorCode)
	if v >= 5 {
		w.Int64(m.ProducerID)
		w.Int16(m.ProducerEpoch)
	}
	w.Tags(flex)
}

// TxnOffsetCommitPartition is one partition in TxnOffsetCommitRequest.
type TxnOffsetCommitPartition struct {
	Index       int32
	Offset      int64
	LeaderEpoch int32 // v2+
	Metadata    string
	HasMetadata bool
}

// TxnOffsetCommitTopic is one topic in TxnOffsetCommitRequest.
type TxnOffsetCommitTopic struct {
	Name       string
	Partitions []TxnOffsetCommitPartition
}

// TxnOffsetCommitRequest is the body of TxnOffsetCommit (key 28, v0-5).
type TxnOffsetCommitRequest struct {
	TransactionalID string
	GroupID         string
	ProducerID      int64
	ProducerEpoch   int16
	GenerationID    int32 // v3+
	MemberID        string
	GroupInstanceID string
	HasInstanceID   bool
	Topics          []TxnOffsetCommitTopic
}

// Decode implements Message.
func (m *TxnOffsetCommitRequest) Decode(r *Reader, v int16) {
	flex := v >= 3
	m.TransactionalID = r.String(flex)
	m.GroupID = r.String(flex)
	m.ProducerID = r.Int64()
	m.ProducerEpoch = r.Int16()
	m.GenerationID = -1
	if v >= 3 {
		m.GenerationID = r.Int32()
		m.MemberID = r.String(flex)
		id, null := r.NullableString(flex)
		m.GroupInstanceID, m.HasInstanceID = id, !null
	}
	m.Topics = Array(r, flex, func() TxnOffsetCommitTopic {
		t := TxnOffsetCommitTopic{Name: r.String(flex)}
		t.Partitions = Array(r, flex, func() TxnOffsetCommitPartition {
			p := TxnOffsetCommitPartition{
				Index:       r.Int32(),
				Offset:      r.Int64(),
				LeaderEpoch: -1,
			}
			if v >= 2 {
				p.LeaderEpoch = r.Int32()
			}
			meta, null := r.NullableString(flex)
			p.Metadata, p.HasMetadata = meta, !null
			r.Tags(flex)
			return p
		})
		r.Tags(flex)
		return t
	})
	r.Tags(flex)
}

// Encode implements Message.
func (m *TxnOffsetCommitRequest) Encode(w *Writer, v int16) {
	flex := v >= 3
	w.String(flex, m.TransactionalID)
	w.String(flex, m.GroupID)
	w.Int64(m.ProducerID)
	w.Int16(m.ProducerEpoch)
	if v >= 3 {
		w.Int32(m.GenerationID)
		w.String(flex, m.MemberID)
		if m.HasInstanceID {
			w.String(flex, m.GroupInstanceID)
		} else {
			w.NullString(flex)
		}
	}
	w.ArrayLen(flex, len(m.Topics))
	for _, t := range m.Topics {
		w.String(flex, t.Name)
		w.ArrayLen(flex, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Int64(p.Offset)
			if v >= 2 {
				w.Int32(p.LeaderEpoch)
			}
			if p.HasMetadata {
				w.String(flex, p.Metadata)
			} else {
				w.NullString(flex)
			}
			w.Tags(flex)
		}
		w.Tags(flex)
	}
	w.Tags(flex)
}

// TxnOffsetCommitPartitionResponse is one partition's result in TxnOffsetCommitResponse.
type TxnOffsetCommitPartitionResponse struct {
	Index     int32
	ErrorCode int16
}

// TxnOffsetCommitTopicResponse is one topic's result in TxnOffsetCommitResponse.
type TxnOffsetCommitTopicResponse struct {
	Name       string
	Partitions []TxnOffsetCommitPartitionResponse
}

// TxnOffsetCommitResponse is the body of TxnOffsetCommit response (key 28, v0-5).
type TxnOffsetCommitResponse struct {
	Topics []TxnOffsetCommitTopicResponse
}

// Decode implements Message.
func (m *TxnOffsetCommitResponse) Decode(r *Reader, v int16) {
	flex := v >= 3
	r.Int32() // ThrottleTimeMs
	m.Topics = Array(r, flex, func() TxnOffsetCommitTopicResponse {
		t := TxnOffsetCommitTopicResponse{Name: r.String(flex)}
		t.Partitions = Array(r, flex, func() TxnOffsetCommitPartitionResponse {
			p := TxnOffsetCommitPartitionResponse{Index: r.Int32(), ErrorCode: r.Int16()}
			r.Tags(flex)
			return p
		})
		r.Tags(flex)
		return t
	})
	r.Tags(flex)
}

// Encode implements Message.
func (m *TxnOffsetCommitResponse) Encode(w *Writer, v int16) {
	flex := v >= 3
	w.Int32(0)
	w.ArrayLen(flex, len(m.Topics))
	for _, t := range m.Topics {
		w.String(flex, t.Name)
		w.ArrayLen(flex, len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.Index)
			w.Int16(p.ErrorCode)
			w.Tags(flex)
		}
		w.Tags(flex)
	}
	w.Tags(flex)
}
