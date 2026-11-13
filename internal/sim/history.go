// Package sim is the deterministic simulator: it runs a broker node on a
// simulated disk, clock and network, drives it with clients that speak real
// Kafka wire bytes, injects faults, and checks what the clients observed
// against a model of what they were promised. Everything is a function of
// one seed. See docs/design/phase-3.md.
package sim

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/amritangshudey/samsa/internal/wire"
)

// TP names one partition of one topic.
type TP struct {
	Topic string
	Part  int32
}

type recState uint8

const (
	recPending  recState = iota // sent, no answer yet
	recAcked                    // the broker reported success
	recRejected                 // the broker reported an error
	recUnknown                  // the connection died first, or no answer was asked for
)

// rec is the model's knowledge of one record a client sent.
type rec struct {
	tp       TP
	first    uint64 // id of the first record of its batch
	n        int    // records in its batch
	state    recState
	offset   int64 // where it was acknowledged or observed; -1 if neither
	ackEpoch int   // model epoch when it was acknowledged
	lost     bool  // seen, then removed by a permitted power-loss truncation
	final    int   // last final pass that observed it
}

// slot is what the model believes one offset of a partition holds.
type slot struct {
	id    uint64 // 0: nothing known
	epoch int    // model epoch when last confirmed
}

type partModel struct {
	at    []slot // by offset
	floor int64  // a read that starts now must report a log end of at least this
	start int64  // the highest log start offset a broker reported
}

type topicState uint8

const (
	topicNever     topicState = iota // nobody tried to create it
	topicAttempted                   // a create was sent; outcome not known
	topicCreated                     // a create was acknowledged
)

type topicModel struct {
	state topicState
	// initial is the count it is created with; parts is the most it can
	// have, raised by every CreatePartitions sent; known is how many a
	// request sent now must find once it is created.
	initial, parts, known int32
	// deleting is set once a DeleteTopics was sent, deleted once one was
	// acknowledged: from then on no request sent may find it (D1).
	deleting, deleted bool
}

// Model is the checker. Clients report what they sent and what they were
// told; the model returns an error at the first report that no correct
// broker could have produced. It knows nothing about the simulator, so it
// can judge a history from any source.
//
// Strict means an acknowledged write must survive anything. Otherwise a
// power loss may remove a suffix of each partition, and nothing else.
//
// Causality: a response can only be held to what was known when its request
// was sent, so requests carry the Floor and Epoch read at send time.
type Model struct {
	strict    bool
	epoch     int // number of power losses so far
	recs      []rec
	parts     map[TP]*partModel
	topics    map[string]*topicModel
	finalPass int
	trimming  bool
	rebatched bool
	groups    groupModel
}

// NewModel returns an empty model.
func NewModel(strict bool) *Model {
	return &Model{strict: strict, recs: make([]rec, 1), parts: map[TP]*partModel{}, topics: map[string]*topicModel{}, groups: newGroupModel()}
}

func (m *Model) part(tp TP) *partModel {
	p := m.parts[tp]
	if p == nil {
		p = &partModel{}
		m.parts[tp] = p
	}
	return p
}

// Epoch returns the number of power losses so far.
func (m *Model) Epoch() int { return m.epoch }

// stale reports whether a response to a request sent in the given epoch
// says nothing about the present: only when acknowledged data may be lost.
func (m *Model) stale(epoch int) bool { return !m.strict && epoch != m.epoch }

// PowerLoss records that the broker lost power. Under a policy that permits
// loss, everything the model expected of the log's end is forgotten.
func (m *Model) PowerLoss() {
	m.epoch++
	if m.strict {
		return
	}
	m.groups.forget()
	for _, p := range m.parts { // order-independent: every entry gets the same update
		p.floor, p.start = 0, 0 // a lost Trim may come back as a lower start
	}
	for _, t := range m.topics {
		if t.state == topicCreated {
			t.state = topicAttempted
		}
		t.known, t.deleted = t.initial, false // a growth or a delete may be lost too
	}
}

// TopicAttempted records that a client asked for a topic to be created.
func (m *Model) TopicAttempted(name string, parts int32) {
	if m.topics[name] == nil {
		m.topics[name] = &topicModel{state: topicAttempted, initial: parts, parts: parts}
	}
}

// TopicCreated records that the broker confirmed the topic exists, in
// answer to a request sent in the given epoch.
func (m *Model) TopicCreated(name string, epoch int) {
	if t := m.topics[name]; t != nil && !m.stale(epoch) {
		t.state = topicCreated
		t.known = max(t.known, t.initial)
	}
}

// Known reports whether a request sent now must find tp.
func (m *Model) Known(tp TP) bool {
	t := m.topics[tp.Topic]
	return t != nil && t.state == topicCreated && !t.deleting && tp.Part >= 0 && tp.Part < t.known
}

// possible reports whether tp can exist at all.
func (m *Model) possible(tp TP) bool {
	t := m.topics[tp.Topic]
	return t != nil && tp.Part >= 0 && tp.Part < t.parts
}

// Floor returns the log end that a read of tp starting now must at least
// report.
func (m *Model) Floor(tp TP) int64 { return m.part(tp).floor }

// Send registers one batch of n records about to be sent to tp and returns
// the id of the first; the others follow consecutively.
func (m *Model) Send(tp TP, n int) uint64 {
	first := uint64(len(m.recs))
	for range n {
		m.recs = append(m.recs, rec{tp: tp, first: first, n: n, offset: -1})
	}
	return first
}

// Value returns the payload of the record with the given id: the id, then
// padding derived from it, so that a damaged or foreign record is noticed.
func Value(id uint64) []byte {
	v := make([]byte, 8+id*2654435761%97)
	binary.BigEndian.PutUint64(v, id)
	for i := 8; i < len(v); i++ {
		v[i] = byte(id) + byte(i)
	}
	return v
}

// Rejected records that the broker answered the n records from first with
// an error.
func (m *Model) Rejected(first uint64, n int) error {
	for id := first; id < first+uint64(n); id++ {
		r := &m.recs[id]
		if r.offset >= 0 {
			return fmt.Errorf("I5: record %d was rejected but had been read at %v offset %d", id, r.tp, r.offset)
		}
		r.state = recRejected
	}
	return nil
}

// Unknown records that the client will never learn what happened to the n
// records from first.
func (m *Model) Unknown(first uint64, n int) {
	for id := first; id < first+uint64(n); id++ {
		m.recs[id].state = recUnknown
	}
}

// Acked records that the broker acknowledged the n records from first, all
// sent to one partition in one request sent in the given epoch, at
// consecutive offsets from base.
func (m *Model) Acked(first uint64, n int, base int64, epoch int) error {
	if m.stale(epoch) {
		m.Unknown(first, n) // acknowledged before a power loss that may have taken it
		return nil
	}
	if base < 0 {
		return fmt.Errorf("I2: records from %d acknowledged at negative offset %d", first, base)
	}
	for i := range n {
		id := first + uint64(i)
		if err := m.place(id, base+int64(i)); err != nil {
			return err
		}
		m.recs[id].state, m.recs[id].ackEpoch = recAcked, m.epoch
	}
	p := m.part(m.recs[first].tp)
	p.floor = max(p.floor, base+int64(n))
	return nil
}

// place records that the record id sits at offset off of its partition, and
// fails if that contradicts anything seen before.
func (m *Model) place(id uint64, off int64) error {
	r := &m.recs[id]
	if r.lost {
		return fmt.Errorf("I3: record %d reappeared at %v offset %d after a power loss had removed it", id, r.tp, off)
	}
	if r.offset >= 0 && r.offset != off {
		return fmt.Errorf("I1/I4: record %d is at %v offset %d but was acknowledged or read at offset %d", id, r.tp, off, r.offset)
	}
	p := m.part(r.tp)
	for int64(len(p.at)) <= off {
		p.at = append(p.at, slot{})
	}
	if old := p.at[off]; old.id != 0 && old.id != id {
		if m.strict || old.epoch == m.epoch {
			return fmt.Errorf("I1/I3: %v offset %d holds record %d but held record %d", r.tp, off, id, old.id)
		}
		// Some power loss after old.epoch cut the log at or before off.
		// Whatever was confirmed at or after off no later than old.epoch
		// went with it. Later confirmations may postdate the cut, so they
		// stay until a read contradicts them too.
		for o := off; o < int64(len(p.at)); o++ {
			if s := p.at[o]; s.id != 0 && s.epoch <= old.epoch {
				m.recs[s.id].lost, m.recs[s.id].offset = true, -1
				p.at[o] = slot{}
			}
		}
	}
	p.at[off] = slot{id: id, epoch: m.epoch}
	r.offset, r.final = off, m.finalPass
	return nil
}

// Fetch is one partition of a fetch response, with what was known when the
// request was sent.
type Fetch struct {
	TP     TP
	Offset int64 // offset asked for
	Floor  int64 // Model.Floor(TP) when the request was sent
	Epoch  int   // Model.Epoch() when the request was sent
	Known  bool  // Model.Known(TP) when the request was sent
	// Deleted is Model.Deleted(TP.Topic) when the request was sent.
	Deleted bool
	Start   int64 // Model.Start(TP) when the request was sent
	// MustProgress is set when no earlier partition of the same response
	// carried records: then the broker must return at least one batch if
	// there is one, whatever the byte limits.
	MustProgress bool

	Err      int16
	HW       int64
	LogStart int64 // -1 below Fetch v5, which does not carry it
	Records  []byte
}

// Start returns the highest log start offset reported for tp: retention
// may raise it, and nothing may lower it (docs/design/a1-snapshots.md, 3.4).
func (m *Model) Start(tp TP) int64 { return m.part(tp).start }

// Rebatched tells the model that its clients form their own batches, as
// a real client does (docs/design/a4-chaos.md): records registered one by
// one may be read back several to a batch, in any mix of ids. Each record
// is then still checked for where it sits, but not the batch's shape.
func (m *Model) Rebatched() { m.rebatched = true }

// Trimming tells the model the broker may delete old records. Without a
// log start in the answer (Fetch below v5), an out-of-range answer below
// the end is then believed. shortcut: unchecked for those versions.
func (m *Model) Trimming() { m.trimming = true }

// Fetched checks one partition of a fetch response. It returns the offset
// after the last record returned, or -1 if there were none.
func (m *Model) Fetched(f Fetch) (next int64, err error) {
	if m.stale(f.Epoch) {
		return -1, nil
	}
	if f.Deleted && f.Err != wire.ErrUnknownTopicOrPartition {
		return -1, fmt.Errorf("D1: %v was deleted, but a fetch sent afterwards was answered with code %d", f.TP, f.Err)
	}
	switch f.Err {
	case wire.ErrNone:
	case wire.ErrUnknownTopicOrPartition:
		if f.Known {
			return -1, fmt.Errorf("I1: %v was created but the broker does not know it", f.TP)
		}
		return -1, nil
	}
	if f.LogStart >= 0 && (f.Err == wire.ErrNone || f.Err == wire.ErrOffsetOutOfRange) {
		switch {
		case f.LogStart < f.Start:
			return -1, fmt.Errorf("R1: %v log start went back from %d to %d", f.TP, f.Start, f.LogStart)
		case f.LogStart > f.HW:
			return -1, fmt.Errorf("R1: %v log start %d is past the high watermark %d", f.TP, f.LogStart, f.HW)
		case f.Err == wire.ErrNone && f.Offset < f.LogStart:
			return -1, fmt.Errorf("R1: %v fetch at %d succeeded below the log start %d", f.TP, f.Offset, f.LogStart)
		}
		p := m.part(f.TP)
		p.start = max(p.start, f.LogStart)
	}
	switch f.Err {
	case wire.ErrNone:
	case wire.ErrOffsetOutOfRange:
		below := f.Offset < f.LogStart || f.LogStart < 0 && m.trimming
		if f.Offset >= 0 && f.Offset <= f.Floor && !below {
			return -1, fmt.Errorf("I1: %v offset %d reported out of range, but the log end was already %d", f.TP, f.Offset, f.Floor)
		}
		return -1, nil
	default:
		return -1, fmt.Errorf("fetch of %v failed with unexpected error code %d", f.TP, f.Err)
	}
	if !m.possible(f.TP) {
		return -1, fmt.Errorf("I4: fetch of %v succeeded, but nobody created it", f.TP)
	}
	if f.HW < f.Floor {
		return -1, fmt.Errorf("I1/I3: %v high watermark %d is below %d, which was acknowledged or seen before the request", f.TP, f.HW, f.Floor)
	}
	if f.Offset < 0 || f.Offset > f.HW {
		return -1, fmt.Errorf("I2: %v fetch at offset %d succeeded with high watermark %d", f.TP, f.Offset, f.HW)
	}
	next = -1
	for rest := f.Records; len(rest) > 0; {
		info, code := wire.ParseBatch(rest, len(rest))
		if code != wire.ErrNone {
			return -1, fmt.Errorf("I4: %v returned a malformed batch (code %d)", f.TP, code)
		}
		base, records, err := wire.DecodeBatch(rest[:info.Size])
		if err != nil {
			return -1, fmt.Errorf("I4: %v returned an undecodable batch: %w", f.TP, err)
		}
		end := base + int64(len(records))
		switch {
		case next == -1 && (base > f.Offset || end <= f.Offset):
			return -1, fmt.Errorf("I2: %v fetch at %d began with a batch covering %d..%d", f.TP, f.Offset, base, end-1)
		case next != -1 && base != next:
			return -1, fmt.Errorf("I2: %v batch at %d follows a batch ending at %d", f.TP, base, next-1)
		}
		if err := m.observe(f.TP, base, records); err != nil {
			return -1, err
		}
		next, rest = end, rest[info.Size:]
	}
	if next > f.HW {
		return -1, fmt.Errorf("I2: %v returned records up to %d beyond its high watermark %d", f.TP, next-1, f.HW)
	}
	if next == -1 && f.MustProgress && f.Offset < f.HW {
		return -1, fmt.Errorf("I2: %v fetch at %d returned nothing although the high watermark is %d", f.TP, f.Offset, f.HW)
	}
	p := m.part(f.TP)
	p.floor = max(p.floor, f.HW)
	return next, nil
}

// observe checks one batch read from tp at base against what was sent.
func (m *Model) observe(tp TP, base int64, records []wire.Record) error {
	var first uint64
	for i, got := range records {
		if len(got.Value) < 8 {
			return fmt.Errorf("I4: %v offset %d holds a record nobody sent", tp, base+int64(i))
		}
		id := binary.BigEndian.Uint64(got.Value)
		if id == 0 || id >= uint64(len(m.recs)) || got.Key != nil || !bytes.Equal(got.Value, Value(id)) {
			return fmt.Errorf("I4: %v offset %d holds a record nobody sent, or a damaged one", tp, base+int64(i))
		}
		r := &m.recs[id]
		if i == 0 && !m.rebatched {
			first = id
			if r.first != id || r.n != len(records) {
				return fmt.Errorf("I4: %v offset %d: batch of %d starting at record %d was sent as a batch of %d starting at record %d",
					tp, base, len(records), id, r.n, r.first)
			}
		}
		switch {
		case !m.rebatched && id != first+uint64(i):
			return fmt.Errorf("I4: %v offset %d: record %d is out of place in its batch", tp, base+int64(i), id)
		case r.tp != tp:
			return fmt.Errorf("I4: record %d was sent to %v but read from %v", id, r.tp, tp)
		case r.state == recRejected:
			return fmt.Errorf("I5: record %d was rejected but is readable at %v offset %d", id, tp, base+int64(i))
		}
		if err := m.place(id, base+int64(i)); err != nil {
			return err
		}
	}
	return nil
}

// BeginFinal starts a final pass: a read of every partition from offset 0,
// made when no more faults will happen.
func (m *Model) BeginFinal() { m.finalPass++ }

// CheckFinal fails if the final pass did not read a record that had to
// survive: every acknowledged one if strict, otherwise every one
// acknowledged since the last power loss.
func (m *Model) CheckFinal() error {
	for id := 1; id < len(m.recs); id++ {
		r := &m.recs[id]
		// Retention may drop what lies below the log start, nothing else;
		// a deleted topic takes everything with it.
		if t := m.topics[r.tp.Topic]; t != nil && t.deleting {
			continue
		}
		if r.state == recAcked && (m.strict || r.ackEpoch == m.epoch) && r.final != m.finalPass && r.offset >= m.part(r.tp).start {
			return fmt.Errorf("I1: record %d was acknowledged at %v offset %d and is gone", id, r.tp, r.offset)
		}
	}
	return nil
}

// PartitionsAttempted records that a client asked for a topic to grow to
// count partitions.
func (m *Model) PartitionsAttempted(name string, count int32) {
	if t := m.topics[name]; t != nil {
		t.parts = max(t.parts, count)
	}
}

// PartitionsCreated records that the broker confirmed the topic has at
// least count partitions, in answer to a request sent in the given epoch.
func (m *Model) PartitionsCreated(name string, count int32, epoch int) {
	if t := m.topics[name]; t != nil && !m.stale(epoch) {
		t.known = max(t.known, count)
	}
}

// PartitionRange returns the fewest and most partitions name can have.
func (m *Model) PartitionRange(name string) (least, most int32) {
	if t := m.topics[name]; t != nil {
		return t.initial, t.parts
	}
	return 0, 0
}

// RecordsDeleted records that DeleteRecords at offset (-1: the high
// watermark) of tp, sent in the given epoch when Floor(tp) was floor and
// Start(tp) was start, was answered with low watermark lw. The log start
// may never go back below it (R1).
func (m *Model) RecordsDeleted(tp TP, offset, lw, floor, start int64, epoch int) error {
	if m.stale(epoch) {
		return nil
	}
	p := m.part(tp)
	switch {
	case lw < offset || lw < start:
		return fmt.Errorf("R1: DeleteRecords of %v at %d answered low watermark %d; the start was already %d", tp, offset, lw, start)
	case offset == -1 && lw < floor:
		return fmt.Errorf("I1: DeleteRecords of %v at the high watermark answered %d, below %d, which was acknowledged or seen before the request", tp, lw, floor)
	}
	p.start, p.floor = max(p.start, lw), max(p.floor, lw)
	return nil
}

// TopicDeleteAttempted records that a client asked for a topic's deletion.
func (m *Model) TopicDeleteAttempted(name string) {
	if t := m.topics[name]; t != nil {
		t.deleting = true
	}
}

// TopicDeleted records that the broker confirmed the deletion, in answer to
// a request sent in the given epoch.
func (m *Model) TopicDeleted(name string, epoch int) {
	if t := m.topics[name]; t != nil && !m.stale(epoch) {
		t.deleted = true
	}
}

// Deleted reports whether a request sent now must not find the topic.
func (m *Model) Deleted(name string) bool {
	t := m.topics[name]
	return t != nil && t.deleted
}
