package wire

// Voter changes of KIP-853 (docs/design/c1-membership.md). Fields follow
// AddRaftVoter*.json (versions 0-1) and RemoveRaftVoter*.json (version 0),
// trunk, read 2026-11-14; both flexible from version 0.

// Listener is one endpoint of a voter.
type Listener struct {
	Name string
	Host string
	Port uint16
}

// AddRaftVoterRequest asks the leader to add a voter.
type AddRaftVoterRequest struct {
	ClusterID        string // null when empty
	TimeoutMs        int32
	VoterID          int32
	VoterDirectoryID UUID
	Listeners        []Listener
	AckWhenCommitted bool // version 1; true in version 0
}

// Decode implements Message.
func (m *AddRaftVoterRequest) Decode(r *Reader, v int16) {
	m.ClusterID, _ = r.NullableString(true)
	m.TimeoutMs = r.Int32()
	m.VoterID = r.Int32()
	m.VoterDirectoryID = r.uuid()
	m.Listeners = Array(r, true, func() Listener {
		l := Listener{Name: r.String(true), Host: r.String(true), Port: uint16(r.Int16())}
		r.Tags(true)
		return l
	})
	m.AckWhenCommitted = true
	if v >= 1 {
		m.AckWhenCommitted = r.Bool()
	}
	r.Tags(true)
}

// Encode implements Message.
func (m *AddRaftVoterRequest) Encode(w *Writer, v int16) {
	w.optString(true, m.ClusterID)
	w.Int32(m.TimeoutMs)
	w.Int32(m.VoterID)
	w.uuid(m.VoterDirectoryID)
	w.ArrayLen(true, len(m.Listeners))
	for _, l := range m.Listeners {
		w.String(true, l.Name)
		w.String(true, l.Host)
		w.Int16(int16(l.Port))
		w.Tags(true)
	}
	if v >= 1 {
		w.Bool(m.AckWhenCommitted)
	}
	w.Tags(true)
}

// VoterChangeResponse is the body of an AddRaftVoter or RemoveRaftVoter
// response: the two are the same.
type VoterChangeResponse struct {
	ErrorCode    int16
	ErrorMessage string // null when empty
}

// Decode implements Message.
func (m *VoterChangeResponse) Decode(r *Reader, _ int16) {
	r.Int32() // ThrottleTimeMs
	m.ErrorCode = r.Int16()
	m.ErrorMessage, _ = r.NullableString(true)
	r.Tags(true)
}

// Encode implements Message.
func (m *VoterChangeResponse) Encode(w *Writer, _ int16) {
	w.Int32(0)
	w.Int16(m.ErrorCode)
	w.optString(true, m.ErrorMessage)
	w.Tags(true)
}

// RemoveRaftVoterRequest asks the leader to remove a voter.
type RemoveRaftVoterRequest struct {
	ClusterID        string // null when empty
	VoterID          int32
	VoterDirectoryID UUID
}

// Decode implements Message.
func (m *RemoveRaftVoterRequest) Decode(r *Reader, _ int16) {
	m.ClusterID, _ = r.NullableString(true)
	m.VoterID = r.Int32()
	m.VoterDirectoryID = r.uuid()
	r.Tags(true)
}

// Encode implements Message.
func (m *RemoveRaftVoterRequest) Encode(w *Writer, _ int16) {
	w.optString(true, m.ClusterID)
	w.Int32(m.VoterID)
	w.uuid(m.VoterDirectoryID)
	w.Tags(true)
}
