package raft

import (
	"encoding/binary"
	"errors"
)

// Message types. Every message starts with a version byte, its type, and
// the sender's term.
const (
	msgVersion byte = 1

	msgRequestVote byte = iota
	msgVoteResponse
	msgAppendEntries
	msgAppendResponse
	msgInstallSnapshot
	msgInstallResponse
	// PreVote asks "would you vote for me in term?" and changes nothing at
	// the receiver; its fields are RequestVote's (docs/design/b3-raft-hardening.md).
	msgPreVote
	msgPreVoteResponse
	// TimeoutNow tells a follower to stand for election at once: the leader
	// is handing over.
	msgTimeoutNow
	// TransferVote is a RequestVote sent because of a TimeoutNow; receivers
	// do not apply the lease rule to it.
	msgTransferVote
)

// Entry kinds.
const (
	kindCommand byte = 0
	kindNoop    byte = 1 // appended by a new leader; carries nothing
)

// entryHeader is what precedes a command in a stored entry: term and kind.
const entryHeader = 8 + 1

// entry is one log entry as it travels in AppendEntries.
type entry struct {
	term int64
	kind byte
	cmd  []byte
}

// message is the union of the four Raft messages. Which fields mean
// anything depends on typ.
type message struct {
	typ  byte
	term int64

	// RequestVote: the candidate's last entry.
	lastIndex, lastTerm int64

	// VoteResponse: granted. AppendResponse: success.
	ok bool

	// AppendEntries: the entry before the new ones, the leader's commit
	// index, and the new entries.
	prevIndex, prevTerm, commit int64
	entries                     []entry

	// AppendEntries: the leader's clock when it sent the message.
	// AppendResponse: the stamp of the request answered (the lease).
	stamp int64

	// AppendResponse. echo is the prevIndex of the request answered. On
	// success, index is the highest index the follower knows to match the
	// leader's log and holds durably. On refusal it is a hint: the leader
	// should try again from the entry after it.
	// InstallResponse: index is the follower's last index.
	echo, index int64

	// InstallSnapshot: the leader's log start (the follower's log will
	// start there) with the term of the entry before it (in prevTerm), the
	// snapshot's index (in snapIndex), its total size,
	// and one chunk of it at offset. InstallResponse: offset is the next
	// byte the follower expects, or -1 if it needs no snapshot.
	cut, snapIndex, total, offset int64
	data                          []byte
}

var errBadMessage = errors.New("raft: malformed message")

func (m *message) encode() []byte {
	b := []byte{msgVersion, m.typ}
	i64 := func(v int64) { b = binary.BigEndian.AppendUint64(b, uint64(v)) }
	flag := func(v bool) {
		if v {
			b = append(b, 1)
		} else {
			b = append(b, 0)
		}
	}
	i64(m.term)
	switch m.typ {
	case msgRequestVote, msgPreVote, msgTransferVote:
		i64(m.lastIndex)
		i64(m.lastTerm)
	case msgVoteResponse, msgPreVoteResponse:
		flag(m.ok)
	case msgAppendEntries:
		i64(m.prevIndex)
		i64(m.prevTerm)
		i64(m.commit)
		i64(m.stamp)
		b = binary.BigEndian.AppendUint32(b, uint32(len(m.entries)))
		for _, e := range m.entries {
			i64(e.term)
			b = append(b, e.kind)
			b = binary.BigEndian.AppendUint32(b, uint32(len(e.cmd)))
			b = append(b, e.cmd...)
		}
	case msgAppendResponse:
		flag(m.ok)
		i64(m.echo)
		i64(m.index)
		i64(m.stamp)
	case msgInstallSnapshot:
		i64(m.stamp)
		i64(m.cut)
		i64(m.prevTerm)
		i64(m.snapIndex)
		i64(m.total)
		i64(m.offset)
		b = binary.BigEndian.AppendUint32(b, uint32(len(m.data)))
		b = append(b, m.data...)
	case msgInstallResponse:
		i64(m.stamp)
		i64(m.offset)
		i64(m.index)
	}
	return b
}

// decodeMessage parses a message from a peer. Peers are not trusted to be
// correct: every length is checked against the bytes present before
// anything is allocated, and maxCmd bounds one command. Entries alias b.
func decodeMessage(b []byte, maxCmd int) (message, error) {
	var m message
	bad := false
	take := func(n int) []byte {
		if bad || n < 0 || n > len(b) {
			bad = true
			return make([]byte, 8)[:min(max(n, 0), 8)] // zeros: callers need not check
		}
		p := b[:n]
		b = b[n:]
		return p
	}
	i64 := func() int64 { return int64(binary.BigEndian.Uint64(take(8))) }
	flag := func() bool {
		v := take(1)[0]
		bad = bad || v > 1
		return v == 1
	}
	if v := take(1)[0]; v != msgVersion {
		return m, errBadMessage
	}
	m.typ = take(1)[0]
	m.term = i64()
	switch m.typ {
	case msgRequestVote, msgPreVote, msgTransferVote:
		m.lastIndex, m.lastTerm = i64(), i64()
		bad = bad || m.lastIndex < -1 || m.lastTerm < 0
	case msgVoteResponse, msgPreVoteResponse:
		m.ok = flag()
	case msgTimeoutNow:
	case msgAppendEntries:
		m.prevIndex, m.prevTerm, m.commit, m.stamp = i64(), i64(), i64(), i64()
		n := int(binary.BigEndian.Uint32(take(4)))
		// An entry takes at least 13 bytes, which bounds n by the input.
		bad = bad || m.prevIndex < -1 || m.prevTerm < 0 || m.commit < -1 || n > len(b)/13
		for ; n > 0 && !bad; n-- {
			e := entry{term: i64(), kind: take(1)[0]}
			size := int(binary.BigEndian.Uint32(take(4)))
			bad = bad || size > maxCmd || e.kind > kindNoop || e.term < 0 || e.term > m.term
			if !bad {
				e.cmd = take(size)
				m.entries = append(m.entries, e)
			}
		}
	case msgAppendResponse:
		m.ok = flag()
		m.echo, m.index, m.stamp = i64(), i64(), i64()
		bad = bad || m.echo < -1 || m.index < -1
	case msgInstallSnapshot:
		m.stamp, m.cut, m.prevTerm, m.snapIndex, m.total, m.offset = i64(), i64(), i64(), i64(), i64(), i64()
		n := int(binary.BigEndian.Uint32(take(4)))
		bad = bad || n > maxCmd || m.cut < 1 || m.prevTerm < 0 || m.prevTerm > m.term || m.snapIndex < m.cut-1 || m.offset < 0 || m.total < 1 || m.offset > m.total-int64(n)
		m.data = take(n)
	case msgInstallResponse:
		m.stamp, m.offset, m.index = i64(), i64(), i64()
		bad = bad || m.offset < -1 || m.index < -1
	default:
		bad = true
	}
	if bad || len(b) != 0 || m.term < 0 {
		return message{}, errBadMessage
	}
	return m, nil
}
