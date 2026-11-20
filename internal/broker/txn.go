package broker

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/amritangshudey/samsa/internal/wire"
)

const (
	resTxnID             int8  = 5
	maxTxnTimeoutMs      int32 = 900_000
	defaultTxnTimeoutMs  int32 = 60_000
	txnCoordinatorLookup int8  = 1
)

type tpName struct {
	topic string
	part  int32
}

func sortedTPNames[V any](m map[tpName]V) []tpName {
	keys := make([]tpName, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b tpName) int {
		if c := cmp.Compare(a.topic, b.topic); c != 0 {
			return c
		}
		return cmp.Compare(a.part, b.part)
	})
	return keys
}

type txnOpen struct {
	epoch       int16
	firstOffset int64
}

type abortedTxn struct {
	pid         int64
	firstOffset int64
	lastOffset  int64
}

func (p *partition) lso() int64 {
	lso := p.next
	for _, ot := range p.openTxns {
		if ot.firstOffset < lso {
			lso = ot.firstOffset
		}
	}
	return lso
}

func (p *partition) trimAborted() {
	if len(p.aborted) == 0 {
		return
	}
	kept := p.aborted[:0]
	for _, a := range p.aborted {
		if a.lastOffset >= p.start {
			kept = append(kept, a)
		}
	}
	p.aborted = kept
}

func (p *partition) abortedInRange(from, to int64) []wire.AbortedTransaction {
	if len(p.aborted) == 0 || to <= from {
		return nil
	}
	var out []wire.AbortedTransaction
	for _, a := range p.aborted {
		if a.lastOffset >= from && a.firstOffset < to {
			out = append(out, wire.AbortedTransaction{ProducerID: a.pid, FirstOffset: a.firstOffset})
		}
	}
	return out
}

func (p *partition) isControlOffset(off int64) bool {
	i := sort.Search(len(p.batches), func(i int) bool { return p.batches[i].base > off }) - 1
	if i < 0 || i >= len(p.batches) {
		return false
	}
	return p.batches[i].base == off && p.batches[i].control != 0
}

type txnPhase uint8

const (
	txnEmpty txnPhase = iota
	txnOngoing
	txnCompleteCommit
	txnCompleteAbort
)

type txnState struct {
	id             string
	pid            int64
	epoch          int16
	prevPID        int64
	prevEpoch      int16
	phase          txnPhase
	timeoutMs      int32
	startedMs      int64
	parts          []tpName
	groups         []string
	pendingOffsets map[string]map[tpName]committedOffset
}

func (tx *txnState) addPart(topic string, part int32) {
	tp := tpName{topic: topic, part: part}
	if !slices.Contains(tx.parts, tp) {
		tx.parts = append(tx.parts, tp)
	}
}

func (tx *txnState) addGroup(group string) {
	if !slices.Contains(tx.groups, group) {
		tx.groups = append(tx.groups, group)
	}
}

func (s *state) txnByPID(pid int64) *txnState {
	for _, id := range sortedKeys(s.txns) {
		if tx := s.txns[id]; tx.pid == pid {
			return tx
		}
	}
	return nil
}

// --- Commands: txnInit, txnAddParts, txnAddOffsets, txnOffsetCommit, txnEnd ---

type txnInitCmd struct {
	id        string
	timeoutMs int32
	pid       int64
	epoch     int16
	nowMs     int64
}

func encodeTxnInit(c txnInitCmd) []byte {
	w := wire.Writer{B: []byte{cmdTxnInit}}
	w.String(false, c.id)
	w.Int32(c.timeoutMs)
	w.Int64(c.pid)
	w.Int16(c.epoch)
	w.Int64(c.nowMs)
	return w.B
}

func decodeTxnInit(r *wire.Reader) (txnInitCmd, bool) {
	c := txnInitCmd{
		id:        r.String(false),
		timeoutMs: r.Int32(),
		pid:       r.Int64(),
		epoch:     r.Int16(),
		nowMs:     r.Int64(),
	}
	return c, r.Err() == nil && r.Len() == 0
}

type txnAddPartsCmd struct {
	id         string
	pid        int64
	epoch      int16
	verifyOnly bool
	nowMs      int64
	parts      []tpName
}

func encodeTxnAddParts(c txnAddPartsCmd) []byte {
	w := wire.Writer{B: []byte{cmdTxnAddParts}}
	w.String(false, c.id)
	w.Int64(c.pid)
	w.Int16(c.epoch)
	w.Bool(c.verifyOnly)
	w.Int64(c.nowMs)
	w.ArrayLen(false, len(c.parts))
	for _, p := range c.parts {
		w.String(false, p.topic)
		w.Int32(p.part)
	}
	return w.B
}

func decodeTxnAddParts(r *wire.Reader) (txnAddPartsCmd, bool) {
	c := txnAddPartsCmd{
		id:         r.String(false),
		pid:        r.Int64(),
		epoch:      r.Int16(),
		verifyOnly: r.Bool(),
		nowMs:      r.Int64(),
	}
	c.parts = wire.Array(r, false, func() tpName {
		return tpName{topic: r.String(false), part: r.Int32()}
	})
	return c, r.Err() == nil && r.Len() == 0
}

type txnAddOffsetsCmd struct {
	id    string
	pid   int64
	epoch int16
	group string
	nowMs int64
}

func encodeTxnAddOffsets(c txnAddOffsetsCmd) []byte {
	w := wire.Writer{B: []byte{cmdTxnAddOffsets}}
	w.String(false, c.id)
	w.Int64(c.pid)
	w.Int16(c.epoch)
	w.String(false, c.group)
	w.Int64(c.nowMs)
	return w.B
}

func decodeTxnAddOffsets(r *wire.Reader) (txnAddOffsetsCmd, bool) {
	c := txnAddOffsetsCmd{
		id:    r.String(false),
		pid:   r.Int64(),
		epoch: r.Int16(),
		group: r.String(false),
		nowMs: r.Int64(),
	}
	return c, r.Err() == nil && r.Len() == 0
}

type txnOffsetEntry struct {
	tp  tpName
	off committedOffset
}

type txnOffsetCommitCmd struct {
	id         string
	group      string
	pid        int64
	epoch      int16
	generation int32
	memberID   string
	nowMs      int64
	entries    []txnOffsetEntry
}

func encodeTxnOffsetCommit(c txnOffsetCommitCmd) []byte {
	w := wire.Writer{B: []byte{cmdTxnOffsetCommit}}
	w.String(false, c.id)
	w.String(false, c.group)
	w.Int64(c.pid)
	w.Int16(c.epoch)
	w.Int32(c.generation)
	w.String(false, c.memberID)
	w.Int64(c.nowMs)
	w.ArrayLen(false, len(c.entries))
	for _, e := range c.entries {
		w.String(false, e.tp.topic)
		w.Int32(e.tp.part)
		w.Int64(e.off.offset)
		w.Int32(e.off.leaderEpoch)
		w.String(false, e.off.metadata)
	}
	return w.B
}

func decodeTxnOffsetCommit(r *wire.Reader) (txnOffsetCommitCmd, bool) {
	c := txnOffsetCommitCmd{
		id:         r.String(false),
		group:      r.String(false),
		pid:        r.Int64(),
		epoch:      r.Int16(),
		generation: r.Int32(),
		memberID:   r.String(false),
		nowMs:      r.Int64(),
	}
	c.entries = wire.Array(r, false, func() txnOffsetEntry {
		return txnOffsetEntry{
			tp:  tpName{topic: r.String(false), part: r.Int32()},
			off: committedOffset{offset: r.Int64(), leaderEpoch: r.Int32(), metadata: r.String(false), time: c.nowMs},
		}
	})
	return c, r.Err() == nil && r.Len() == 0
}

type txnEndCmd struct {
	id        string
	pid       int64
	epoch     int16
	commit    bool
	bumpEpoch bool
	nowMs     int64
	isTimeout bool
}

func encodeTxnEnd(c txnEndCmd) []byte {
	w := wire.Writer{B: []byte{cmdTxnEnd}}
	w.String(false, c.id)
	w.Int64(c.pid)
	w.Int16(c.epoch)
	w.Bool(c.commit)
	w.Bool(c.bumpEpoch)
	w.Int64(c.nowMs)
	w.Bool(c.isTimeout)
	return w.B
}

func decodeTxnEnd(r *wire.Reader) (txnEndCmd, bool) {
	c := txnEndCmd{
		id:        r.String(false),
		pid:       r.Int64(),
		epoch:     r.Int16(),
		commit:    r.Bool(),
		bumpEpoch: r.Bool(),
		nowMs:     r.Int64(),
		isTimeout: r.Bool(),
	}
	return c, r.Err() == nil && r.Len() == 0
}

// --- Apply ---

func (s *state) applyTxnInit(index int64, cmd txnInitCmd) result {
	if cmd.timeoutMs <= 0 || cmd.timeoutMs > maxTxnTimeoutMs {
		return result{errCode: wire.ErrInvalidTxnTimeout, txnPID: -1, txnEpoch: -1}
	}
	tx := s.txns[cmd.id]
	if tx == nil {
		if cmd.pid >= 0 {
			return result{errCode: wire.ErrInvalidProducerIDMapping, txnPID: -1, txnEpoch: -1}
		}
		tx = &txnState{
			id:        cmd.id,
			pid:       index << 16,
			epoch:     0,
			prevPID:   -1,
			prevEpoch: -1,
			phase:     txnEmpty,
			timeoutMs: cmd.timeoutMs,
		}
		s.txns[cmd.id] = tx
		return result{txnPID: tx.pid, txnEpoch: tx.epoch}
	}
	if cmd.pid >= 0 && (cmd.pid != tx.pid || cmd.epoch != tx.epoch) {
		return result{errCode: wire.ErrProducerFenced, txnPID: -1, txnEpoch: -1}
	}
	if tx.phase == txnOngoing {
		s.endTxn(index, tx, false, cmd.nowMs, false)
	}
	tx.prevPID, tx.prevEpoch = tx.pid, tx.epoch
	if tx.epoch == math.MaxInt16 {
		tx.pid = index << 16
		tx.epoch = 0
	} else {
		tx.epoch++
	}
	tx.timeoutMs = cmd.timeoutMs
	return result{txnPID: tx.pid, txnEpoch: tx.epoch}
}

func (s *state) applyTxnAddParts(cmd txnAddPartsCmd) result {
	tx := s.txns[cmd.id]
	if tx == nil {
		return result{errCode: wire.ErrInvalidProducerIDMapping}
	}
	if tx.pid != cmd.pid || tx.epoch != cmd.epoch {
		return result{errCode: wire.ErrProducerFenced}
	}
	errs := make(map[tpName]int16, len(cmd.parts))
	if cmd.verifyOnly {
		for _, tp := range cmd.parts {
			switch {
			case s.partition(tp.topic, tp.part) == nil:
				errs[tp] = wire.ErrUnknownTopicOrPartition
			case tx.phase != txnOngoing || !slices.Contains(tx.parts, tp):
				errs[tp] = wire.ErrInvalidTxnState
			}
		}
		return result{txnPartErrs: errs}
	}
	if tx.phase != txnOngoing {
		tx.phase = txnOngoing
		tx.startedMs = cmd.nowMs
	}
	for _, tp := range cmd.parts {
		if s.partition(tp.topic, tp.part) == nil {
			errs[tp] = wire.ErrUnknownTopicOrPartition
			continue
		}
		tx.addPart(tp.topic, tp.part)
	}
	return result{txnPartErrs: errs}
}

func (s *state) applyTxnAddOffsets(cmd txnAddOffsetsCmd) result {
	tx := s.txns[cmd.id]
	if tx == nil {
		return result{errCode: wire.ErrInvalidProducerIDMapping}
	}
	if tx.pid != cmd.pid || tx.epoch != cmd.epoch {
		return result{errCode: wire.ErrProducerFenced}
	}
	if tx.phase != txnOngoing {
		tx.phase = txnOngoing
		tx.startedMs = cmd.nowMs
	}
	tx.addGroup(cmd.group)
	return result{}
}

func (s *state) applyTxnOffsetCommit(cmd txnOffsetCommitCmd) result {
	tx := s.txns[cmd.id]
	if tx == nil {
		return result{errCode: wire.ErrInvalidProducerIDMapping}
	}
	if tx.pid != cmd.pid || tx.epoch != cmd.epoch {
		return result{errCode: wire.ErrProducerFenced}
	}
	if tx.phase != txnOngoing || !slices.Contains(tx.groups, cmd.group) {
		return result{errCode: wire.ErrInvalidTxnState}
	}
	g := s.group(cmd.group)
	if cmd.generation >= 0 || cmd.memberID != "" || g.generation != 0 {
		if code := g.checkCommit(cmd.generation, cmd.memberID, s.noFencing); code != wire.ErrNone {
			return result{errCode: code}
		}
	}
	if tx.pendingOffsets == nil {
		tx.pendingOffsets = map[string]map[tpName]committedOffset{}
	}
	if tx.pendingOffsets[cmd.group] == nil {
		tx.pendingOffsets[cmd.group] = map[tpName]committedOffset{}
	}
	errs := make(map[tpName]int16, len(cmd.entries))
	for _, e := range cmd.entries {
		if s.partition(e.tp.topic, e.tp.part) == nil {
			errs[e.tp] = wire.ErrUnknownTopicOrPartition
			continue
		}
		tx.pendingOffsets[cmd.group][e.tp] = e.off
	}
	return result{txnOffsetErrs: errs}
}

func (s *state) applyTxnEnd(index int64, cmd txnEndCmd) result {
	tx := s.txns[cmd.id]
	if tx == nil {
		return result{errCode: wire.ErrInvalidProducerIDMapping, txnPID: -1, txnEpoch: -1}
	}
	if cmd.isTimeout {
		if tx.phase != txnOngoing || tx.pid != cmd.pid || tx.epoch != cmd.epoch {
			return result{applied: false, txnPID: tx.pid, txnEpoch: tx.epoch}
		}
		s.endTxn(index, tx, false, cmd.nowMs, true)
		return result{applied: true, txnPID: tx.pid, txnEpoch: tx.epoch}
	}
	if tx.pid != cmd.pid || tx.epoch != cmd.epoch {
		// Idempotent retry after KIP-890 EndTxn v5 bumped the epoch.
		if cmd.bumpEpoch && tx.prevPID == cmd.pid && tx.prevEpoch == cmd.epoch &&
			((tx.phase == txnCompleteCommit && cmd.commit) || (tx.phase == txnCompleteAbort && !cmd.commit)) {
			return result{txnPID: tx.pid, txnEpoch: tx.epoch}
		}
		return result{errCode: wire.ErrProducerFenced, txnPID: -1, txnEpoch: -1}
	}
	if (tx.phase == txnCompleteCommit && cmd.commit) || (tx.phase == txnCompleteAbort && !cmd.commit) {
		return result{txnPID: tx.pid, txnEpoch: tx.epoch}
	}
	if tx.phase != txnOngoing {
		return result{errCode: wire.ErrInvalidTxnState, txnPID: tx.pid, txnEpoch: tx.epoch}
	}
	s.endTxn(index, tx, cmd.commit, cmd.nowMs, cmd.bumpEpoch)
	return result{txnPID: tx.pid, txnEpoch: tx.epoch}
}

func (s *state) endTxn(index int64, tx *txnState, commit bool, nowMs int64, bumpEpoch bool) {
	ctrl := uint8(1)
	if commit {
		ctrl = 2
	}
	for _, tp := range tx.parts {
		p := s.partition(tp.topic, tp.part)
		if p == nil {
			continue
		}
		markerOff := p.next
		p.batches = append(p.batches, batchRef{
			base:         markerOff,
			logIndex:     index,
			maxTimestamp: nowMs,
			control:      ctrl,
			controlPID:   tx.pid,
			controlEpoch: tx.epoch,
		})
		p.next++
		if ot, ok := p.openTxns[tx.pid]; ok {
			delete(p.openTxns, tx.pid)
			if !commit {
				p.aborted = append(p.aborted, abortedTxn{
					pid:         tx.pid,
					firstOffset: ot.firstOffset,
					lastOffset:  markerOff,
				})
			}
		}
	}
	if commit {
		for _, gname := range sortedKeys(tx.pendingOffsets) {
			g := s.forWrite(gname)
			for _, tp := range sortedTPNames(tx.pendingOffsets[gname]) {
				if s.partition(tp.topic, tp.part) == nil && !s.keepOffsetsOnDelete {
					continue
				}
				if g.offsets[tp.topic] == nil {
					g.offsets[tp.topic] = map[int32]committedOffset{}
				}
				g.offsets[tp.topic][tp.part] = tx.pendingOffsets[gname][tp]
			}
		}
	}
	tx.parts = nil
	tx.groups = nil
	tx.pendingOffsets = nil
	if commit {
		tx.phase = txnCompleteCommit
	} else {
		tx.phase = txnCompleteAbort
	}
	if bumpEpoch {
		tx.prevPID, tx.prevEpoch = tx.pid, tx.epoch
		if tx.epoch == math.MaxInt16 {
			tx.pid = index << 16
			tx.epoch = 0
		} else {
			tx.epoch++
		}
	}
}

// --- Node handlers and timers ---

func (n *Node) readBatch(ref batchRef) ([]byte, bool) {
	if ref.control != 0 {
		return wire.ControlBatch(ref.base, 0, ref.controlPID, ref.controlEpoch, ref.maxTimestamp, ref.control == 2), true
	}
	if ref.objKey != "" {
		return n.objCache.get(ref)
	}
	cmd, _, err := n.raft.Read(ref.logIndex)
	if err != nil {
		if n.fatal == nil {
			n.fatal = fmt.Errorf("read committed batch at log index %d: %w", ref.logIndex, err)
		}
		return nil, false
	}
	return appendBatch(cmd), true
}

func (n *Node) initTransactionalProducer(c *conn, h wire.RequestHeader, m *wire.InitProducerIDRequest) {
	if !n.may(c, opWrite, resTxnID, m.TransactionalID) {
		n.respond(c, h, &wire.InitProducerIDResponse{ErrorCode: wire.ErrTransactionalIDAuthFailed, ProducerID: -1, ProducerEpoch: -1})
		return
	}
	if n.levelRefused(7) != "" {
		n.respond(c, h, &wire.InitProducerIDResponse{ErrorCode: wire.ErrCoordinatorNotAvailable, ProducerID: -1, ProducerEpoch: -1})
		return
	}
	if m.TransactionalID == "" {
		n.respond(c, h, &wire.InitProducerIDResponse{ErrorCode: wire.ErrInvalidRequest, ProducerID: -1, ProducerEpoch: -1})
		return
	}
	timeout := m.TransactionTimeoutMs
	if timeout == 0 {
		timeout = defaultTxnTimeoutMs
	}
	cmd := txnInitCmd{
		id:        m.TransactionalID,
		timeoutMs: timeout,
		pid:       m.ProducerID,
		epoch:     m.ProducerEpoch,
		nowMs:     n.nowMs(),
	}
	n.propose(encodeTxnInit(cmd), func(res result) {
		n.armTxnTimer()
		n.respond(c, h, &wire.InitProducerIDResponse{
			ErrorCode:     res.errCode,
			ProducerID:    res.txnPID,
			ProducerEpoch: res.txnEpoch,
		})
	})
}

func (n *Node) addPartitionsToTxn(c *conn, h wire.RequestHeader, req *wire.AddPartitionsToTxnRequest) {
	if n.levelRefused(7) != "" {
		n.respond(c, h, &wire.AddPartitionsToTxnResponse{ErrorCode: wire.ErrCoordinatorNotAvailable})
		return
	}
	if len(req.Transactions) == 0 {
		n.respond(c, h, &wire.AddPartitionsToTxnResponse{})
		return
	}
	// Handle each transaction sequentially in one callback chain (normally len == 1).
	results := make([]wire.AddPartitionsToTxnResult, len(req.Transactions))
	var step func(i int)
	step = func(i int) {
		if i == len(req.Transactions) {
			n.respond(c, h, &wire.AddPartitionsToTxnResponse{ResultsByTransaction: results})
			return
		}
		tx := req.Transactions[i]
		tr := wire.AddPartitionsToTxnResult{TransactionalID: tx.TransactionalID}
		if !n.may(c, opWrite, resTxnID, tx.TransactionalID) {
			for _, t := range tx.Topics {
				tr.TopicResults = append(tr.TopicResults, topicTxnErr(t, wire.ErrTransactionalIDAuthFailed))
			}
			results[i] = tr
			step(i + 1)
			return
		}
		var parts []tpName
		denied := map[string]bool{}
		for _, t := range tx.Topics {
			if !n.may(c, opWrite, resTopic, t.Name) {
				denied[t.Name] = true
				continue
			}
			for _, p := range t.Partitions {
				parts = append(parts, tpName{topic: t.Name, part: p})
			}
		}
		cmd := txnAddPartsCmd{
			id: tx.TransactionalID, pid: tx.ProducerID, epoch: tx.ProducerEpoch,
			verifyOnly: tx.VerifyOnly, nowMs: n.nowMs(), parts: parts,
		}
		n.propose(encodeTxnAddParts(cmd), func(res result) {
			n.armTxnTimer()
			for _, t := range tx.Topics {
				if denied[t.Name] {
					tr.TopicResults = append(tr.TopicResults, topicTxnErr(t, wire.ErrTopicAuthorizationFailed))
					continue
				}
				tres := wire.AddPartitionsToTxnTopicResult{Name: t.Name}
				for _, p := range t.Partitions {
					code := res.errCode
					if code == wire.ErrNone {
						code = res.txnPartErrs[tpName{topic: t.Name, part: p}]
					}
					tres.Partitions = append(tres.Partitions, wire.AddPartitionsToTxnPartitionResult{
						PartitionIndex: p,
						ErrorCode:      code,
					})
				}
				tr.TopicResults = append(tr.TopicResults, tres)
			}
			results[i] = tr
			step(i + 1)
		})
	}
	step(0)
}

func topicTxnErr(t wire.AddPartitionsToTxnTopic, code int16) wire.AddPartitionsToTxnTopicResult {
	res := wire.AddPartitionsToTxnTopicResult{Name: t.Name}
	for _, p := range t.Partitions {
		res.Partitions = append(res.Partitions, wire.AddPartitionsToTxnPartitionResult{PartitionIndex: p, ErrorCode: code})
	}
	return res
}

func (n *Node) addOffsetsToTxn(c *conn, h wire.RequestHeader, req *wire.AddOffsetsToTxnRequest) {
	if !n.may(c, opWrite, resTxnID, req.TransactionalID) {
		n.respond(c, h, &wire.AddOffsetsToTxnResponse{ErrorCode: wire.ErrTransactionalIDAuthFailed})
		return
	}
	if !n.may(c, opRead, resGroup, req.GroupID) {
		n.respond(c, h, &wire.AddOffsetsToTxnResponse{ErrorCode: wire.ErrGroupAuthorizationFailed})
		return
	}
	if n.levelRefused(7) != "" {
		n.respond(c, h, &wire.AddOffsetsToTxnResponse{ErrorCode: wire.ErrCoordinatorNotAvailable})
		return
	}
	cmd := txnAddOffsetsCmd{
		id: req.TransactionalID, pid: req.ProducerID, epoch: req.ProducerEpoch,
		group: req.GroupID, nowMs: n.nowMs(),
	}
	n.propose(encodeTxnAddOffsets(cmd), func(res result) {
		n.armTxnTimer()
		n.respond(c, h, &wire.AddOffsetsToTxnResponse{ErrorCode: res.errCode})
	})
}

func (n *Node) txnOffsetCommit(c *conn, h wire.RequestHeader, req *wire.TxnOffsetCommitRequest) {
	if !n.may(c, opWrite, resTxnID, req.TransactionalID) {
		n.respond(c, h, txnOffsetCommitAllErr(req, wire.ErrTransactionalIDAuthFailed))
		return
	}
	if !n.may(c, opRead, resGroup, req.GroupID) {
		n.respond(c, h, txnOffsetCommitAllErr(req, wire.ErrGroupAuthorizationFailed))
		return
	}
	if n.levelRefused(7) != "" {
		n.respond(c, h, txnOffsetCommitAllErr(req, wire.ErrCoordinatorNotAvailable))
		return
	}
	var entries []txnOffsetEntry
	for _, t := range req.Topics {
		for _, p := range t.Partitions {
			entries = append(entries, txnOffsetEntry{
				tp:  tpName{topic: t.Name, part: p.Index},
				off: committedOffset{offset: p.Offset, leaderEpoch: p.LeaderEpoch, metadata: p.Metadata, time: n.nowMs()},
			})
		}
	}
	cmd := txnOffsetCommitCmd{
		id: req.TransactionalID, group: req.GroupID, pid: req.ProducerID, epoch: req.ProducerEpoch,
		generation: req.GenerationID, memberID: req.MemberID, nowMs: n.nowMs(), entries: entries,
	}
	n.propose(encodeTxnOffsetCommit(cmd), func(res result) {
		resp := &wire.TxnOffsetCommitResponse{}
		for _, t := range req.Topics {
			tr := wire.TxnOffsetCommitTopicResponse{Name: t.Name}
			for _, p := range t.Partitions {
				code := res.errCode
				if code == wire.ErrNone {
					code = res.txnOffsetErrs[tpName{topic: t.Name, part: p.Index}]
				}
				tr.Partitions = append(tr.Partitions, wire.TxnOffsetCommitPartitionResponse{Index: p.Index, ErrorCode: code})
			}
			resp.Topics = append(resp.Topics, tr)
		}
		n.respond(c, h, resp)
	})
}

func txnOffsetCommitAllErr(req *wire.TxnOffsetCommitRequest, code int16) *wire.TxnOffsetCommitResponse {
	resp := &wire.TxnOffsetCommitResponse{}
	for _, t := range req.Topics {
		tr := wire.TxnOffsetCommitTopicResponse{Name: t.Name}
		for _, p := range t.Partitions {
			tr.Partitions = append(tr.Partitions, wire.TxnOffsetCommitPartitionResponse{Index: p.Index, ErrorCode: code})
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return resp
}

func (n *Node) endTxn(c *conn, h wire.RequestHeader, req *wire.EndTxnRequest) {
	if !n.may(c, opWrite, resTxnID, req.TransactionalID) {
		n.respond(c, h, &wire.EndTxnResponse{ErrorCode: wire.ErrTransactionalIDAuthFailed, ProducerID: -1, ProducerEpoch: -1})
		return
	}
	if n.levelRefused(7) != "" {
		n.respond(c, h, &wire.EndTxnResponse{ErrorCode: wire.ErrCoordinatorNotAvailable, ProducerID: -1, ProducerEpoch: -1})
		return
	}
	cmd := txnEndCmd{
		id: req.TransactionalID, pid: req.ProducerID, epoch: req.ProducerEpoch,
		commit: req.Committed, bumpEpoch: h.Version >= 5, nowMs: n.nowMs(),
	}
	n.propose(encodeTxnEnd(cmd), func(res result) {
		n.armTxnTimer()
		n.wakeFetches()
		if res.errCode == wire.ErrNone && n.faults.inject(FaultLostAck) {
			n.log.Info("fault injected", "kind", FaultLostAck, "key", h.Key, "fault", n.faults.count)
			if !c.closed {
				c.closed = true
				n.net.Close(c.id)
			}
			return
		}
		n.respond(c, h, &wire.EndTxnResponse{
			ErrorCode:     res.errCode,
			ProducerID:    res.txnPID,
			ProducerEpoch: res.txnEpoch,
		})
	})
}

func (n *Node) armTxnTimer() {
	if !n.serving || n.fatal != nil {
		return
	}
	var earliest int64
	for _, id := range sortedKeys(n.st.txns) {
		tx := n.st.txns[id]
		if tx.phase != txnOngoing {
			continue
		}
		exp := tx.startedMs + int64(tx.timeoutMs)
		if earliest == 0 || exp < earliest {
			earliest = exp
		}
	}
	if n.txnTimer != 0 {
		n.clock.CancelTimer(n.txnTimer)
		delete(n.timers, n.txnTimer)
		n.txnTimer = 0
	}
	if earliest == 0 {
		return
	}
	delay := time.Duration(earliest-n.nowMs()) * time.Millisecond
	if delay < time.Millisecond {
		delay = time.Millisecond
	}
	tid := n.nextTimer
	n.nextTimer++
	n.txnTimer = tid
	n.timers[tid] = func() {
		n.txnTimer = 0
		n.expireTxns()
	}
	n.clock.SetTimer(tid, delay)
}

func (n *Node) expireTxns() {
	if !n.serving || n.fatal != nil || n.st.level < 7 {
		return
	}
	now := n.nowMs()
	for _, id := range sortedKeys(n.st.txns) {
		tx := n.st.txns[id]
		if tx.phase == txnOngoing && tx.startedMs+int64(tx.timeoutMs) <= now {
			n.propose(encodeTxnEnd(txnEndCmd{
				id: tx.id, pid: tx.pid, epoch: tx.epoch,
				commit: false, bumpEpoch: true, nowMs: now, isTimeout: true,
			}), func(res result) {
				if res.applied {
					n.wakeFetches()
				}
				n.armTxnTimer()
			})
			return
		}
	}
	n.armTxnTimer()
}
