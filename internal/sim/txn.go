package sim

import (
	"fmt"

	"github.com/amritangshudey/samsa/internal/wire"
)

const txnTopic = "tx0"

type txnPhase int

const (
	txnPhaseInit txnPhase = iota
	txnPhaseAddParts
	txnPhaseProduce
	txnPhaseEnd
)

type txnProducer struct {
	id         string
	pid        int64
	epoch      int16
	seq        int32
	phase      txnPhase
	shouldAbt  bool
	produced   int
	inFlightOK bool
	inFlight   bool
}

type rcConsumer struct {
	cursors  map[TP]int64
	inFlight bool
}

// txnModel tracks transactional producer state for invariant T1: a
// read_committed consumer never reads uncommitted or aborted records.
type txnModel struct {
	open          map[int64]map[TP][]int64
	pendingCommit map[int64]map[TP]map[int64]bool
	committed     map[TP]map[int64]bool
	afterLoss     bool
}

func (t *txnModel) forget() {
	for _, parts := range t.pendingCommit {
		for tp, offs := range parts {
			if t.committed == nil {
				t.committed = map[TP]map[int64]bool{}
			}
			if t.committed[tp] == nil {
				t.committed[tp] = map[int64]bool{}
			}
			for off := range offs {
				t.committed[tp][off] = true
			}
		}
	}
	clear(t.open)
	clear(t.pendingCommit)
	t.afterLoss = true
}

// TxnProduced records an acknowledged transactional offset for pid.
func (m *Model) TxnProduced(pid int64, tp TP, base int64, count int32, epoch int) {
	if m.stale(epoch) || base < 0 {
		return
	}
	if m.txn.open == nil {
		m.txn.open = map[int64]map[TP][]int64{}
	}
	if m.txn.open[pid] == nil {
		m.txn.open[pid] = map[TP][]int64{}
	}
	for i := int32(0); i < count; i++ {
		m.txn.open[pid][tp] = append(m.txn.open[pid][tp], base+int64(i))
	}
}

// TxnCommitSent marks pid's open offsets as in-flight commit so a concurrent
// read_committed fetch racing ahead of EndTxnResponse sees them as valid.
func (m *Model) TxnCommitSent(pid int64, epoch int) {
	if m.stale(epoch) {
		return
	}
	if m.txn.pendingCommit == nil {
		m.txn.pendingCommit = map[int64]map[TP]map[int64]bool{}
	}
	if m.txn.pendingCommit[pid] == nil {
		m.txn.pendingCommit[pid] = map[TP]map[int64]bool{}
	}
	for tp, offs := range m.txn.open[pid] {
		if m.txn.pendingCommit[pid][tp] == nil {
			m.txn.pendingCommit[pid][tp] = map[int64]bool{}
		}
		for _, off := range offs {
			m.txn.pendingCommit[pid][tp][off] = true
		}
	}
}

// TxnEnded transitions pid's open offsets to committed (if commit) or drops them (if aborted).
func (m *Model) TxnEnded(pid int64, commit bool, epoch int) {
	if m.stale(epoch) {
		return
	}
	parts := m.txn.open[pid]
	delete(m.txn.open, pid)
	delete(m.txn.pendingCommit, pid)
	if !commit {
		return
	}
	if m.txn.committed == nil {
		m.txn.committed = map[TP]map[int64]bool{}
	}
	for tp, offs := range parts {
		if m.txn.committed[tp] == nil {
			m.txn.committed[tp] = map[int64]bool{}
		}
		for _, off := range offs {
			m.txn.committed[tp][off] = true
		}
	}
}

// CheckReadCommitted verifies invariant T1 for a transactional record read
// under IsolationLevel=1 (READ_COMMITTED).
func (m *Model) CheckReadCommitted(tp TP, pid, off, lso int64, epoch int) error {
	if m.stale(epoch) {
		return nil
	}
	if off >= lso {
		return fmt.Errorf("T1: read_committed consumer read %v offset %d >= LSO %d", tp, off, lso)
	}
	if !m.txn.afterLoss && !m.txn.committed[tp][off] && !m.txn.pendingCommit[pid][tp][off] {
		return fmt.Errorf("T1: read_committed consumer read uncommitted or aborted %v offset %d (pid=%d)", tp, off, pid)
	}
	return nil
}

func (cl *client) txnProduce() {
	if cl.txn == nil {
		cl.txn = &txnProducer{id: fmt.Sprintf("sim-tx-%d", cl.lane), pid: -1, epoch: -1}
	}
	tx := cl.txn
	if tx.inFlight {
		return
	}
	if cl.w.model.TopicID(txnTopic) == (wire.UUID{}) {
		tx.inFlight = true
		cl.w.model.TopicAttempted(txnTopic, 1)
		req := &wire.CreateTopicsRequest{
			Topics:    []wire.CreateTopic{{Name: txnTopic, NumPartitions: 1, ReplicationFactor: cl.w.replication()}},
			TimeoutMs: 1000,
		}
		resp := &wire.CreateTopicsResponse{}
		epoch := cl.w.model.Epoch()
		cl.request(wire.KeyCreateTopics, cl.version(wire.KeyCreateTopics), req, resp, func(ok bool) {
			tx.inFlight = false
			if ok && len(resp.Topics) == 1 && (resp.Topics[0].ErrorCode == wire.ErrNone || resp.Topics[0].ErrorCode == wire.ErrTopicAlreadyExists) {
				cl.w.model.TopicCreated(txnTopic, epoch)
				if resp.Topics[0].TopicID != (wire.UUID{}) {
					_ = cl.w.model.NoteTopicID(txnTopic, resp.Topics[0].TopicID, epoch)
				}
			}
		})
		return
	}
	switch tx.phase {
	case txnPhaseInit:
		tx.inFlight = true
		req := &wire.InitProducerIDRequest{
			Transactional:        true,
			TransactionalID:      tx.id,
			TransactionTimeoutMs: 2000,
			ProducerID:           tx.pid,
			ProducerEpoch:        tx.epoch,
		}
		resp := &wire.InitProducerIDResponse{}
		cl.request(wire.KeyInitProducerID, cl.version(wire.KeyInitProducerID), req, resp, func(ok bool) {
			tx.inFlight = false
			if !ok {
				return
			}
			switch resp.ErrorCode {
			case wire.ErrNone:
				tx.pid, tx.epoch = resp.ProducerID, resp.ProducerEpoch
				tx.seq = 0
				tx.produced = 0
				tx.shouldAbt = false
				tx.inFlightOK = true
				tx.phase = txnPhaseAddParts
			case wire.ErrCoordinatorNotAvailable, wire.ErrCoordinatorLoadInProgress, wire.ErrConcurrentTransactions, wire.ErrNotCoordinator:
			case wire.ErrProducerFenced, wire.ErrInvalidProducerIDMapping:
				tx.pid, tx.epoch = -1, -1
			default:
				cl.w.fail("txn InitProducerId failed with code %d", resp.ErrorCode)
			}
		})
	case txnPhaseAddParts:
		if tx.pid < 0 {
			tx.phase = txnPhaseInit
			return
		}
		tx.inFlight = true
		req := &wire.AddPartitionsToTxnRequest{
			Transactions: []wire.AddPartitionsToTxnTransaction{{
				TransactionalID: tx.id,
				ProducerID:      tx.pid,
				ProducerEpoch:   tx.epoch,
				Topics:          []wire.AddPartitionsToTxnTopic{{Name: txnTopic, Partitions: []int32{0}}},
			}},
		}
		resp := &wire.AddPartitionsToTxnResponse{}
		cl.request(wire.KeyAddPartitionsToTxn, cl.version(wire.KeyAddPartitionsToTxn), req, resp, func(ok bool) {
			tx.inFlight = false
			if !ok {
				return
			}
			if resp.ErrorCode == wire.ErrCoordinatorNotAvailable || resp.ErrorCode == wire.ErrConcurrentTransactions || resp.ErrorCode == wire.ErrNotCoordinator {
				return
			}
			if len(resp.ResultsByTransaction) == 0 || len(resp.ResultsByTransaction[0].TopicResults) == 0 {
				return
			}
			code := resp.ResultsByTransaction[0].TopicResults[0].Partitions[0].ErrorCode
			switch code {
			case wire.ErrNone:
				tx.produced = 0
				tx.inFlightOK = true
				tx.shouldAbt = cl.rng.Chance(0.4)
				tx.phase = txnPhaseProduce
			case wire.ErrProducerFenced, wire.ErrInvalidProducerEpoch, wire.ErrInvalidProducerIDMapping:
				tx.phase = txnPhaseInit
				tx.pid, tx.epoch = -1, -1
			case wire.ErrUnknownTopicOrPartition, wire.ErrCoordinatorNotAvailable, wire.ErrConcurrentTransactions, wire.ErrNotCoordinator:
			default:
				cl.w.fail("AddPartitionsToTxn failed with code %d", code)
			}
		})
	case txnPhaseProduce:
		if tx.pid < 0 {
			tx.phase = txnPhaseInit
			return
		}
		tx.inFlight = true
		tp := TP{txnTopic, 0}
		val := fmt.Sprintf("tx-%s-%d", tx.id, tx.seq)
		b := wire.EncodeBatch([]wire.Record{{Key: []byte(tx.id), Value: []byte(val)}})
		wire.SetBatchTransactional(b)
		wire.SetBatchProducer(b, tx.pid, tx.epoch, tx.seq)
		req := &wire.ProduceRequest{
			TransactionalID:    tx.id,
			HasTransactionalID: true,
			Acks:               -1,
			TimeoutMs:          1000,
			Topics:             []wire.ProduceTopic{{Name: tp.Topic, Partitions: []wire.ProducePartition{{Index: tp.Part, Records: b}}}},
		}
		epoch := cl.w.model.Epoch()
		resp := &wire.ProduceResponse{}
		cl.request(wire.KeyProduce, cl.version(wire.KeyProduce), req, resp, func(ok bool) {
			tx.inFlight = false
			if !ok {
				tx.inFlightOK = false
				tx.shouldAbt = true
				tx.phase = txnPhaseEnd
				return
			}
			if len(resp.Topics) == 0 || len(resp.Topics[0].Partitions) == 0 {
				return
			}
			pr := resp.Topics[0].Partitions[0]
			switch pr.ErrorCode {
			case wire.ErrNone:
				cl.w.model.TxnProduced(tx.pid, tp, pr.BaseOffset, 1, epoch)
				tx.seq = wire.AddSequence(tx.seq, 1)
				tx.produced++
				if tx.produced >= 2 || cl.rng.Chance(0.5) {
					tx.phase = txnPhaseEnd
				}
			case wire.ErrInvalidTxnState, wire.ErrInvalidProducerEpoch, wire.ErrOutOfOrderSequenceNumber, wire.ErrUnknownTopicOrPartition, wire.ErrNotLeaderOrFollower:
				tx.shouldAbt = true
				tx.phase = txnPhaseEnd
			default:
				cl.w.fail("txn Produce failed with code %d", pr.ErrorCode)
			}
		})
	case txnPhaseEnd:
		if tx.pid < 0 {
			tx.phase = txnPhaseInit
			return
		}
		tx.inFlight = true
		commit := !tx.shouldAbt && tx.inFlightOK
		v := cl.version(wire.KeyEndTxn)
		req := &wire.EndTxnRequest{
			TransactionalID: tx.id,
			ProducerID:      tx.pid,
			ProducerEpoch:   tx.epoch,
			Committed:       commit,
		}
		epoch := cl.w.model.Epoch()
		pid := tx.pid
		if commit {
			cl.w.model.TxnCommitSent(pid, epoch)
		}
		resp := &wire.EndTxnResponse{}
		cl.request(wire.KeyEndTxn, v, req, resp, func(ok bool) {
			tx.inFlight = false
			if !ok {
				if commit {
					cl.w.model.TxnEnded(pid, true, epoch)
				}
				tx.phase = txnPhaseInit
				return
			}
			switch resp.ErrorCode {
			case wire.ErrNone:
				cl.w.model.TxnEnded(pid, commit, epoch)
				if commit {
					cl.w.stats.TxnCommits++
				} else {
					cl.w.stats.TxnAborts++
				}
				if v >= 5 && resp.ProducerEpoch >= 0 {
					tx.pid, tx.epoch = resp.ProducerID, resp.ProducerEpoch
					tx.seq = 0
					tx.phase = txnPhaseAddParts
				} else {
					tx.phase = txnPhaseInit
				}
			case wire.ErrCoordinatorNotAvailable, wire.ErrConcurrentTransactions, wire.ErrNotCoordinator:
			case wire.ErrProducerFenced, wire.ErrInvalidProducerEpoch, wire.ErrInvalidTxnState, wire.ErrInvalidProducerIDMapping:
				cl.w.model.TxnEnded(pid, false, epoch)
				tx.phase = txnPhaseInit
				tx.pid, tx.epoch = -1, -1
			default:
				cl.w.fail("EndTxn failed with code %d", resp.ErrorCode)
			}
		})
	}
}

func (cl *client) rcConsume() {
	if cl.rc == nil {
		cl.rc = &rcConsumer{cursors: map[TP]int64{}}
	}
	if cl.rc.inFlight {
		return
	}
	tp := TP{txnTopic, 0}
	tid := cl.w.model.TopicID(tp.Topic)
	if tid == (wire.UUID{}) {
		return
	}
	cl.rc.inFlight = true
	offset := cl.rc.cursors[tp]
	v := cl.version(wire.KeyFetch)
	req := &wire.FetchRequest{
		MaxWaitMs:      20,
		MinBytes:       1,
		MaxBytes:       1 << 20,
		IsolationLevel: 1,
		Topics: []wire.FetchTopic{{
			Name:       tp.Topic,
			TopicID:    tid,
			Partitions: []wire.FetchPartition{{Index: tp.Part, Offset: offset, MaxBytes: 1 << 20}},
		}},
	}
	epoch := cl.w.model.Epoch()
	resp := &wire.FetchResponse{}
	cl.request(wire.KeyFetch, v, req, resp, func(ok bool) {
		cl.rc.inFlight = false
		if !ok || len(resp.Topics) == 0 || len(resp.Topics[0].Partitions) == 0 {
			return
		}
		fpr := resp.Topics[0].Partitions[0]
		switch fpr.ErrorCode {
		case wire.ErrNone:
			cl.w.stats.RCFetches++
			aborted := make(map[int64]int64, len(fpr.AbortedTransactions))
			for _, a := range fpr.AbortedTransactions {
				if prev, ok := aborted[a.ProducerID]; !ok || a.FirstOffset < prev {
					aborted[a.ProducerID] = a.FirstOffset
				}
			}
			for _, b := range wire.SplitBatches(fpr.Records) {
				info, ok := wire.PeekBatch(b)
				if !ok {
					continue
				}
				base := wire.BatchBaseOffset(b)
				if next := base + int64(wire.BatchLastOffsetDelta(b)) + 1; next > cl.rc.cursors[tp] {
					cl.rc.cursors[tp] = next
				}
				if info.Control {
					continue
				}
				if firstAbt, isAbt := aborted[info.ProducerID]; isAbt && base >= firstAbt {
					continue
				}
				if info.Transactional {
					for i := int32(0); i < info.Count; i++ {
						off := base + int64(i)
						cl.check(cl.w.model.CheckReadCommitted(tp, info.ProducerID, off, fpr.LastStableOffset, epoch))
					}
				}
			}
		case wire.ErrOffsetOutOfRange:
			if fpr.LogStartOffset > cl.rc.cursors[tp] {
				cl.rc.cursors[tp] = fpr.LogStartOffset
			}
		case wire.ErrNotLeaderOrFollower, wire.ErrOffsetNotAvailable, wire.ErrUnknownTopicOrPartition, wire.ErrUnknownTopicID:
		default:
			cl.w.fail("rc Fetch failed with code %d", fpr.ErrorCode)
		}
	})
}
