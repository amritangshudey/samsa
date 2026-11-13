package sim

import (
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// The idempotent producer (docs/design/a2-idempotent-producer.md, section
// 4) behaves as the Java client does with idempotence on: it gets an id,
// numbers each partition's batches, and after an unknown outcome re-sends
// the very same batches, in order, with the same sequences. Its records
// keep their ids across retries, so a retry appended twice puts one record
// at two offsets, which the history checker reports (I1/I4). Two checks
// are its own:
//
//	I8  a producer that follows the protocol is never answered a sequence
//	    or epoch error;
//	I9  no producer id is handed out twice, by any node in any life.

// giveUp is how long a batch may stay unacknowledged before the producer
// abandons it and moves its partition to a new epoch, as the Java client
// does at delivery.timeout.ms.
const giveUp = 2 * time.Second

// idemBatch is one batch, kept until it is acknowledged or abandoned.
type idemBatch struct {
	seq      int32
	first    uint64 // id of its first record
	n        int
	bytes    []byte // sent unchanged on every attempt
	born     sys.Instant
	sends    int
	inFlight bool
}

// idemPart is what the producer knows of one partition: the epoch and next
// sequence there, and the batches not yet acknowledged, oldest first.
// Those in flight are always a prefix: all of them travel on the one
// connection, and when it fails they all fail together.
type idemPart struct {
	tp    TP
	epoch int16
	next  int32
	queue []*idemBatch
	// left is set once a batch has left the queue, acknowledged or
	// abandoned; lastBorn is when the newest of those was first sent.
	left     bool
	lastBorn sys.Instant
}

// leave records that the batches bs, oldest first, left p's queue.
func (p *idemPart) leave(bs ...*idemBatch) {
	if len(bs) > 0 {
		p.left, p.lastBorn = true, bs[len(bs)-1].born
	}
}

// restart moves p to a new epoch and renumbers its queue from 0, as the
// Java client does after the broker has forgotten the producer.
func (p *idemPart) restart(id int64) {
	p.epoch, p.next = p.epoch+1, 0
	for _, b := range p.queue {
		b.seq = p.next
		wire.SetBatchProducer(b.bytes, id, p.epoch, b.seq)
		p.next = wire.AddSequence(p.next, int32(b.n))
	}
}

// idemProducer is one producer session.
type idemProducer struct {
	id     int64 // -1 until InitProducerId answers
	asking bool
	parts  []*idemPart // in first-use order, so that nothing depends on map order
}

func (ip *idemProducer) part(tp TP) *idemPart {
	for _, p := range ip.parts {
		if p.tp == tp {
			return p
		}
	}
	p := &idemPart{tp: tp}
	ip.parts = append(ip.parts, p)
	return p
}

// produceIdempotent issues the idempotent producer's next request: an id
// if it has none, else a retry, else a new batch.
func (cl *client) produceIdempotent() {
	w, ip := cl.w, cl.idem
	if ip.id < 0 {
		if !ip.asking {
			cl.initProducerID()
		}
		return
	}
	idle := true
	for _, p := range ip.parts {
		i := 0
		for i < len(p.queue) && p.queue[i].inFlight {
			i++
		}
		idle = idle && len(p.queue) == 0
		switch {
		case i == len(p.queue):
		case i == 0 && w.now-p.queue[0].born > sys.Instant(giveUp):
			// Nothing in flight: abandon the lot. Their copies may yet be
			// applied, before the new epoch's first batch or refused
			// after it; either way no record is written twice.
			w.stats.EpochBumps++
			p.leave(p.queue...)
			p.queue, p.epoch, p.next = nil, p.epoch+1, 0
		default:
			cl.sendIdempotent(p, p.queue[i])
			return
		}
	}
	if idle && cl.rng.Chance(0.02) {
		// A new session, so that ids are asked for across restarts too.
		cl.idem = &idemProducer{id: -1}
		return
	}
	t := catalogue[cl.rng.Intn(len(catalogue))]
	tp := TP{t.name, int32(cl.rng.Intn(int(t.parts)))}
	if !w.model.Known(tp) {
		cl.createTopic()
		return
	}
	p := ip.part(tp)
	n := 1 + cl.rng.Intn(5)
	b := &idemBatch{seq: p.next, first: w.model.Send(tp, n), n: n, born: w.now}
	recs := make([]wire.Record, n)
	for i := range recs {
		recs[i] = wire.Record{Value: Value(b.first + uint64(i)), Timestamp: int64(w.now / 1e6)}
	}
	b.bytes = wire.EncodeBatch(recs)
	wire.SetBatchProducer(b.bytes, ip.id, p.epoch, b.seq)
	p.next = wire.AddSequence(p.next, int32(n))
	p.queue = append(p.queue, b)
	cl.sendIdempotent(p, b)
}

// initProducerID asks the connected node, whichever it is, for an id.
func (cl *client) initProducerID() {
	w, ip := cl.w, cl.idem
	ip.asking = true
	resp := &wire.InitProducerIDResponse{}
	cl.request(wire.KeyInitProducerID, cl.version(wire.KeyInitProducerID), &wire.InitProducerIDRequest{}, resp, func(ok bool) {
		ip.asking = false
		if !ok {
			return
		}
		if resp.ErrorCode != wire.ErrNone || resp.ProducerID < 0 || resp.ProducerEpoch != 0 {
			w.fail("InitProducerId answered code %d, id %d, epoch %d", resp.ErrorCode, resp.ProducerID, resp.ProducerEpoch)
			return
		}
		if w.producerIDs[resp.ProducerID] {
			w.fail("I9: producer id %d was handed out twice", resp.ProducerID)
			return
		}
		w.producerIDs[resp.ProducerID] = true
		w.stats.ProducerIDs++
		ip.id = resp.ProducerID
	})
}

// sendIdempotent sends one batch of p, which must be the first of p's
// queue that is not in flight.
func (cl *client) sendIdempotent(p *idemPart, b *idemBatch) {
	w, m := cl.w, cl.w.model
	if b.sends++; b.sends > 1 {
		w.stats.Retries++
	}
	b.inFlight = true
	epoch := m.Epoch()
	req := &wire.ProduceRequest{Acks: -1, TimeoutMs: 1000, Topics: []wire.ProduceTopic{{Name: p.tp.Topic, Partitions: []wire.ProducePartition{{Index: p.tp.Part, Records: b.bytes}}}}}
	resp := &wire.ProduceResponse{}
	cl.request(wire.KeyProduce, cl.version(wire.KeyProduce), req, resp, func(ok bool) {
		b.inFlight = false
		if !ok {
			m.Unknown(b.first, b.n) // it stays queued and is sent again
			return
		}
		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
			w.fail("produce response does not match the request")
			return
		}
		switch code := resp.Topics[0].Partitions[0].ErrorCode; code {
		case wire.ErrNone:
			if len(p.queue) == 0 || p.queue[0] != b {
				w.fail("%v: batch with sequence %d acknowledged before the batches ahead of it", p.tp, b.seq)
				return
			}
			p.leave(b)
			p.queue = p.queue[1:]
			cl.check(m.Acked(b.first, b.n, resp.Topics[0].Partitions[0].BaseOffset, epoch))
			w.stats.RecordsAcked += b.n
			w.stats.IdempotentAcked += b.n
			w.tl.mark(w.opts.nodes+cl.lane, w.now, "ack", "")
		case wire.ErrOutOfOrderSequenceNumber, wire.ErrInvalidProducerEpoch:
			// Every copy the partition accepted was sent no later than
			// lastBorn. Expiry needs ProducerExpiry of silence after that
			// (the simulated clocks do not drift), so a refusal sooner is
			// a broker bug.
			if code == wire.ErrOutOfOrderSequenceNumber && w.opts.expiry > 0 && p.left && w.now-p.lastBorn >= sys.Instant(w.opts.expiry) {
				w.stats.ExpiredRefusals++
				p.restart(cl.idem.id)
				cl.hangUp() // the batches behind it fail too, and go again renumbered
				return
			}
			w.fail("I8: %v refused producer %d epoch %d sequence %d (attempt %d) with code %d", p.tp, cl.idem.id, p.epoch, b.seq, b.sends, code)
		default:
			w.fail("the broker rejected a valid idempotent write to %v with code %d", p.tp, code)
		}
	})
}
