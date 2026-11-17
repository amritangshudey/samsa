package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func seedInput(t *testing.T, b *broker, topic string, parts int32, n int) {
	t.Helper()
	admin := franz(t, b)
	createTopic(t, admin, topic, parts)
	createTopic(t, admin, "out", parts)
	var recs []*kgo.Record
	for i := range n {
		recs = append(recs, &kgo.Record{
			Topic:     topic,
			Partition: int32(i) % parts,
			Key:       fmt.Appendf(nil, "k%04d", i),
			Value:     []byte("v"),
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	in := franz(t, b, kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err := in.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatalf("produce input: %v", err)
	}
	in.Close()
}

// TestFaultVerdictTransactions verifies that `-faults` + `samsa verdict -expect exactly-once`
// catches a non-transactional/non-idempotent pipeline under lostack+notleader and passes a
// transactional GroupTransactSession pipeline under the same faults.
func TestFaultVerdictTransactions(t *testing.T) {
	const faults = "seed=7,rate=0.25,kinds=lostack+notleader"

	t.Run("buggy_duplicates", func(t *testing.T) {
		b := startBroker(t, "-faults", faults)
		seedInput(t, b, "in", 2, 60)

		cl := franz(t, b,
			kgo.ConsumerGroup("app"), kgo.ConsumeTopics("in"),
			kgo.DisableAutoCommit(),
			kgo.DisableIdempotentWrite(),
			kgo.MetadataMinAge(100*time.Millisecond),
			kgo.ProducerBatchCompression(kgo.NoCompression()),
		)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for last := time.Now(); time.Since(last) < 2*time.Second; {
			pctx, pc := context.WithTimeout(ctx, 300*time.Millisecond)
			fs := cl.PollFetches(pctx)
			pc()
			recs := fs.Records()
			if len(recs) == 0 {
				continue
			}
			for _, r := range recs {
				_ = cl.ProduceSync(ctx, &kgo.Record{Topic: "out", Partition: r.Partition, Key: r.Key, Value: r.Value}).FirstErr()
			}
			_ = cl.CommitRecords(ctx, recs...)
			last = time.Now()
		}
		cl.Close()
		b.kill()

		out, code := verdict(t, b, "-expect", "exactly-once")
		if code != 1 || !strings.Contains(out, "duplicated") {
			t.Fatalf("buggy txn app should fail with duplicated (code=%d):\n%s", code, out)
		}
	})

	t.Run("fixed_transactional", func(t *testing.T) {
		b := startBroker(t, "-faults", faults)
		seedInput(t, b, "in", 2, 60)

		sess, err := kgo.NewGroupTransactSession(
			kgo.SeedBrokers(b.addr),
			kgo.ConsumerGroup("app"),
			kgo.ConsumeTopics("in"),
			kgo.TransactionalID("tx-app"),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
			kgo.FetchMaxWait(100*time.Millisecond),
			kgo.MetadataMinAge(100*time.Millisecond),
			kgo.ProducerBatchCompression(kgo.NoCompression()),
		)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		for last := time.Now(); time.Since(last) < 3*time.Second; {
			pctx, pc := context.WithTimeout(ctx, 300*time.Millisecond)
			fs := sess.PollFetches(pctx)
			pc()
			recs := fs.Records()
			if len(recs) == 0 {
				continue
			}
			if err := sess.Begin(); err != nil {
				t.Fatalf("Begin: %v", err)
			}
			var outs []*kgo.Record
			for _, r := range recs {
				outs = append(outs, &kgo.Record{Topic: "out", Partition: r.Partition, Key: r.Key, Value: r.Value})
			}
			perr := sess.ProduceSync(ctx, outs...).FirstErr()
			committed, cerr := sess.End(ctx, kgo.TransactionEndTry(perr == nil))
			if cerr != nil {
				t.Fatalf("End: %v (committed=%v)", cerr, committed)
			}
			last = time.Now()
		}
		sess.Close()
		b.kill()

		out, code := verdict(t, b, "-expect", "exactly-once")
		if code != 0 {
			t.Fatalf("fixed transactional app should pass (code=%d):\n%s", code, out)
		}
	})
}

// TestFaultVerdictKIP848 exercises a KIP-848 consumer group (ConsumerGroupHeartbeat)
// under rebalance+expire+notleader faults: committing before processing loses records
// on revocation/expiry, while committing after processing passes.
func TestFaultVerdictKIP848(t *testing.T) {
	const faults = "seed=11,rate=0.10,kinds=rebalance+expire+notleader"

	run := func(t *testing.T, commitFirst bool) (string, int) {
		t.Helper()
		b := startBroker(t, "-faults", faults)
		seedInput(t, b, "in", 2, 60)

		cl := franz(t, b,
			kgo.MetadataMinAge(100*time.Millisecond),
			kgo.ProducerBatchCompression(kgo.NoCompression()),
		)
		defer cl.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()

		mreq := kmsg.NewMetadataRequest()
		mreq.Topics = []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr("in")}}
		mresp, err := mreq.RequestWith(ctx, cl)
		if err != nil || len(mresp.Topics) == 0 {
			t.Fatalf("metadata: %v %+v", err, mresp)
		}
		inID := mresp.Topics[0].TopicID

		memberID := "k848-m1"
		var epoch int32
		nextOff := map[int32]int64{0: 0, 1: 0}
		owned := map[int32]bool{}

		fetchCommitted := func() {
			of := kmsg.NewOffsetFetchRequest()
			of.Groups = []kmsg.OffsetFetchRequestGroup{{
				Group: "app",
				Topics: []kmsg.OffsetFetchRequestGroupTopic{{
					Topic:      "in",
					Partitions: []int32{0, 1},
				}},
			}}
			if resp, err := of.RequestWith(ctx, cl); err == nil && len(resp.Groups) > 0 {
				for _, tp := range resp.Groups[0].Topics {
					for _, pp := range tp.Partitions {
						if pp.ErrorCode == 0 && pp.Offset >= 0 {
							nextOff[pp.Partition] = pp.Offset
						}
					}
				}
			}
		}

		heartbeat := func() (revoked bool) {
			req := kmsg.NewConsumerGroupHeartbeatRequest()
			req.Group = "app"
			req.MemberID = memberID
			req.MemberEpoch = epoch
			req.RebalanceTimeoutMillis = 5000
			if epoch == 0 {
				req.SubscribedTopicNames = []string{"in"}
			}
			var ownParts []int32
			for p := range owned {
				ownParts = append(ownParts, p)
			}
			req.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{{TopicID: inID, Partitions: ownParts}}
			resp, err := req.RequestWith(ctx, cl)
			if err != nil {
				return true
			}
			if resp.ErrorCode != 0 {
				if resp.ErrorCode == wire.ErrUnknownMemberID || resp.ErrorCode == wire.ErrFencedMemberEpoch {
					epoch = 0
					clear(owned)
					fetchCommitted()
				}
				return true
			}
			epoch = resp.MemberEpoch
			if resp.Assignment != nil {
				newOwned := map[int32]bool{}
				for _, tp := range resp.Assignment.Topics {
					if tp.TopicID == inID {
						for _, p := range tp.Partitions {
							newOwned[p] = true
						}
					}
				}
				for p := range owned {
					if !newOwned[p] {
						revoked = true
					}
				}
				owned = newOwned
				if revoked {
					fetchCommitted()
				}
			}
			return revoked
		}

		commitOffset := func(p int32, off int64) {
			for range 5 {
				creq := kmsg.NewOffsetCommitRequest()
				creq.Group = "app"
				creq.MemberID = memberID
				creq.Generation = epoch
				creq.Topics = []kmsg.OffsetCommitRequestTopic{{
					Topic:      "in",
					Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: p, Offset: off, LeaderEpoch: -1}},
				}}
				cresp, err := creq.RequestWith(ctx, cl)
				if err == nil && len(cresp.Topics) > 0 && cresp.Topics[0].Partitions[0].ErrorCode == 0 {
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
		}

		lastHB := time.Time{}
		for last := time.Now(); time.Since(last) < 1500*time.Millisecond; {
			if time.Since(lastHB) >= 50*time.Millisecond {
				_ = heartbeat()
				lastHB = time.Now()
			}
			if len(owned) == 0 {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			freq := kmsg.NewFetchRequest()
			freq.MaxWaitMillis = 50
			freq.MinBytes = 1
			freq.MaxBytes = 1 << 20
			ft := kmsg.NewFetchRequestTopic()
			ft.Topic = "in"
			ft.TopicID = inID
			for p := range owned {
				fp := kmsg.NewFetchRequestTopicPartition()
				fp.Partition = p
				fp.FetchOffset = nextOff[p]
				fp.PartitionMaxBytes = 16 << 10
				ft.Partitions = append(ft.Partitions, fp)
			}
			freq.Topics = append(freq.Topics, ft)
			fresp, err := freq.RequestWith(ctx, cl)
			if err != nil {
				continue
			}
			type fetchedRec struct {
				part int32
				off  int64
				key  []byte
				val  []byte
			}
			var recs []fetchedRec
			for _, rt := range fresp.Topics {
				for _, rp := range rt.Partitions {
					if rp.ErrorCode != 0 || len(rp.RecordBatches) == 0 {
						continue
					}
					batches := wire.SplitBatches(rp.RecordBatches)
					for _, raw := range batches {
						base, dec, err := wire.DecodeBatch(raw)
						if err != nil {
							continue
						}
						for j, r := range dec {
							recs = append(recs, fetchedRec{part: rp.Partition, off: base + int64(j), key: r.Key, val: r.Value})
						}
					}
				}
			}
			if len(recs) == 0 {
				continue
			}
			if commitFirst {
				for _, r := range recs {
					if r.off+1 > nextOff[r.part] {
						nextOff[r.part] = r.off + 1
					}
				}
				for p, off := range nextOff {
					commitOffset(p, off)
				}
			}
			for _, r := range recs {
				if commitFirst || time.Since(lastHB) >= 50*time.Millisecond {
					lastHB = time.Now()
					if heartbeat() {
						break // revoked/expired mid-batch
					}
				}
				if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "out", Partition: r.part, Key: r.key, Value: r.val}).FirstErr(); err != nil {
					break
				}
				if !commitFirst {
					if r.off+1 > nextOff[r.part] {
						nextOff[r.part] = r.off + 1
					}
					commitOffset(r.part, nextOff[r.part])
				}
				time.Sleep(time.Millisecond)
			}
			last = time.Now()
		}
		b.kill()
		return verdict(t, b)
	}

	out, code := run(t, true)
	if code != 1 || !strings.Contains(out, "lost:") {
		t.Fatalf("buggy KIP-848 app should fail with lost (code=%d):\n%s", code, out)
	}
	out, code = run(t, false)
	if code != 0 {
		t.Fatalf("fixed KIP-848 app should pass (code=%d):\n%s", code, out)
	}
}

// TestFaultVerdictShareGroup exercises a share group (ShareGroupHeartbeat + ShareFetch +
// ShareAcknowledge) under rebalance+expire+lostack faults: acking before producing loses
// records when revoked/expired/failed, while acking after producing passes.
func TestFaultVerdictShareGroup(t *testing.T) {
	const faults = "seed=13,rate=0.10,kinds=rebalance+expire+lostack"

	run := func(t *testing.T, ackFirst bool) (string, int) {
		t.Helper()
		b := startBroker(t, "-faults", faults)
		seedInput(t, b, "in", 2, 60)

		cl := franz(t, b,
			kgo.MetadataMinAge(100*time.Millisecond),
			kgo.ProducerBatchCompression(kgo.NoCompression()),
		)
		defer cl.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()

		mreq := kmsg.NewMetadataRequest()
		mreq.Topics = []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr("in")}}
		mresp, err := mreq.RequestWith(ctx, cl)
		if err != nil || len(mresp.Topics) == 0 {
			t.Fatalf("metadata: %v %+v", err, mresp)
		}
		inID := mresp.Topics[0].TopicID

		memberID := "sg-m1"
		var epoch int32
		var sessEpoch int32
		assigned := map[int32]bool{}

		heartbeat := func() (revoked bool) {
			req := kmsg.NewShareGroupHeartbeatRequest()
			req.GroupID = "app"
			req.MemberID = memberID
			req.MemberEpoch = epoch
			if epoch == 0 {
				req.SubscribedTopicNames = []string{"in"}
			}
			resp, err := req.RequestWith(ctx, cl)
			if err != nil {
				return true
			}
			if resp.ErrorCode != 0 {
				if resp.ErrorCode == wire.ErrUnknownMemberID || resp.ErrorCode == wire.ErrFencedMemberEpoch {
					epoch = 0
					sessEpoch = 0
					clear(assigned)
				}
				return true
			}
			epoch = resp.MemberEpoch
			if resp.Assignment != nil {
				newAssigned := map[int32]bool{}
				for _, tp := range resp.Assignment.TopicPartitions {
					if tp.TopicID == inID {
						for _, p := range tp.Partitions {
							newAssigned[p] = true
						}
					}
				}
				for p := range assigned {
					if !newAssigned[p] {
						revoked = true
					}
				}
				assigned = newAssigned
			}
			return revoked
		}

		ackRecord := func(p int32, off int64) {
			areq := kmsg.NewShareAcknowledgeRequest()
			areq.GroupID = kmsg.StringPtr("app")
			areq.MemberID = kmsg.StringPtr(memberID)
			areq.ShareSessionEpoch = sessEpoch
			at := kmsg.NewShareAcknowledgeRequestTopic()
			at.TopicID = inID
			ap := kmsg.NewShareAcknowledgeRequestTopicPartition()
			ap.Partition = p
			ab := kmsg.NewShareAcknowledgeRequestTopicPartitionAcknowledgementBatch()
			ab.FirstOffset = off
			ab.LastOffset = off
			ab.AcknowledgeTypes = []int8{wire.AckAccept}
			ap.AcknowledgementBatches = append(ap.AcknowledgementBatches, ab)
			at.Partitions = append(at.Partitions, ap)
			areq.Topics = append(areq.Topics, at)
			_, _ = areq.RequestWith(ctx, cl)
		}

		lastHB := time.Time{}
		for last := time.Now(); time.Since(last) < 1500*time.Millisecond; {
			if time.Since(lastHB) >= 50*time.Millisecond {
				_ = heartbeat()
				lastHB = time.Now()
			}
			if len(assigned) == 0 {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			sf := kmsg.NewShareFetchRequest()
			sf.GroupID = kmsg.StringPtr("app")
			sf.MemberID = kmsg.StringPtr(memberID)
			sf.ShareSessionEpoch = sessEpoch
			sf.MaxWaitMillis = 50
			sf.MinBytes = 1
			sf.MaxBytes = 1 << 20
			sf.MaxRecords = 10
			st := kmsg.NewShareFetchRequestTopic()
			st.TopicID = inID
			for p := range assigned {
				sp := kmsg.NewShareFetchRequestTopicPartition()
				sp.Partition = p
				st.Partitions = append(st.Partitions, sp)
			}
			sf.Topics = append(sf.Topics, st)
			sresp, err := sf.RequestWith(ctx, cl)
			if err != nil {
				continue
			}
			if sresp.ErrorCode != 0 {
				sessEpoch = 0
				continue
			}
			if sessEpoch == 0 {
				sessEpoch = 1
			}
			type acquiredRec struct {
				part int32
				off  int64
				key  []byte
				val  []byte
			}
			var recs []acquiredRec
			for _, rt := range sresp.Topics {
				for _, rp := range rt.Partitions {
					if rp.ErrorCode != 0 || len(rp.Records) == 0 || len(rp.AcquiredRecords) == 0 {
						continue
					}
					batches := wire.SplitBatches(rp.Records)
					wanted := map[int64]bool{}
					for _, ar := range rp.AcquiredRecords {
						for o := ar.FirstOffset; o <= ar.LastOffset; o++ {
							wanted[o] = true
						}
					}
					for _, raw := range batches {
						base, dec, err := wire.DecodeBatch(raw)
						if err != nil {
							continue
						}
						for j, r := range dec {
							off := base + int64(j)
							if wanted[off] {
								recs = append(recs, acquiredRec{part: rp.Partition, off: off, key: r.Key, val: r.Value})
							}
						}
					}
				}
			}
			if len(recs) == 0 {
				continue
			}
			if ackFirst {
				for _, r := range recs {
					ackRecord(r.part, r.off)
				}
			}
			for _, r := range recs {
				if ackFirst || time.Since(lastHB) >= 50*time.Millisecond {
					lastHB = time.Now()
					if heartbeat() {
						break
					}
				}
				if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "out", Partition: r.part, Key: r.key, Value: r.val}).FirstErr(); err != nil {
					break
				}
				if !ackFirst {
					ackRecord(r.part, r.off)
				}
				time.Sleep(time.Millisecond)
			}
			last = time.Now()
		}
		b.kill()
		return verdict(t, b)
	}

	out, code := run(t, true)
	if code != 1 || !strings.Contains(out, "lost:") {
		t.Fatalf("buggy share group app should fail with lost (code=%d):\n%s", code, out)
	}
	out, code = run(t, false)
	if code != 0 {
		t.Fatalf("fixed share group app should pass (code=%d):\n%s", code, out)
	}
}
