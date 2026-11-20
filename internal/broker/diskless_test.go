package broker

import (
	"strings"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/simsys"
	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

func TestDisklessProduceIdempotenceAndRaftBytes(t *testing.T) {
	shared := simsys.NewSharedObjects(simsys.ObjFaults{MaxDelay: 2 * time.Millisecond})
	var h *harness
	var pending []func()
	obj := simsys.NewNodeObjectStore(
		shared,
		simsys.NewRand(42),
		func(_ time.Duration, fn func()) { pending = append(pending, fn) },
		func(id sys.ObjOpID, data []byte, err error) {
			if err := h.node.OnObjectDone(id, data, err); err != nil {
				t.Errorf("OnObjectDone: %v", err)
			}
		},
	)
	drain := func() {
		for len(pending) > 0 {
			q := pending
			pending = nil
			for _, fn := range q {
				fn()
			}
			h.run()
		}
	}

	h = newHarness(t, Config{ObjectStore: obj})
	h.open(1)
	h.advance(2 * time.Second)

	// Create one diskless topic and one classic topic.
	var cresp wire.CreateTopicsResponse
	h.call(1, wire.KeyCreateTopics, 7, &wire.CreateTopicsRequest{
		Topics: []wire.CreateTopic{
			{
				Name: "dl", NumPartitions: 1, ReplicationFactor: 1,
				Configs: []wire.TopicConfig{{Name: "diskless.enabled", Value: "true"}},
			},
			{
				Name: "classic", NumPartitions: 1, ReplicationFactor: 1,
			},
		},
	}, &cresp)
	for _, r := range cresp.Topics {
		if r.ErrorCode != wire.ErrNone {
			t.Fatalf("create %s: %d %s", r.Name, r.ErrorCode, r.ErrorMessage)
		}
	}

	payload := strings.Repeat("d", 4096)
	b := wire.EncodeBatch([]wire.Record{{Key: []byte("k0"), Value: []byte(payload)}})
	wire.SetBatchProducer(b, 100, 0, 0)

	// Measure Raft log bytes before/after diskless produce vs classic produce.
	beforeDL := h.node.raft.LogBytesFrom(0)
	corr := h.send(1, wire.KeyProduce, 9, produceReq("dl", 0, b))
	h.run()
	drain()
	var presp wire.ProduceResponse
	if !h.recv(1, wire.KeyProduce, 9, corr, &presp) || presp.Topics[0].Partitions[0].ErrorCode != wire.ErrNone || presp.Topics[0].Partitions[0].BaseOffset != 0 {
		t.Fatalf("diskless produce: %+v", presp)
	}
	dlRaftBytes := h.node.raft.LogBytesFrom(0) - beforeDL

	// Idempotent duplicate of the same batch returns BaseOffset=0 without advancing next.
	corr2 := h.send(1, wire.KeyProduce, 9, produceReq("dl", 0, b))
	h.run()
	drain()
	var dupResp wire.ProduceResponse
	if !h.recv(1, wire.KeyProduce, 9, corr2, &dupResp) || dupResp.Topics[0].Partitions[0].ErrorCode != wire.ErrNone || dupResp.Topics[0].Partitions[0].BaseOffset != 0 {
		t.Fatalf("diskless dup produce: %+v", dupResp)
	}
	if h.node.st.partition("dl", 0).next != 1 {
		t.Fatalf("diskless dup advanced next to %d", h.node.st.partition("dl", 0).next)
	}

	beforeClassic := h.node.raft.LogBytesFrom(0)
	bClassic := wire.EncodeBatch([]wire.Record{{Key: []byte("k0"), Value: []byte(payload)}})
	var cpresp wire.ProduceResponse
	h.call(1, wire.KeyProduce, 9, produceReq("classic", 0, bClassic), &cpresp)
	classicRaftBytes := h.node.raft.LogBytesFrom(0) - beforeClassic

	t.Logf("4 KiB batch Raft log bytes: diskless=%d classic=%d", dlRaftBytes, classicRaftBytes)
	if dlRaftBytes >= classicRaftBytes/10 {
		t.Fatalf("expected diskless Raft bytes (%d) << classic (%d)", dlRaftBytes, classicRaftBytes)
	}

	// Cold-cache Fetch reads back from ObjectStore.
	h.node.objCache = newObjCache(32 << 20)
	fcorr := h.send(1, wire.KeyFetch, 11, fetchReq("dl", 0, 0, 0, 1))
	h.run()
	drain()
	var fresp wire.FetchResponse
	if !h.recv(1, wire.KeyFetch, 11, fcorr, &fresp) {
		t.Fatal("no fetch response")
	}
	pr := fresp.Topics[0].Partitions[0]
	if pr.ErrorCode != wire.ErrNone || len(pr.Records) == 0 {
		t.Fatalf("fetch diskless: err=%d bytes=%d", pr.ErrorCode, len(pr.Records))
	}
	base, recs, err := wire.DecodeBatch(wire.SplitBatches(pr.Records)[0])
	if err != nil || base != 0 || string(recs[0].Value) != payload {
		t.Fatalf("decoded diskless record: err=%v base=%d len=%d", err, base, len(recs))
	}
}
