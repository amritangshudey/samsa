package broker

import (
	"fmt"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/wire"
)

// produceNow writes one record stamped with the harness clock.
func (h *harness) produceNow(topic string, value string) {
	h.t.Helper()
	ms := int64(h.clock.now) / int64(time.Millisecond)
	var resp wire.ProduceResponse
	h.call(1, wire.KeyProduce, 8, produceReq(topic, 0, wire.EncodeBatch([]wire.Record{{Key: []byte("k"), Value: []byte(value), Timestamp: ms}})), &resp)
	if code := resp.Topics[0].Partitions[0].ErrorCode; code != 0 {
		h.t.Fatalf("produce to %s: %d", topic, code)
	}
}

func (h *harness) alterTopic(topic string, cs ...wire.AlterableConfig) {
	h.t.Helper()
	var resp wire.IncrementalAlterConfigsResponse
	h.call(1, wire.KeyIncrementalAlterConfigs, 1, &wire.IncrementalAlterConfigsRequest{Resources: []wire.AlterConfigsResource{{Type: wire.ResourceTopic, Name: topic, Configs: cs}}}, &resp)
	if code := resp.Results[0].ErrorCode; code != 0 {
		h.t.Fatalf("alter %s: %d %s", topic, code, resp.Results[0].ErrorMessage)
	}
}

func setConfig(name, value string) wire.AlterableConfig {
	return wire.AlterableConfig{Name: name, Operation: wire.ConfigSet, Value: value}
}

// retention.ms moves a partition's start past batches older than it;
// retention.bytes keeps at most that many bytes per partition; a topic
// with neither keeps everything.
func TestRetentionByTopic(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	for _, name := range []string{"timed", "sized", "kept"} {
		h.createTopic(1, name, 1)
	}
	h.alterTopic("timed", setConfig("retention.ms", "60000"))
	h.alterTopic("sized", setConfig("retention.bytes", "300"))
	for i := range 10 {
		for _, name := range []string{"timed", "sized", "kept"} {
			h.produceNow(name, fmt.Sprintf("value-%d", i))
		}
		h.advance(10 * time.Second)
	}
	h.advance(retainEvery)
	parts := func(name string) *partition { return h.node.st.topics[name].parts[0] }
	// Written at 0, 10, ..., 90 s; now 101 s: those at 40 s and before are
	// older than a minute.
	if got := parts("timed").start; got != 5 {
		t.Fatalf("timed start %d, want 5", got)
	}
	var kept int64
	for _, b := range parts("sized").batches {
		kept += int64(b.size)
	}
	if p := parts("sized"); p.start == 0 || kept > 300 || kept+int64(p.batches[0].size) <= 300 {
		t.Fatalf("sized: start %d, %d bytes kept", p.start, kept)
	}
	if got := parts("kept").start; got != 0 {
		t.Fatalf("kept start %d", got)
	}
	if p, vals := h.fetch(1, "timed", 0, 5); p.ErrorCode != 0 || vals[0] != "value-5" {
		t.Fatalf("fetch at the new start: %+v %v", p, vals)
	}
}

// The cleaner: a long-kept topic beside a short-kept one. The short one's
// data expires; the cleaner copies the long one's batches out of the head
// segment and trims it, so the log stays near slack times the live data,
// and every long-kept record still reads back as written.
func TestCleanerCopiesForward(t *testing.T) {
	h := newHarness(t, Config{SegmentBytes: 4096, MaxBatchBytes: 1000, SnapshotEvery: 1})
	h.open(1)
	h.createTopic(1, "short", 1)
	h.createTopic(1, "long", 1)
	h.alterTopic("short", setConfig("retention.ms", "10000"))
	var long []string
	for i := range 400 {
		v := fmt.Sprintf("value-%04d-%s", i, "padding-padding-padding")
		if i%10 == 0 {
			h.produceNow("long", v)
			long = append(long, v)
		} else {
			h.produceNow("short", v)
		}
		h.advance(time.Second / 4)
	}
	for range 30 {
		h.advance(time.Second)
	}
	m := h.node.Metrics()
	if m.RelocatedBytes == 0 || h.node.st.logStart == 0 {
		t.Fatalf("nothing cleaned: relocated %d, log start %d", m.RelocatedBytes, h.node.st.logStart)
	}
	if m.ForcedRetentionBytes != 0 {
		t.Fatalf("forced %d bytes without a safety limit", m.ForcedRetentionBytes)
	}
	disk, live := h.node.raft.LogBytesFrom(h.node.st.logStart), h.node.liveBytes()
	t.Logf("log %d bytes, live %d, relocated %d, log start %d", disk, live, m.RelocatedBytes, h.node.st.logStart)
	if float64(disk) > 2*float64(max(live, 4096))+4096 {
		t.Fatalf("log %d bytes for %d live", disk, live)
	}
	var got []string
	for off := int64(0); off < int64(len(long)); {
		p, vals := h.fetch(1, "long", 0, off)
		if p.ErrorCode != 0 || len(vals) == 0 {
			t.Fatalf("fetch long at %d: %+v", off, p)
		}
		got = append(got, vals...)
		off += int64(len(vals))
	}
	if fmt.Sprint(got[:len(long)]) != fmt.Sprint(long) {
		t.Fatalf("long-kept records changed:\n%v\n%v", got, long)
	}
	// A restart replays the relocations to the same state.
	hash := h.node.st.hash()
	h.start()
	if h.node.st.hash() != hash {
		t.Fatal("state after restart differs")
	}
}

// A follower forwards IncrementalAlterConfigs to the leader, as Kafka's
// admin client sends it to any broker, and every node applies it.
func TestAlterConfigsForwarded(t *testing.T) {
	tr := newTrio(t)
	for _, h := range tr.h {
		h.start()
		h.open(1)
	}
	tr.elect(1, 1, 2, 3)
	lead, follower := tr.h[1], tr.h[2]
	corr := lead.send(1, wire.KeyCreateTopics, 4, &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{{Name: "t", NumPartitions: 1, ReplicationFactor: 3}}})
	tr.deliver(1, 2, 3)
	var ct wire.CreateTopicsResponse
	if !lead.recv(1, wire.KeyCreateTopics, 4, corr, &ct) || ct.Topics[0].ErrorCode != 0 {
		t.Fatalf("create topic: %+v", ct)
	}
	lead.must(lead.node.OnTimer(timerHeartbeat))
	tr.deliver(1, 2, 3)
	if follower.node.st.level < 2 {
		t.Fatalf("follower at level %d", follower.node.st.level)
	}
	corr = follower.send(1, wire.KeyIncrementalAlterConfigs, 1, &wire.IncrementalAlterConfigsRequest{Resources: []wire.AlterConfigsResource{
		{Type: wire.ResourceTopic, Name: "t", Configs: []wire.AlterableConfig{setConfig("retention.ms", "60000")}}}})
	follower.run()
	tr.deliver(1, 2, 3)
	lead.must(lead.node.OnTimer(timerHeartbeat))
	tr.deliver(1, 2, 3)
	var resp wire.IncrementalAlterConfigsResponse
	if !follower.recv(1, wire.KeyIncrementalAlterConfigs, 1, corr, &resp) || resp.Results[0].ErrorCode != 0 {
		t.Fatalf("through a follower: %+v", resp)
	}
	for id, h := range tr.h {
		if v := h.node.st.topics["t"].configs["retention.ms"]; v != "60000" {
			t.Fatalf("node %d has retention.ms %q", id, v)
		}
	}
}
