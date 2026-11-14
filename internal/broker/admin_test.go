package broker

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

func (h *harness) deleteTopics(c sys.ConnID, topics ...wire.DeleteTopic) []wire.DeleteTopicResult {
	h.t.Helper()
	var resp wire.DeleteTopicsResponse
	h.call(c, wire.KeyDeleteTopics, 6, &wire.DeleteTopicsRequest{Topics: topics, TimeoutMs: 1000}, &resp)
	return resp.Topics
}

func (h *harness) deleteRecords(c sys.ConnID, topic string, part int32, offset int64) wire.DeleteRecordsPartitionResult {
	h.t.Helper()
	var resp wire.DeleteRecordsResponse
	h.call(c, wire.KeyDeleteRecords, 2, &wire.DeleteRecordsRequest{Topics: []wire.DeleteRecordsTopic{{Name: topic, Partitions: []wire.DeleteRecordsPartition{{Index: part, Offset: offset}}}}}, &resp)
	return resp.Topics[0].Partitions[0]
}

func (h *harness) createPartitions(c sys.ConnID, t wire.CreatePartitionsTopic, validateOnly bool) wire.CreatePartitionsResult {
	h.t.Helper()
	var resp wire.CreatePartitionsResponse
	h.call(c, wire.KeyCreatePartitions, 3, &wire.CreatePartitionsRequest{Topics: []wire.CreatePartitionsTopic{t}, TimeoutMs: 1000, ValidateOnly: validateOnly}, &resp)
	return resp.Results[0]
}

func (h *harness) earliest(c sys.ConnID, topic string, part int32) int64 {
	h.t.Helper()
	var resp wire.ListOffsetsResponse
	h.call(c, wire.KeyListOffsets, 5, &wire.ListOffsetsRequest{Topics: []wire.ListOffsetsTopic{{Name: topic, Partitions: []wire.ListOffsetsPartition{{Index: part, Timestamp: wire.TimestampEarliest}}}}}, &resp)
	return resp.Topics[0].Partitions[0].Offset
}

// A topic deleted and created again under its name starts empty: no
// records, no committed offsets, no producer state (design section 3.1).
func TestDeleteTopicForgetsEverything(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(fmt.Sprint("UnsafeKeepOffsetsOnDelete=", keep), func(t *testing.T) {
			h := newHarness(t, Config{SnapshotEvery: 3, UnsafeKeepOffsetsOnDelete: keep})
			h.open(1)
			h.createTopic(1, "t", 1)
			h.produce(1, "t", 0, "old0", "old1")
			if code := h.commit(1, -1, "", 2); code != 0 {
				t.Fatalf("commit: %d", code)
			}
			if res := h.deleteTopics(1, wire.DeleteTopic{Name: "t"}); res[0].ErrorCode != 0 {
				t.Fatalf("delete: %+v", res)
			}
			if p, _ := h.fetch(1, "t", 0, 0); p.ErrorCode != wire.ErrUnknownTopicOrPartition {
				t.Fatalf("fetch from a deleted topic: %+v", p)
			}
			if code := h.commit(1, -1, "", 1); code != wire.ErrUnknownTopicOrPartition {
				t.Fatalf("commit to a deleted topic: %d", code)
			}
			h.createTopic(1, "t", 1)
			got := h.committed(1)
			if want := map[bool]int64{false: -1, true: 2}[keep]; got != want {
				t.Fatalf("committed offset of the new topic: %d, want %d", got, want)
			}
			if p, vals := h.fetch(1, "t", 0, 0); p.ErrorCode != 0 || len(vals) != 0 || p.HighWatermark != 0 {
				t.Fatalf("new topic is not empty: %+v %v", p, vals)
			}
			if res := h.produce(1, "t", 0, "new"); res.BaseOffset != 0 {
				t.Fatalf("first offset of the new topic: %+v", res)
			}
			hash := h.node.st.hash()
			h.start() // replayed from snapshot and log, it must come out the same
			if h.node.st.hash() != hash {
				t.Fatal("state after restart differs")
			}
		})
	}
}

func TestDeleteTopicsRefuses(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	h.createTopic(1, "a", 1)
	res := h.deleteTopics(1, wire.DeleteTopic{Name: "missing"}, wire.DeleteTopic{TopicID: wire.UUID{9}}, wire.DeleteTopic{Name: "a"}, wire.DeleteTopic{Name: "a"})
	want := []int16{wire.ErrUnknownTopicOrPartition, wire.ErrUnknownTopicID, wire.ErrNone, wire.ErrInvalidRequest}
	for i, r := range res {
		if r.ErrorCode != want[i] {
			t.Errorf("topic %d: %+v, want error %d", i, r, want[i])
		}
	}
	if res[1].TopicID != (wire.UUID{9}) {
		t.Errorf("the id is not echoed: %+v", res[1])
	}
}

func TestCreatePartitions(t *testing.T) {
	h := newHarness(t, Config{MaxPartitions: 8})
	h.open(1)
	h.createTopic(1, "t", 1)
	h.produce(1, "t", 0, "x")
	for _, tc := range []struct {
		topic        wire.CreatePartitionsTopic
		validateOnly bool
		code         int16
	}{
		{wire.CreatePartitionsTopic{Name: "t", Count: 1}, false, wire.ErrInvalidPartitions},
		{wire.CreatePartitionsTopic{Name: "t", Count: 9}, false, wire.ErrInvalidPartitions},
		{wire.CreatePartitionsTopic{Name: "t", Count: 3, HasAssignments: true}, false, wire.ErrInvalidReplicaAssignment},
		{wire.CreatePartitionsTopic{Name: "missing", Count: 3}, false, wire.ErrUnknownTopicOrPartition},
		{wire.CreatePartitionsTopic{Name: "t", Count: 3}, true, wire.ErrNone},
	} {
		if res := h.createPartitions(1, tc.topic, tc.validateOnly); res.ErrorCode != tc.code {
			t.Errorf("%+v: %+v, want %d", tc.topic, res, tc.code)
		}
	}
	if n := len(h.node.st.topics["t"].parts); n != 1 {
		t.Fatalf("refused and validate-only requests changed the topic: %d partitions", n)
	}
	if res := h.createPartitions(1, wire.CreatePartitionsTopic{Name: "t", Count: 3}, false); res.ErrorCode != 0 {
		t.Fatal(res)
	}
	if res := h.produce(1, "t", 2, "new"); res.ErrorCode != 0 || res.BaseOffset != 0 {
		t.Fatalf("produce to a new partition: %+v", res)
	}
	if _, vals := h.fetch(1, "t", 0, 0); !reflect.DeepEqual(vals, []string{"x"}) {
		t.Fatalf("old partition changed: %v", vals)
	}
}

// Three batches of three records, offsets 0-8; DeleteRecords at 4 lands
// inside the second batch.
func TestDeleteRecords(t *testing.T) {
	h := newHarness(t, Config{SnapshotEvery: 1})
	h.open(1)
	h.createTopic(1, "t", 1)
	for i := range 3 {
		h.produce(1, "t", 0, fmt.Sprint(3*i), fmt.Sprint(3*i+1), fmt.Sprint(3*i+2))
	}
	if res := h.deleteRecords(1, "t", 0, 10); res.ErrorCode != wire.ErrOffsetOutOfRange {
		t.Fatalf("beyond the high watermark: %+v", res)
	}
	if res := h.deleteRecords(1, "missing", 0, 1); res.ErrorCode != wire.ErrUnknownTopicOrPartition {
		t.Fatalf("unknown topic: %+v", res)
	}
	if res := h.deleteRecords(1, "t", 0, 4); res.ErrorCode != 0 || res.LowWatermark != 4 {
		t.Fatalf("delete below 4: %+v", res)
	}
	check := func(when string) {
		t.Helper()
		if p, _ := h.fetch(1, "t", 0, 3); p.ErrorCode != wire.ErrOffsetOutOfRange || p.LogStartOffset != 4 {
			t.Fatalf("%s: fetch below the start: %+v", when, p)
		}
		// The batch holding 4 comes whole, as from Kafka; the client skips 3.
		if p, vals := h.fetch(1, "t", 0, 4); p.ErrorCode != 0 || vals[0] != "3" || len(vals) != 6 {
			t.Fatalf("%s: fetch from 4: %+v %v", when, p, vals)
		}
		if got := h.earliest(1, "t", 0); got != 4 {
			t.Fatalf("%s: earliest offset %d, want 4", when, got)
		}
	}
	check("after delete")
	if res := h.deleteRecords(1, "t", 0, 2); res.ErrorCode != 0 || res.LowWatermark != 4 {
		t.Fatalf("a lower offset must not move the start back: %+v", res)
	}
	if n := len(h.node.st.topics["t"].parts[0].batches); n != 2 {
		t.Fatalf("%d batches kept, want 2", n)
	}
	h.start() // from the snapshot, whose start is inside a batch
	h.open(1)
	check("after restart")
	if res := h.deleteRecords(1, "t", 0, -1); res.ErrorCode != 0 || res.LowWatermark != 9 {
		t.Fatalf("delete to the high watermark: %+v", res)
	}
	if n := len(h.node.st.topics["t"].parts[0].batches); n != 0 {
		t.Fatalf("%d batches kept, want 0", n)
	}
}

// Retention trims whole batches; it must not move a DeleteRecords start
// back to the first batch it keeps (design section 3.3).
func TestTrimKeepsDeleteRecordsStart(t *testing.T) {
	for _, reset := range []bool{false, true} {
		s := newState()
		s.trimResetsStart = reset
		cmds := [][]byte{encodeCreateTopic("t", 1, nil)}
		for range 3 {
			cmds = append(cmds, encodeAppend("t", 0, batch("a", "b", "c")))
		}
		cmds = append(cmds, encodeDeleteRecords("t", 0, 4), encodeTrim(2)) // the trim keeps batches 2 and 3
		for i, cmd := range cmds {
			if _, err := s.apply(int64(i), cmd); err != nil {
				t.Fatal(err)
			}
		}
		if got, want := s.topics["t"].parts[0].start, map[bool]int64{false: 4, true: 3}[reset]; got != want {
			t.Errorf("trimResetsStart=%v: start %d, want %d", reset, got, want)
		}
	}
}

func TestDescribeConfigs(t *testing.T) {
	h := newHarness(t, Config{RetentionBytes: 1 << 30, SegmentBytes: 4 << 20, MaxBatchBytes: 1 << 20})
	h.open(1)
	h.createTopic(1, "t", 1)
	var resp wire.DescribeConfigsResponse
	h.call(1, wire.KeyDescribeConfigs, 4, &wire.DescribeConfigsRequest{Resources: []wire.DescribeConfigsResource{
		{Type: wire.ResourceTopic, Name: "t", Keys: []string{"retention.bytes", "no.such.key"}},
		{Type: wire.ResourceTopic, Name: "missing"},
		{Type: wire.ResourceBroker, Name: "1"},
		{Type: wire.ResourceBroker, Name: "7"},
	}}, &resp)
	r := resp.Results
	if len(r) != 4 || len(r[0].Configs) != 1 || r[0].Configs[0].Value != "-1" || r[0].Configs[0].ReadOnly {
		t.Fatalf("topic keys: %+v", r)
	}
	if r[1].ErrorCode != wire.ErrUnknownTopicOrPartition || r[3].ErrorCode != wire.ErrInvalidRequest {
		t.Fatalf("errors: %+v %+v", r[1], r[3])
	}
	found := false
	for _, c := range r[2].Configs {
		found = found || c.Name == "samsa.sync" && c.Value == "always"
	}
	if !found {
		t.Fatalf("broker configs: %+v", r[2].Configs)
	}
}

func TestListAndDescribeGroups(t *testing.T) {
	h, a, b := stableGroup(t)
	var list wire.ListGroupsResponse
	h.call(1, wire.KeyListGroups, 5, &wire.ListGroupsRequest{}, &list)
	if want := []wire.ListedGroup{{ID: "g", ProtocolType: "consumer", State: "Stable", Type: "classic"}}; !reflect.DeepEqual(list.Groups, want) {
		t.Fatalf("ListGroups: %+v", list.Groups)
	}
	h.call(1, wire.KeyListGroups, 5, &wire.ListGroupsRequest{States: []string{"empty"}}, &list)
	if len(list.Groups) != 0 {
		t.Fatalf("state filter: %+v", list.Groups)
	}
	var desc wire.DescribeGroupsResponse
	h.call(1, wire.KeyDescribeGroups, 6, &wire.DescribeGroupsRequest{Groups: []string{"g", "none"}}, &desc)
	g := desc.Groups[0]
	assigned := map[string]string{}
	for _, m := range g.Members {
		assigned[m.ID] = string(m.Assignment)
		if string(m.Metadata) != "sub-range" || m.ClientID != "test" {
			t.Fatalf("member: %+v", m)
		}
	}
	if g.State != "Stable" || g.ProtocolData != "range" || !reflect.DeepEqual(assigned, map[string]string{a: "a", b: "b"}) {
		t.Fatalf("DescribeGroups: %+v", g)
	}
	if d := desc.Groups[1]; d.State != "Dead" || d.ErrorCode != 0 {
		t.Fatalf("unknown group: %+v", d)
	}
}

func TestDescribeCluster(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	var resp wire.DescribeClusterResponse
	h.call(1, wire.KeyDescribeCluster, 2, &wire.DescribeClusterRequest{EndpointType: wire.EndpointBrokers}, &resp)
	if resp.ClusterID != "samsa" || resp.ControllerID != 1 || len(resp.Brokers) != 1 || resp.Brokers[0].Port != 9092 {
		t.Fatalf("%+v", resp)
	}
	h.call(1, wire.KeyDescribeCluster, 2, &wire.DescribeClusterRequest{EndpointType: wire.EndpointControllers}, &resp)
	if resp.ErrorCode != wire.ErrUnsupportedEndpointType {
		t.Fatalf("controllers: %+v", resp)
	}
}

// IncrementalAlterConfigs sets and deletes topic configs, all of a
// resource or none of it, and DescribeConfigs reports them.
func TestAlterConfigs(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	h.createTopic(1, "t", 1)
	alter := func(validate bool, res ...wire.AlterConfigsResource) []wire.AlterConfigsResult {
		t.Helper()
		var resp wire.IncrementalAlterConfigsResponse
		h.call(1, wire.KeyIncrementalAlterConfigs, 1, &wire.IncrementalAlterConfigsRequest{Resources: res, ValidateOnly: validate}, &resp)
		if len(resp.Results) != len(res) {
			t.Fatalf("%d results for %d resources", len(resp.Results), len(res))
		}
		return resp.Results
	}
	set := func(name, value string) wire.AlterableConfig {
		return wire.AlterableConfig{Name: name, Operation: wire.ConfigSet, Value: value}
	}
	topic := func(name string, cs ...wire.AlterableConfig) wire.AlterConfigsResource {
		return wire.AlterConfigsResource{Type: wire.ResourceTopic, Name: name, Configs: cs}
	}
	describe := func() map[string]wire.ConfigEntry {
		t.Helper()
		var resp wire.DescribeConfigsResponse
		h.call(1, wire.KeyDescribeConfigs, 4, &wire.DescribeConfigsRequest{Resources: []wire.DescribeConfigsResource{{Type: wire.ResourceTopic, Name: "t"}}}, &resp)
		m := map[string]wire.ConfigEntry{}
		for _, e := range resp.Results[0].Configs {
			m[e.Name] = e
		}
		return m
	}

	if r := alter(true, topic("t", set("retention.ms", "60000"))); r[0].ErrorCode != 0 {
		t.Fatalf("validate only: %+v", r)
	}
	if e := describe()["retention.ms"]; e.Value != "-1" {
		t.Fatalf("validate only changed the config: %+v", e)
	}
	r := alter(false,
		topic("t", set("retention.ms", "60000"), set("retention.bytes", "1000")),
		topic("missing", set("retention.ms", "1")),
		topic("t", set("retention.ms", "1")),
		wire.AlterConfigsResource{Type: wire.ResourceBroker, Name: "1"},
	)
	want := []int16{0, wire.ErrUnknownTopicOrPartition, wire.ErrInvalidRequest, wire.ErrInvalidRequest}
	for i, w := range want {
		if r[i].ErrorCode != w {
			t.Fatalf("result %d: %+v, want error %d", i, r[i], w)
		}
	}
	d := describe()
	if e := d["retention.ms"]; e.Value != "60000" || e.Source != wire.ConfigSourceDynamicTopic || e.ReadOnly {
		t.Fatalf("retention.ms: %+v", e)
	}
	if e := d["retention.bytes"]; e.Value != "1000" {
		t.Fatalf("retention.bytes: %+v", e)
	}
	for _, bad := range []wire.AlterableConfig{
		{Name: "retention.ms", Operation: wire.ConfigAppend, Value: "1"},
		set("retention.ms", "-2"),
		set("retention.ms", "soon"),
		set("cleanup.policy", "compact"), // part (b)
		set("no.such.config", "1"),
		{Name: "retention.ms", Operation: wire.ConfigSet, Null: true},
	} {
		// The valid change beside it must not apply either.
		if r := alter(false, topic("t", set("retention.bytes", "5"), bad)); r[0].ErrorCode != wire.ErrInvalidConfig {
			t.Fatalf("%+v: %+v, want INVALID_CONFIG", bad, r[0])
		}
	}
	if e := describe()["retention.bytes"]; e.Value != "1000" {
		t.Fatalf("a refused resource applied part of its changes: %+v", e)
	}
	alter(false, topic("t", wire.AlterableConfig{Name: "retention.ms", Operation: wire.ConfigDelete}))
	if e := describe()["retention.ms"]; e.Value != "-1" || e.Source != wire.ConfigSourceDefault {
		t.Fatalf("after delete: %+v", e)
	}
	// The configs are state: they survive a restart.
	h.start()
	h.open(1)
	if e := describe()["retention.bytes"]; e.Value != "1000" {
		t.Fatalf("after restart: %+v", e)
	}
}

// CreateTopics keeps the configs Samsa knows, refuses bad values, and
// refuses configs below format level 2.
func TestCreateTopicConfigs(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	h.createTopic(1, "first", 1) // the single node raises its level meanwhile
	if h.node.st.level < 2 {
		t.Fatalf("level %d", h.node.st.level)
	}
	create := func(name string, cs ...wire.TopicConfig) int16 {
		t.Helper()
		var resp wire.CreateTopicsResponse
		h.call(1, wire.KeyCreateTopics, 4, &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{{Name: name, NumPartitions: 1, ReplicationFactor: 1, Configs: cs}}}, &resp)
		return resp.Topics[0].ErrorCode
	}
	if code := create("t", wire.TopicConfig{Name: "retention.ms", Value: "1000"}, wire.TopicConfig{Name: "segment.ms", Value: "1"}, wire.TopicConfig{Name: "retention.bytes", Null: true}); code != 0 {
		t.Fatalf("create: %d", code)
	}
	if c := h.node.st.topics["t"].configs; len(c) != 1 || c["retention.ms"] != "1000" {
		t.Fatalf("configs %v", c)
	}
	if code := create("u", wire.TopicConfig{Name: "retention.ms", Value: "-5"}); code != wire.ErrInvalidConfig || h.node.st.topics["u"] != nil {
		t.Fatalf("bad value: %d", code)
	}
	h.node.st.level = 1
	if code := create("v", wire.TopicConfig{Name: "retention.ms", Value: "1"}); code != wire.ErrUnsupportedVersion {
		t.Fatalf("below level 2: %d", code)
	}
	if code := create("w"); code != 0 {
		t.Fatalf("no configs below level 2: %d", code)
	}
}

// A state from a snapshot older than version 8 gets its batch sizes from
// the log.
func TestAdoptFillsSizes(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	h.createTopic(1, "t", 1)
	h.produce(1, "t", 0, "a", "b")
	p := h.node.st.topics["t"].parts[0]
	want := p.batches[0].size
	p.batches[0].size = 0
	h.node.adopt(h.node.st)
	if want <= 0 || p.batches[0].size != want {
		t.Fatalf("size %d, want %d", p.batches[0].size, want)
	}
}
