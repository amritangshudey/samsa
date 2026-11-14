package broker

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/amritangshudey/samsa/internal/scram"
	"github.com/amritangshudey/samsa/internal/wire"
)

// sampleState has topics, batches, a group with members, protocols, an
// assignment and committed offsets, and a group with offsets only.
func sampleState(t *testing.T) *state {
	t.Helper()
	s := newState()
	g := &group{generation: 1, protocolType: "consumer", protocol: "range", leader: "a",
		members: []*member{{id: "a", sessionTimeout: 6000, rebalanceTimeout: 9000,
			protocols: []wire.GroupProtocol{{Name: "range", Metadata: []byte("m")}, {Name: "rr"}}}, {id: "b"}}}
	cmds := [][]byte{
		encodeCreateTopic("b", 2, nil),
		encodeCreateTopic("a", 1, nil),
		encodeAppend("a", 0, batch("x", "y")),
		encodeAppend("b", 1, batch("z")),
		encodeAppend("a", 0, batch("w")),
		encodeAppend("a", 0, batchAs(1<<47|3, 2, 0, "i", "j")),
		encodeAppend("a", 0, batchAs(1<<47|3, 2, 2, "k")),
		encodeAppend("b", 0, batchAs(9, 0, 0, "l")),
		encodeAppend("a", 0, batchAs(9, 0, 0, "m")),
		encodeOffsetCommit("solo", -1, "", 5, []offsetEntry{{"a", 0, committedOffset{offset: 2, metadata: "md"}}}),
		encodeGroupGeneration("g", g),
		encodeGroupAssignment("g", 1, []wire.MemberAssignment{{MemberID: "a", Assignment: []byte("x")}, {MemberID: "b", Assignment: []byte{}}}),
		encodeOffsetCommit("g", 1, "a", 7, []offsetEntry{{"a", 0, committedOffset{offset: 3, leaderEpoch: 4}}, {"b", 1, committedOffset{offset: 1}}}),
		encodeCreateACL(acl(resTopic, "a", patLiteral, "User:x", "*", opRead, permAllow)),
		encodeCreateACL(acl(resGroup, "g", patPrefixed, "User:*", "10.0.0.0/8", opAll, permDeny)),
		encodeCreateACL(acl(resTopic, "a", patLiteral, "User:x", "*", opRead, permAllow)), // again: no change
		encodeCreateACL(acl(resCluster, clusterName, patLiteral, "User:y", "*", opAlter, permAllow)),
		encodeDeleteACLs(wire.ACLFilter{ResourceType: resAny, PatternType: patAny, Principal: &userY, Operation: opAny, Permission: permAny}),
		encodeAlterCredentials(nil, []upsert{{"carol", cred(t, scram.SHA512, "c")}, {"carol", cred(t, scram.SHA256, "c")}, {"dave", cred(t, scram.SHA256, "d")}}),
		encodeAlterCredentials([][2]string{{"dave", scram.SHA256}}, []upsert{{"carol", cred(t, scram.SHA256, "c2")}}),
		encodeForwarded(2, 1<<63+5, encodeCreateACL(acl(resTopic, "b", patPrefixed, "User:z", "*", opWrite, permAllow))),
	}
	for i, cmd := range cmds {
		if res, err := s.apply(int64(i), cmd); err != nil || res.errCode != 0 {
			t.Fatalf("command %d: %+v, %v", i, res, err)
		}
	}
	return s
}

var userY = "User:y"

func cred(t *testing.T, mech, password string) scram.Credential {
	t.Helper()
	c, err := scram.NewCredential(mech, password, []byte("salt"), scram.MinIterations)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// Credentials: an upsert replaces the same mechanism, a delete removes one
// mechanism and the user with its last; a forwarded command applies its
// inner command and names its origin.
func TestCredentialCommands(t *testing.T) {
	s := sampleState(t)
	if len(s.creds) != 1 || len(s.creds["carol"]) != 2 || s.creds["carol"][0].Mechanism != scram.SHA256 ||
		!s.creds["carol"][0].CheckPassword("c2") || !s.creds["carol"][1].CheckPassword("c") {
		t.Fatalf("credentials %+v", s.creds)
	}
	if len(s.acls) != 3 {
		t.Fatalf("the forwarded ACL was not applied: %+v", s.acls)
	}
	res, err := s.apply(200, encodeForwarded(3, 77, encodeCreateACL(acl(resTopic, "q", patLiteral, "User:z", "*", opRead, permAllow))))
	if err != nil || res.origin != 3 || res.tag != 77 {
		t.Fatalf("forwarded result %+v, %v", res, err)
	}
	if _, err := s.apply(201, encodeForwarded(3, 78, encodeCreateTopic("x", 1, nil))); err == nil {
		t.Fatal("a forwarded command other than an ACL change was applied")
	}
}

// ACLs are created once however often they are asked for, and a delete
// returns what it removed.
func TestACLCommands(t *testing.T) {
	s := sampleState(t)
	if len(s.acls) != 3 || s.acls[0].Principal != "User:x" || s.acls[1].ResourceType != resGroup { // the third is forwarded
		t.Fatalf("ACLs %+v", s.acls)
	}
	res, err := s.apply(100, encodeDeleteACLs(wire.ACLFilter{ResourceType: resTopic, PatternType: patMatch, ResourceName: new(string), Operation: opAny, Permission: permAny}))
	if err != nil || len(res.acls) != 0 {
		t.Fatalf("a filter for topic \"\" removed %+v, %v", res.acls, err)
	}
	a := "a"
	res, _ = s.apply(101, encodeDeleteACLs(wire.ACLFilter{ResourceType: resTopic, PatternType: patMatch, ResourceName: &a, Operation: opAny, Permission: permAny}))
	if len(res.acls) != 1 || len(s.acls) != 2 {
		t.Fatalf("removed %+v, left %+v", res.acls, s.acls)
	}
	if _, err := s.apply(102, encodeCreateACL(acl(resTopic, "", patLiteral, "User:x", "*", opRead, permAllow))); err == nil {
		t.Fatal("an invalid ACL in the log was applied")
	}
}

func TestStateEncoding(t *testing.T) {
	for name, s := range map[string]*state{"empty": newState(), "sample": sampleState(t), "level 2": sampleLevel2(t)} {
		b := s.encode()
		got, err := decodeState(b)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got.hash() != s.hash() || !bytes.Equal(got.encode(), b) {
			t.Fatalf("%s: state changed through encode and decode", name)
		}
		for cut := range len(b) {
			if _, err := decodeState(b[:cut]); err == nil {
				t.Fatalf("%s: accepted %d of %d bytes", name, cut, len(b))
			}
		}
	}
	// The decoded state must go on behaving like the original.
	s, got := sampleState(t), sampleState(t)
	got, _ = decodeState(got.encode())
	for _, st := range []*state{s, got} {
		if res, _ := st.apply(100, encodeAppend("b", 1, batch("q"))); res.baseOffset != 1 {
			t.Fatalf("append after decode got offset %d", res.baseOffset)
		}
		if res, _ := st.apply(101, encodeOffsetCommit("g", 0, "a", 0, nil)); res.errCode != wire.ErrIllegalGeneration {
			t.Fatalf("a fenced commit after decode: %d", res.errCode)
		}
	}
	if s.hash() != got.hash() {
		t.Fatal("decoded state diverged after more commands")
	}
}

func FuzzDecodeState(f *testing.F) {
	f.Add(newState().encode())
	f.Fuzz(func(t *testing.T, b []byte) {
		s, err := decodeState(b)
		if err != nil {
			return
		}
		again, err := decodeState(s.encode())
		if err != nil || again.hash() != s.hash() {
			t.Fatalf("not stable: %v", err)
		}
	})
}

// BenchmarkRestart times a restart after n produced batches, with
// snapshots off and every 1000 entries. With them, the time must stay flat
// as the log grows (docs/design/a1-snapshots.md); without, it grows.
//
//	go test ./internal/broker -run '^$' -bench Restart
func BenchmarkRestart(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 50_000} {
		for _, every := range []int64{-1, 1000} {
			b.Run(fmt.Sprintf("entries=%d/snapshot=%d", n, every), func(b *testing.B) {
				// shortcut: the harness wants a *testing.T; a failure in it
				// ends the benchmark with 0 iterations instead of an error.
				h := newHarness(&testing.T{}, Config{SnapshotEvery: every, SegmentBytes: 64 << 10, MaxBatchBytes: 1000})
				h.open(1)
				h.createTopic(1, "t", 1)
				for i := range n {
					h.produce(1, "t", 0, fmt.Sprint(i))
				}
				b.ResetTimer()
				for range b.N {
					h.start()
				}
				b.StopTimer()
				if every > 0 && h.node.applied-1-h.node.raft.SnapshotIndex() > every+1 {
					b.Fatalf("restart replayed %d entries with a snapshot every %d", h.node.applied-1-h.node.raft.SnapshotIndex(), every)
				}
			})
		}
	}
}

// sampleLevel2 is sampleState at format level 2: topic configs and a
// relocated batch, which snapshot version 8 carries.
func sampleLevel2(t *testing.T) *state {
	t.Helper()
	s := sampleState(t)
	cmds := [][]byte{
		{cmdSetLevel, 2},
		encodeCreateTopic("c", 1, map[string]string{"retention.ms": "60000", "cleanup.policy": "delete"}),
		encodeTopicConfigs("a", []configChange{{name: "retention.bytes", value: "100"}}),
		encodeRelocate("a", 0, 0, 2, batch("x", "y")),
	}
	for i, cmd := range cmds {
		if res, err := s.apply(int64(100+i), cmd); err != nil || res.errCode != 0 {
			t.Fatalf("command %d: %+v, %v", i, res, err)
		}
	}
	if b := s.encode(); b[0] != 8 {
		t.Fatalf("snapshot version %d at level 2, want 8", b[0])
	}
	return s
}

// A relocation moves a reference only if it still points where the leader
// read the batch; trim then drops what nothing references below the cut,
// whatever the order of the references.
func TestRelocate(t *testing.T) {
	s := newState()
	apply := func(index int64, cmd []byte) result {
		t.Helper()
		res, err := s.apply(index, cmd)
		if err != nil {
			t.Fatalf("index %d: %v", index, err)
		}
		return res
	}
	apply(1, []byte{cmdSetLevel, 2})
	apply(2, encodeCreateTopic("t", 1, nil))
	apply(3, encodeAppend("t", 0, batch("a", "b"))) // offsets 0-1
	apply(4, encodeAppend("t", 0, batch("c")))      // offset 2
	apply(5, encodeAppend("t", 0, batch("d")))      // offset 3
	p := s.topics["t"].parts[0]

	apply(6, encodeRelocate("t", 0, 0, 3, batch("a", "b")))
	apply(7, encodeRelocate("t", 0, 0, 3, batch("a", "b"))) // stale: it moved to 6
	apply(8, encodeRelocate("t", 0, 3, 4, batch("d")))      // stale: offset 3 is at 5
	if got := []int64{p.batches[0].logIndex, p.batches[1].logIndex, p.batches[2].logIndex}; got[0] != 6 || got[1] != 4 || got[2] != 5 {
		t.Fatalf("log indexes %v, want [6 4 5]", got)
	}
	if _, err := s.apply(9, encodeRelocate("t", 0, 2, 4, batch("c", "x"))); err == nil {
		t.Fatal("a copy with another record count was accepted")
	}
	if _, err := s.apply(9, encodeRelocate("t", 0, 2, 9, batch("c"))); err == nil {
		t.Fatal("a copy from its own index was accepted")
	}
	// Cut 5: index 4 (offset 2) is below it; offsets 0-1 at 6 are not, but
	// offsets stay dense, so everything up to offset 2 goes.
	apply(10, encodeTrim(5))
	if len(p.batches) != 1 || p.batches[0].base != 3 || p.start != 3 {
		t.Fatalf("after trim: %+v, start %d", p.batches, p.start)
	}
	if got := appendBatch(encodeRelocate("t", 0, 0, 3, batch("a"))); !bytes.Equal(got, batch("a")) {
		t.Fatal("appendBatch does not return a relocated batch's bytes")
	}
}

// Topic configs: set, delete, and refused on a missing topic.
func TestTopicConfigCommands(t *testing.T) {
	s := newState()
	s.apply(1, encodeCreateTopic("t", 1, map[string]string{"retention.ms": "5"}))
	s.apply(2, encodeTopicConfigs("t", []configChange{{name: "retention.bytes", value: "9"}, {name: "retention.ms", delete: true}}))
	if c := s.topics["t"].configs; len(c) != 1 || c["retention.bytes"] != "9" {
		t.Fatalf("configs %v", c)
	}
	if res, _ := s.apply(3, encodeTopicConfigs("u", []configChange{{name: "retention.ms", value: "1"}})); res.errCode != wire.ErrUnknownTopicOrPartition {
		t.Fatalf("missing topic: %+v", res)
	}
	if _, err := s.apply(4, encodeTopicConfigs("t", []configChange{{name: "retention.ms", value: "x"}})); err == nil {
		t.Fatal("an invalid value was applied")
	}
}
