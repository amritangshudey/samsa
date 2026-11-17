package integration

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestJavaTransactions exercises Apache Kafka 4.4.0 KafkaProducer transactions
// (abort, commit, sendOffsetsToTransaction, KIP-890 EndTxn v5 epoch bump) and
// a read_committed KafkaConsumer.
func TestJavaTransactions(t *testing.T) {
	java, jars := javaAndJars(t)
	b := startBroker(t)
	const topic, txnID, group = "java-txn-topic", "java-txn-1", "java-txn-group"
	createTopic(t, franz(t, b), topic, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, java, "-cp", jars, "java/TxnMember.java", b.addr, topic, txnID, group)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("TxnMember.java failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "OK 3") {
		t.Fatalf("TxnMember.java unexpected output:\n%s", out)
	}

	// Verify sendOffsetsToTransaction committed offset 5 to group.
	cl := franz(t, b)
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Groups = []kmsg.OffsetFetchRequestGroup{{
		Group:  group,
		Topics: []kmsg.OffsetFetchRequestGroupTopic{{Topic: topic, Partitions: []int32{0}}},
	}}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("OffsetFetch: %v", err)
	}
	if got := resp.Groups[0].Topics[0].Partitions[0].Offset; got != 5 {
		t.Fatalf("committed txn offset = %d, want 5", got)
	}
}

// TestFranzGoTransactions exercises franz-go transactional produce (abort and
// commit), broker kill/restart, and read_committed consumption.
func TestFranzGoTransactions(t *testing.T) {
	b := startBroker(t)
	const topic = "franz-txn-topic"
	createTopic(t, franz(t, b), topic, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	prod, err := kgo.NewClient(
		kgo.SeedBrokers(b.addr),
		kgo.TransactionalID("franz-tx-1"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	// Transaction 1: abort.
	if err := prod.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction 1: %v", err)
	}
	if err := prod.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("aborted-1")}).FirstErr(); err != nil {
		t.Fatalf("ProduceSync 1: %v", err)
	}
	if err := prod.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("EndTransaction abort: %v", err)
	}

	// Transaction 2: commit.
	if err := prod.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction 2: %v", err)
	}
	if err := prod.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("committed-1")}).FirstErr(); err != nil {
		t.Fatalf("ProduceSync 2: %v", err)
	}
	if err := prod.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("EndTransaction commit: %v", err)
	}

	// Restart broker and verify read_committed consumer sees only committed-1.
	b.kill()
	b.start()

	cons, err := kgo.NewClient(
		kgo.SeedBrokers(b.addr),
		kgo.ConsumeTopics(topic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()

	var vals []string
	deadline := time.Now().Add(10 * time.Second)
	for len(vals) < 1 && time.Now().Before(deadline) {
		fetches := cons.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("PollFetches errors: %+v", errs)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			vals = append(vals, string(r.Value))
		})
	}
	if len(vals) != 1 || vals[0] != "committed-1" {
		t.Fatalf("read_committed values = %v, want [committed-1]", vals)
	}
}
