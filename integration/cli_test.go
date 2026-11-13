package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// kafkaTools finds the Kafka distribution's CLI scripts (`make integration`
// downloads it) and a JAVA_HOME for them, or skips (fails, in CI).
func kafkaTools(t *testing.T) (bin, javaHome string) {
	t.Helper()
	dirs, _ := filepath.Glob(".cache/kafka_2.13-*/bin")
	if len(dirs) == 0 {
		missing(t, "the Kafka distribution (run `make integration` to download it)")
	}
	javaHome = os.Getenv("JAVA_HOME")
	if javaHome == "" {
		java, _ := javaAndJars(t)
		if p, err := filepath.EvalSymlinks(java); err == nil {
			java = p
		}
		javaHome = filepath.Dir(filepath.Dir(java))
	}
	bin, err := filepath.Abs(dirs[0])
	if err != nil {
		t.Fatal(err)
	}
	return bin, javaHome
}

// TestKafkaCLI runs the standard Kafka 4.4.0 tools against a three-node
// cluster: every command of docs/design/b4-operations.md, section 1.
func TestKafkaCLI(t *testing.T) {
	bin, javaHome := kafkaTools(t)
	c := startCluster(t, 3)
	var addrs []string
	for id := int32(1); id <= 3; id++ {
		addrs = append(addrs, c.nodes[id].addr)
	}
	bootstrap := strings.Join(addrs, ",")
	run := func(tool string, args ...string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		// Last: kafka-cluster takes its subcommand first.
		cmd := exec.CommandContext(ctx, filepath.Join(bin, tool), append(args, "--bootstrap-server", bootstrap)...)
		cmd.Env = append(os.Environ(), "JAVA_HOME="+javaHome)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s %v: %v\n%s", tool, args, err, out)
		}
		return string(out)
	}
	expect := func(out, pattern string) {
		t.Helper()
		if !regexp.MustCompile(pattern).MatchString(out) {
			t.Fatalf("output does not match %q:\n%s", pattern, out)
		}
	}
	cl := c.client()
	c.leader(cl)

	expect(run("kafka-cluster.sh", "cluster-id"), `Cluster ID: samsa`)
	expect(run("kafka-topics.sh", "--create", "--topic", "cli", "--partitions", "2", "--replication-factor", "3"), `Created topic cli\.`)
	expect(run("kafka-topics.sh", "--list"), `(?m)^cli$`)
	expect(run("kafka-topics.sh", "--describe", "--topic", "cli"), `PartitionCount: 2\s+ReplicationFactor: 3`)
	run("kafka-topics.sh", "--alter", "--topic", "cli", "--partitions", "3")
	expect(run("kafka-topics.sh", "--describe", "--topic", "cli"), `PartitionCount: 3`)

	expect(run("kafka-configs.sh", "--describe", "--entity-type", "topics", "--entity-name", "cli", "--all"), `retention\.ms=-1`)
	leader := c.leader(cl)
	expect(run("kafka-configs.sh", "--describe", "--entity-type", "brokers", "--entity-name", fmt.Sprint(leader), "--all"), `samsa\.sync=always`)

	var recs []*kgo.Record
	for i := range 8 {
		recs = append(recs, &kgo.Record{Topic: "cli", Partition: 0, Value: fmt.Appendf(nil, "v%d", i)})
	}
	if err := cl.ProduceSync(context.Background(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	offsets := filepath.Join(t.TempDir(), "offsets.json")
	if err := os.WriteFile(offsets, []byte(`{"partitions":[{"topic":"cli","partition":0,"offset":5}],"version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	expect(run("kafka-delete-records.sh", "--offset-json-file", offsets), `partition: cli-0\s+low_watermark: 5`)

	// What is left of partition 0, read without a group, then by group
	// clig, which commits and leaves.
	out := run("kafka-console-consumer.sh", "--topic", "cli", "--partition", "0", "--offset", "earliest", "--max-messages", "3", "--timeout-ms", "30000")
	expect(out, `(?s)v5.*v6.*v7`)
	if strings.Contains(out, "v4") {
		t.Fatalf("a deleted record was read:\n%s", out)
	}
	expect(run("kafka-console-consumer.sh", "--topic", "cli", "--group", "clig", "--from-beginning", "--max-messages", "3", "--timeout-ms", "30000"), `(?s)v5.*v6.*v7`)
	expect(run("kafka-consumer-groups.sh", "--list"), `(?m)^clig$`)
	expect(run("kafka-consumer-groups.sh", "--describe", "--group", "clig"), `clig\s+cli\s+0\s+8\s+8\s+0`)
	expect(run("kafka-consumer-groups.sh", "--describe", "--group", "clig", "--state"), `clig\s+.*\s+Empty\s+0`)

	// Deleted and created again under its name, the topic starts empty and
	// the group has no offset for it.
	run("kafka-topics.sh", "--delete", "--topic", "cli")
	if out := run("kafka-topics.sh", "--list"); regexp.MustCompile(`(?m)^cli$`).MatchString(out) {
		t.Fatalf("deleted topic still listed:\n%s", out)
	}
	expect(run("kafka-topics.sh", "--create", "--topic", "cli", "--partitions", "1"), `Created topic cli\.`)
	if out := run("kafka-consumer-groups.sh", "--describe", "--group", "clig"); strings.Contains(out, "cli ") {
		t.Fatalf("the group kept an offset of the deleted topic:\n%s", out)
	}
	expect(run("kafka-get-offsets.sh", "--topic", "cli"), `(?m)^cli:0:0$`)
}
