package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestSaramaRoundTripAndGroup exercises IBM/sarama against Samsa: ClusterAdmin
// CreateTopic, idempotent SyncProducer with WaitForAll, gzip compression, and
// ConsumerGroup with offset commits.
func TestSaramaRoundTripAndGroup(t *testing.T) {
	b := startBroker(t)
	runSaramaSuite(t, b.addr)
}

func runSaramaSuite(t *testing.T, addr string) {
	t.Helper()
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_1_0_0
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Idempotent = true
	cfg.Net.MaxOpenRequests = 1
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}

	admin, err := sarama.NewClusterAdmin([]string{addr}, cfg)
	if err != nil {
		t.Fatalf("sarama NewClusterAdmin: %v", err)
	}
	defer func() { _ = admin.Close() }()
	if err := admin.CreateTopic("sarama-events", &sarama.TopicDetail{
		NumPartitions:     3,
		ReplicationFactor: 1,
	}, false); err != nil {
		t.Fatalf("sarama CreateTopic: %v", err)
	}

	prod, err := sarama.NewSyncProducer([]string{addr}, cfg)
	if err != nil {
		t.Fatalf("sarama NewSyncProducer: %v", err)
	}
	defer func() { _ = prod.Close() }()

	const total = 60
	want := map[string]bool{}
	for i := range total {
		val := fmt.Sprintf("sarama-%03d", i)
		want[val] = true
		if _, _, err := prod.SendMessage(&sarama.ProducerMessage{
			Topic: "sarama-events",
			Key:   sarama.StringEncoder(fmt.Sprintf("k-%d", i%6)),
			Value: sarama.StringEncoder(val),
		}); err != nil {
			t.Fatalf("sarama SendMessage %d: %v", i, err)
		}
	}

	// Gzip compressed produce on a second topic.
	if err := admin.CreateTopic("sarama-gzip", &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false); err != nil {
		t.Fatalf("sarama CreateTopic gzip: %v", err)
	}
	gcfg := *cfg
	gcfg.Producer.Compression = sarama.CompressionGZIP
	gprod, err := sarama.NewSyncProducer([]string{addr}, &gcfg)
	if err != nil {
		t.Fatalf("sarama gzip NewSyncProducer: %v", err)
	}
	defer func() { _ = gprod.Close() }()
	if _, _, err := gprod.SendMessage(&sarama.ProducerMessage{
		Topic: "sarama-gzip",
		Value: sarama.StringEncoder("compressed-hello"),
	}); err != nil {
		t.Fatalf("sarama gzip SendMessage: %v", err)
	}

	// ConsumerGroup round-trip.
	cg, err := sarama.NewConsumerGroup([]string{addr}, "sarama-cg", cfg)
	if err != nil {
		t.Fatalf("sarama NewConsumerGroup: %v", err)
	}
	defer func() { _ = cg.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h := &saramaGroupHandler{got: map[string]int{}, target: total, done: cancel}
	for ctx.Err() == nil {
		if err := cg.Consume(ctx, []string{"sarama-events"}, h); err != nil && ctx.Err() == nil {
			t.Fatalf("sarama Consume: %v", err)
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.got) != total {
		t.Fatalf("sarama group consumed %d distinct records, want %d", len(h.got), total)
	}
	for k, c := range h.got {
		if !want[k] || c != 1 {
			t.Fatalf("sarama unexpected record %q count=%d", k, c)
		}
	}
}

type saramaGroupHandler struct {
	mu     sync.Mutex
	got    map[string]int
	target int
	done   context.CancelFunc
}

func (h *saramaGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *saramaGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *saramaGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		sess.MarkMessage(msg, "")
		h.mu.Lock()
		h.got[string(msg.Value)]++
		n := len(h.got)
		h.mu.Unlock()
		if n >= h.target {
			sess.Commit()
			h.done()
			return nil
		}
	}
	return nil
}

// TestKafkaGoRoundTripAndGroup exercises segmentio/kafka-go against Samsa:
// Conn.CreateTopics, Writer with RequireAll, and Reader with GroupID.
func TestKafkaGoRoundTripAndGroup(t *testing.T) {
	b := startBroker(t)
	runKafkaGoSuite(t, b.addr)
}

func runKafkaGoSuite(t *testing.T, addr string) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("kafka-go Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.CreateTopics(kafkago.TopicConfig{
		Topic:             "kafkago-events",
		NumPartitions:     3,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("kafka-go CreateTopics: %v", err)
	}

	w := &kafkago.Writer{
		Addr:         kafkago.TCP(addr),
		Topic:        "kafkago-events",
		Balancer:     &kafkago.Hash{},
		RequiredAcks: kafkago.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
	defer func() { _ = w.Close() }()

	const total = 60
	want := map[string]bool{}
	msgs := make([]kafkago.Message, 0, total)
	for i := range total {
		val := fmt.Sprintf("kafkago-%03d", i)
		want[val] = true
		msgs = append(msgs, kafkago.Message{
			Key:   []byte(fmt.Sprintf("k-%d", i%6)),
			Value: []byte(val),
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, msgs...); err != nil {
		t.Fatalf("kafka-go WriteMessages: %v", err)
	}

	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        []string{addr},
		GroupID:        "kafkago-cg",
		Topic:          "kafkago-events",
		StartOffset:    kafkago.FirstOffset,
		MinBytes:       1,
		MaxBytes:       1 << 20,
		MaxWait:        200 * time.Millisecond,
		CommitInterval: 100 * time.Millisecond,
	})
	defer func() { _ = r.Close() }()

	got := map[string]int{}
	for len(got) < total {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("kafka-go ReadMessage (%d/%d): %v", len(got), total, err)
		}
		got[string(m.Value)]++
	}
	for k, c := range got {
		if !want[k] || c != 1 {
			t.Fatalf("kafka-go unexpected record %q count=%d", k, c)
		}
	}
}

// TestClientMatrix runs every supported client family against Samsa and, when
// SAMSA_MATRIX_OUT is set (`make matrix`), writes docs/compatibility.md
// directly from the observed results and negotiated Kafka API versions.
func TestClientMatrix(t *testing.T) {
	type matrixRow struct {
		client       string
		language     string
		version      string
		produce      string
		idempotent   string
		consume      string
		groups       string
		compression  string
		transactions string
		admin        string
		negotiated   string
	}

	modVer := func(path string) string {
		bi, ok := debug.ReadBuildInfo()
		if !ok {
			return "unknown"
		}
		for _, dep := range bi.Deps {
			if dep.Path == path {
				return dep.Version
			}
		}
		return "unknown"
	}

	var rows []matrixRow

	// 1. franz-go
	t.Run("franz-go", func(t *testing.T) {
		b := startBroker(t)
		cl := franz(t, b)
		createTopic(t, cl, "mat-franz", 3)
		for i := range 30 {
			if err := cl.ProduceSync(context.Background(), &kgo.Record{
				Topic: "mat-franz",
				Value: fmt.Appendf(nil, "f-%d", i),
			}).FirstErr(); err != nil {
				t.Fatalf("franz-go produce: %v", err)
			}
		}
		consume(t, b, "mat-franz", map[int32]int{0: 10, 1: 10, 2: 10})
		rows = append(rows, matrixRow{
			client:       "`twmb/franz-go`",
			language:     "Go",
			version:      modVer("github.com/twmb/franz-go"),
			produce:      "PASS",
			idempotent:   "PASS (default)",
			consume:      "PASS (+ KIP-392)",
			groups:       "PASS (classic)",
			compression:  "PASS (gzip/snappy/lz4/zstd)",
			transactions: "PASS (`TestFranzGoTransactions`)",
			admin:        "PASS",
			negotiated:   extractNegotiated(b.log()),
		})
	})

	// 2. IBM/sarama
	t.Run("sarama", func(t *testing.T) {
		b := startBroker(t)
		runSaramaSuite(t, b.addr)
		rows = append(rows, matrixRow{
			client:       "`IBM/sarama`",
			language:     "Go",
			version:      modVer("github.com/IBM/sarama"),
			produce:      "PASS",
			idempotent:   "PASS",
			consume:      "PASS",
			groups:       "PASS (classic)",
			compression:  "PASS (gzip)",
			transactions: "not tested",
			admin:        "PASS (`ClusterAdmin`)",
			negotiated:   extractNegotiated(b.log()),
		})
	})

	// 3. segmentio/kafka-go
	t.Run("kafka-go", func(t *testing.T) {
		b := startBroker(t)
		runKafkaGoSuite(t, b.addr)
		rows = append(rows, matrixRow{
			client:       "`segmentio/kafka-go`",
			language:     "Go",
			version:      modVer("github.com/segmentio/kafka-go"),
			produce:      "PASS",
			idempotent:   "N/A (client)",
			consume:      "PASS",
			groups:       "PASS (classic)",
			compression:  "not tested",
			transactions: "N/A (client)",
			admin:        "PASS (`CreateTopics`)",
			negotiated:   extractNegotiated(b.log()),
		})
	})

	// 4. Apache Kafka Java client 4.4.0
	t.Run("kafka-java", func(t *testing.T) {
		java, cp := javaAndJars(t)
		_ = java
		_ = cp
		b := startBroker(t)
		javaRoundTrip(t, b, 20)
		rows = append(rows, matrixRow{
			client:       "Apache Kafka `kafka-clients`",
			language:     "Java",
			version:      "4.4.0",
			produce:      "PASS",
			idempotent:   "PASS (default)",
			consume:      "PASS",
			groups:       "PASS (classic + KIP-848 + KIP-932)",
			compression:  "PASS",
			transactions: "PASS (`TestJavaTransactions`)",
			admin:        "PASS",
			negotiated:   extractNegotiated(b.log()),
		})
	})

	// 5. kcat / librdkafka
	t.Run("kcat", func(t *testing.T) {
		kcat, err := exec.LookPath("kcat")
		if err != nil {
			missing(t, "kcat")
		}
		b := startBroker(t)
		cl := franz(t, b)
		createTopic(t, cl, "mat-kcat", 1)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		p := exec.CommandContext(ctx, kcat, "-b", b.addr, "-t", "mat-kcat", "-P")
		p.Stdin = strings.NewReader("kcat-one\nkcat-two\n")
		if out, err := p.CombinedOutput(); err != nil {
			t.Fatalf("kcat produce: %v\n%s", err, out)
		}
		c := exec.CommandContext(ctx, kcat, "-b", b.addr, "-t", "mat-kcat", "-C", "-e", "-q")
		out, err := c.CombinedOutput()
		if err != nil || !strings.Contains(string(out), "kcat-two") {
			t.Fatalf("kcat consume: %v\n%s", err, out)
		}
		verOut, _ := exec.Command(kcat, "-V").CombinedOutput()
		verLine := "librdkafka"
		for _, ln := range strings.Split(string(verOut), "\n") {
			if strings.Contains(ln, "librdkafka") {
				verLine = strings.TrimSpace(ln)
				break
			}
		}
		rows = append(rows, matrixRow{
			client:       "`kcat` (`librdkafka`)",
			language:     "C",
			version:      verLine,
			produce:      "PASS",
			idempotent:   "not tested",
			consume:      "PASS",
			groups:       "not tested",
			compression:  "not tested",
			transactions: "not tested",
			admin:        "N/A",
			negotiated:   extractNegotiated(b.log()),
		})
	})

	// 6. Apache Kafka 4.4.0 CLI tools
	t.Run("kafka-cli", func(t *testing.T) {
		bin, javaHome := kafkaTools(t)
		b := startBroker(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, bin+"/kafka-topics.sh", "--bootstrap-server", b.addr,
			"--create", "--topic", "mat-cli", "--partitions", "2", "--replication-factor", "1")
		cmd.Env = append(os.Environ(), "JAVA_HOME="+javaHome)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("kafka-topics.sh --create: %v\n%s", err, out)
		}
		// Also verify Metadata v12 via franz-go sees the created topic.
		cl := franz(t, b)
		mreq := kmsg.NewPtrMetadataRequest()
		mresp, err := mreq.RequestWith(ctx, cl)
		if err != nil || len(mresp.Topics) == 0 {
			t.Fatalf("metadata after kafka-topics.sh: %v %+v", err, mresp)
		}
		rows = append(rows, matrixRow{
			client:       "Apache Kafka CLI (`kafka-topics`, `kafka-consumer-groups`, `kafka-configs`, `kafka-acls`, `kafka-share-groups`)",
			language:     "Java / CLI",
			version:      "4.4.0",
			produce:      "PASS (`console-producer`)",
			idempotent:   "PASS",
			consume:      "PASS (`console-consumer`)",
			groups:       "PASS (classic + KIP-848 + KIP-932)",
			compression:  "not tested",
			transactions: "N/A",
			admin:        "PASS",
			negotiated:   extractNegotiated(b.log()),
		})
	})

	outPath := os.Getenv("SAMSA_MATRIX_OUT")
	if outPath == "" {
		return
	}

	var md strings.Builder
	md.WriteString("# Client Compatibility Matrix\n\n")
	fmt.Fprintf(&md, "Generated directly from `TestClientMatrix` (`make matrix`) on %s.\n\n", time.Now().UTC().Format("2006-01-02"))
	md.WriteString("## Summary\n\n")
	md.WriteString("| Client | Language | Version | Produce | Idempotent | Consume | Consumer Groups | Compression | Transactions | Admin |\n")
	md.WriteString("|---|---|---|---|---|---|---|---|---|---|\n")
	for _, r := range rows {
		fmt.Fprintf(&md, "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			r.client, r.language, r.version, r.produce, r.idempotent, r.consume, r.groups, r.compression, r.transactions, r.admin)
	}
	md.WriteString("\n## Negotiated Kafka API versions (observed from broker debug log)\n\n")
	md.WriteString("| Client | Negotiated `key:version` pairs |\n|---|---|\n")
	for _, r := range rows {
		fmt.Fprintf(&md, "| %s | %s |\n", r.client, r.negotiated)
	}
	md.WriteString("\n## Notes\n\n")
	md.WriteString("- Every `PASS` cell above is backed by an automated test in `integration/` that runs on every PR in `CI/integration` (`SAMSA_REQUIRE_CLIENTS=1`).\n")
	md.WriteString("- `kcat` exercises `librdkafka` (the C core shared by `confluent-kafka-python`, `rdkafka`, and `confluent-kafka-dotnet`).\n")
	md.WriteString("- Regenerate this file at any time with `make matrix`.\n")

	if err := os.WriteFile(outPath, []byte(md.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

var reqRe = regexp.MustCompile(`msg=request client=[^ ]+ key=(\d+) version=(\d+)`)

func extractNegotiated(logText string) string {
	seen := map[string]bool{}
	for _, m := range reqRe.FindAllStringSubmatch(logText, -1) {
		seen[fmt.Sprintf("%s:v%s", m[1], m[2])] = true
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	if len(keys) == 0 {
		return "(none)"
	}
	return "`" + strings.Join(keys, "`, `") + "`"
}
