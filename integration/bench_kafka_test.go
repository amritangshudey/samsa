package integration

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestBenchKafka compares Samsa against Apache Kafka 4.4.0 (KRaft) on the same
// machine with the same franz-go client and workload (docs/design/g8-kafka-comparison.md,
// ADR 0040). Runs only with SAMSA_BENCH_KAFKA=1 (`make bench-kafka`).
func TestBenchKafka(t *testing.T) {
	if os.Getenv("SAMSA_BENCH_KAFKA") == "" {
		t.Skip("set SAMSA_BENCH_KAFKA=1 to run the Kafka comparison benchmark")
	}
	kafkaBin, javaHome := kafkaTools(t)
	throughputFor := envDuration(t, "SAMSA_BENCH_THROUGHPUT", 5*time.Second)
	latencyFor := envDuration(t, "SAMSA_BENCH_LATENCY", 5*time.Second)
	const reps = 3

	var report strings.Builder

	// Section A: Cold startup time and idle RSS.
	t.Log("=== Section A: Cold startup & idle RSS ===")
	report.WriteString("### Cold startup and idle RSS\n\n")
	report.WriteString("| Broker | Startup ms (median, min–max) | Idle RSS MiB (median) |\n|---|---|---|\n")
	for _, kind := range []string{"samsa-1-always", "kafka-kraft-default"} {
		var starts []float64
		var rsses []float64
		for i := range reps {
			var ms, rss float64
			t.Run(fmt.Sprintf("startup-%s-%d", kind, i), func(t *testing.T) {
				if kind == "samsa-1-always" {
					ms, rss = measureSamsaStartup(t)
				} else {
					ms, rss = measureKafkaStartup(t, kafkaBin, javaHome, false)
				}
			})
			starts = append(starts, ms)
			rsses = append(rsses, rss)
		}
		slices.Sort(starts)
		slices.Sort(rsses)
		row := fmt.Sprintf("| %s | %.1f (%.1f–%.1f) | %.1f |", kind, starts[1], starts[0], starts[2], rsses[1])
		t.Log(row)
		report.WriteString(row + "\n")
	}

	// Section B: Produce throughput & open-loop latency.
	t.Log("=== Section B: Produce throughput & open-loop latency ===")
	report.WriteString("\n### Produce throughput and open-loop latency (3 partitions, acks=all, idempotence on)\n\n")
	report.WriteString("| Target | Record | Throughput, rec/s (min–max) | MB/s | Latency rate, rec/s | p50 ms | p99 ms | p99.9 ms | max ms |\n|---|---|---|---|---|---|---|---|---|\n")
	type targetSpec struct {
		name      string
		kind      string // "samsa", "samsa-diskless", "kafka"
		nodes     int
		policy    string
		flushEach bool
		warmup    time.Duration
	}
	targets := []targetSpec{
		{name: "Samsa 1-node (sync=always)", kind: "samsa", nodes: 1, policy: "always", warmup: time.Second},
		{name: "Samsa 1-node (sync=never)", kind: "samsa", nodes: 1, policy: "never", warmup: time.Second},
		{name: "Kafka 4.4.0 KRaft 1-node (default)", kind: "kafka", nodes: 1, flushEach: false, warmup: 3 * time.Second},
		{name: "Kafka 4.4.0 KRaft 1-node (flush=1)", kind: "kafka", nodes: 1, flushEach: true, warmup: 3 * time.Second},
		{name: "Samsa 3-node (sync=always)", kind: "samsa", nodes: 3, policy: "always", warmup: time.Second},
		{name: "Samsa 3-node diskless (file://)", kind: "samsa-diskless", nodes: 3, policy: "always", warmup: time.Second},
	}
	for _, tgt := range targets {
		for _, size := range []int{100, 1024} {
			var runs []benchRun
			for i := range reps {
				var r benchRun
				t.Run(fmt.Sprintf("produce-%s-%dB-%d", sanitizeName(tgt.name), size, i), func(t *testing.T) {
					var cl *kgo.Client
					switch tgt.kind {
					case "samsa":
						c := startCluster(t, tgt.nodes, "-sync", tgt.policy, "-log-level", "warn")
						cl = c.client(kgo.MaxBufferedRecords(10000))
						benchTopic(t, cl)
					case "samsa-diskless":
						objDir := t.TempDir()
						c := startCluster(t, tgt.nodes, "-sync", "always", "-log-level", "warn", "-object-store-endpoint", "file://"+objDir, "-object-store-bucket", "bench")
						cl = c.client(kgo.MaxBufferedRecords(10000))
						benchTopicConfig(t, cl, "bench", 3, map[string]string{"diskless.enabled": "true"})
					case "kafka":
						kb := startKafkaBroker(t, kafkaBin, javaHome, tgt.flushEach)
						cl = kafkaClient(t, kb.addr, kgo.MaxBufferedRecords(10000))
						benchTopicConfig(t, cl, "bench", 3, nil)
					}
					value := make([]byte, size)
					closedLoop(t, cl, value, tgt.warmup)
					r.throughput = closedLoop(t, cl, value, throughputFor)
					r.rate = r.throughput / 2
					r.latencies, r.achieved = openLoop(t, cl, value, r.rate, latencyFor)
				})
				runs = append(runs, r)
			}
			row := summariseNamed(tgt.name, size, runs)
			t.Log(row)
			report.WriteString(row + "\n")
		}
	}

	// Section C: Sequential consume throughput (50,000 x 1 KiB = 50 MiB).
	t.Log("=== Section C: Sequential consume throughput (50,000 x 1 KiB) ===")
	report.WriteString("\n### Sequential consume throughput (50,000 × 1 KiB = 51.2 MB, 3 partitions)\n\n")
	report.WriteString("| Target | Consume rec/s | Consume MB/s | Local Raft disk KiB | Object store KiB |\n|---|---|---|---|---|\n")
	consumeTargets := []targetSpec{
		{name: "Samsa 1-node (sync=always)", kind: "samsa", nodes: 1, policy: "always"},
		{name: "Kafka 4.4.0 KRaft 1-node (default)", kind: "kafka", nodes: 1, flushEach: false},
		{name: "Samsa 3-node (sync=always)", kind: "samsa", nodes: 3, policy: "always"},
		{name: "Samsa 3-node diskless (file://)", kind: "samsa-diskless", nodes: 3, policy: "always"},
	}
	const consumeN = 50000
	for _, tgt := range consumeTargets {
		t.Run("consume-"+sanitizeName(tgt.name), func(t *testing.T) {
			var addrs []string
			var dataDir, objDir string
			switch tgt.kind {
			case "samsa":
				c := startCluster(t, tgt.nodes, "-sync", tgt.policy, "-log-level", "warn")
				addrs = clusterAddrs(c)
				dataDir = c.nodes[1].dir
				cl := c.client(kgo.MaxBufferedRecords(10000))
				benchTopic(t, cl)
				produceExact(t, cl, "bench", 3, consumeN, 1024)
			case "samsa-diskless":
				objDir = t.TempDir()
				c := startCluster(t, tgt.nodes, "-sync", "always", "-log-level", "warn", "-object-store-endpoint", "file://"+objDir, "-object-store-bucket", "bench")
				addrs = clusterAddrs(c)
				dataDir = c.nodes[1].dir
				cl := c.client(kgo.MaxBufferedRecords(10000))
				benchTopicConfig(t, cl, "bench", 3, map[string]string{"diskless.enabled": "true"})
				produceExact(t, cl, "bench", 3, consumeN, 1024)
			case "kafka":
				kb := startKafkaBroker(t, kafkaBin, javaHome, false)
				addrs = []string{kb.addr}
				dataDir = kb.dir
				cl := kafkaClient(t, kb.addr, kgo.MaxBufferedRecords(10000))
				benchTopicConfig(t, cl, "bench", 3, nil)
				produceExact(t, cl, "bench", 3, consumeN, 1024)
			}
			recPerSec := consumeExact(t, addrs, "bench", consumeN)
			mbPerSec := recPerSec * 1024 / 1e6
			diskKiB := dirSizeBytes(dataDir) / 1024
			objKiB := int64(0)
			if objDir != "" {
				objKiB = dirSizeBytes(objDir) / 1024
			}
			row := fmt.Sprintf("| %s | %.0f | %.1f | %d | %d |", tgt.name, recPerSec, mbPerSec, diskKiB, objKiB)
			t.Log(row)
			report.WriteString(row + "\n")
		})
	}

	// Section D: Partition scaling (10, 100, 500 partitions).
	t.Log("=== Section D: Partition scaling ===")
	report.WriteString("\n### Partition scaling (10, 100, 500 partitions; 10 × 256 B records per partition)\n\n")
	report.WriteString("| Broker | Partitions | Create topic ms | Produce all ms | RSS MiB | Data dir KiB |\n|---|---|---|---|---|---|\n")
	for _, pCount := range []int32{10, 100, 500} {
		for _, kind := range []string{"Samsa 1-node (sync=always)", "Kafka 4.4.0 KRaft 1-node"} {
			t.Run(fmt.Sprintf("partitions-%d-%s", pCount, sanitizeName(kind)), func(t *testing.T) {
				var cl *kgo.Client
				var pid int
				var dataDir string
				if strings.HasPrefix(kind, "Samsa") {
					b := startBroker(t, "-sync", "always", "-log-level", "warn")
					cl = franz(t, b, kgo.MaxBufferedRecords(10000))
					pid = b.cmd.Process.Pid
					dataDir = b.dir
				} else {
					kb := startKafkaBroker(t, kafkaBin, javaHome, false)
					cl = kafkaClient(t, kb.addr, kgo.MaxBufferedRecords(10000))
					pid = kb.cmd.Process.Pid
					dataDir = kb.dir
				}
				t0 := time.Now()
				benchTopicConfig(t, cl, "scale", pCount, nil)
				createMs := float64(time.Since(t0)) / 1e6

				t1 := time.Now()
				produceExact(t, cl, "scale", int(pCount), int(pCount)*10, 256)
				prodMs := float64(time.Since(t1)) / 1e6

				rssMiB := processRSSMiB(pid)
				diskKiB := dirSizeBytes(dataDir) / 1024
				row := fmt.Sprintf("| %s | %d | %.1f | %.1f | %.1f | %d |", kind, pCount, createMs, prodMs, rssMiB, diskKiB)
				t.Log(row)
				report.WriteString(row + "\n")
			})
		}
	}

	if out := os.Getenv("SAMSA_BENCH_KAFKA_OUT"); out != "" {
		if err := os.WriteFile(out, []byte(report.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

type kafkaBroker struct {
	addr string
	dir  string
	cmd  *exec.Cmd
	logs bytes.Buffer
}

func startKafkaBroker(t *testing.T, kafkaBin, javaHome string, flushEach bool) *kafkaBroker {
	t.Helper()
	kb, _ := startKafkaBrokerTimed(t, kafkaBin, javaHome, flushEach)
	return kb
}

func startKafkaBrokerTimed(t *testing.T, kafkaBin, javaHome string, flushEach bool) (*kafkaBroker, time.Duration) {
	t.Helper()
	clientAddr := freeAddr(t)
	ctrlAddr := freeAddr(t)
	dir := t.TempDir()
	logDir := filepath.Join(dir, "kraft-logs")
	propsPath := filepath.Join(dir, "server.properties")

	var props strings.Builder
	fmt.Fprintf(&props, "process.roles=broker,controller\n")
	fmt.Fprintf(&props, "node.id=1\n")
	fmt.Fprintf(&props, "controller.quorum.voters=1@%s\n", ctrlAddr)
	fmt.Fprintf(&props, "listeners=PLAINTEXT://%s,CONTROLLER://%s\n", clientAddr, ctrlAddr)
	fmt.Fprintf(&props, "inter.broker.listener.name=PLAINTEXT\n")
	fmt.Fprintf(&props, "advertised.listeners=PLAINTEXT://%s\n", clientAddr)
	fmt.Fprintf(&props, "controller.listener.names=CONTROLLER\n")
	fmt.Fprintf(&props, "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT\n")
	fmt.Fprintf(&props, "log.dirs=%s\n", logDir)
	fmt.Fprintf(&props, "num.partitions=1\n")
	fmt.Fprintf(&props, "offsets.topic.replication.factor=1\n")
	fmt.Fprintf(&props, "transaction.state.log.replication.factor=1\n")
	fmt.Fprintf(&props, "transaction.state.log.min.isr=1\n")
	fmt.Fprintf(&props, "share.coordinator.state.topic.replication.factor=1\n")
	fmt.Fprintf(&props, "share.coordinator.state.topic.min.isr=1\n")
	fmt.Fprintf(&props, "group.initial.rebalance.delay.ms=0\n")
	if flushEach {
		fmt.Fprintf(&props, "log.flush.interval.messages=1\n")
	}
	if err := os.WriteFile(propsPath, []byte(props.String()), 0o600); err != nil {
		t.Fatal(err)
	}

	env := append(os.Environ(), "JAVA_HOME="+javaHome, "KAFKA_HEAP_OPTS=-Xmx1G -Xms1G")

	// Format KRaft storage directory.
	uuidCmd := exec.Command(filepath.Join(kafkaBin, "kafka-storage.sh"), "random-uuid")
	uuidCmd.Env = env
	uuidOut, err := uuidCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("kafka-storage random-uuid: %v\n%s", err, uuidOut)
	}
	clusterID := strings.TrimSpace(string(uuidOut))

	fmtCmd := exec.Command(filepath.Join(kafkaBin, "kafka-storage.sh"), "format", "-t", clusterID, "-c", propsPath)
	fmtCmd.Env = env
	if out, err := fmtCmd.CombinedOutput(); err != nil {
		t.Fatalf("kafka-storage format: %v\n%s", err, out)
	}

	kb := &kafkaBroker{addr: clientAddr, dir: logDir}
	cmd := exec.Command(filepath.Join(kafkaBin, "kafka-server-start.sh"), propsPath)
	cmd.Env = env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = &kb.logs
	cmd.Stderr = &kb.logs
	t0 := time.Now()
	if err := cmd.Start(); err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	kb.cmd = cmd
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
			done := make(chan struct{})
			go func() { _ = cmd.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				<-done
			}
		}
	})

	// Wait until Kafka answers a Metadata request.
	deadline := time.Now().Add(60 * time.Second)
	for {
		if conn, err := net.DialTimeout("tcp", clientAddr, 200*time.Millisecond); err == nil {
			_ = conn.Close()
			cl, err := kgo.NewClient(kgo.SeedBrokers(clientAddr))
			if err == nil {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				resp, rerr := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
				cancel()
				cl.Close()
				if rerr == nil && len(resp.Brokers) > 0 {
					return kb, time.Since(t0)
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("kafka did not become ready on %s:\n%s", clientAddr, last(kb.logs.String(), 4000))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func measureSamsaStartup(t *testing.T) (ms, rssMiB float64) {
	t.Helper()
	addr := freeAddr(t)
	dir := t.TempDir()
	cmd := exec.Command(samsaBin(), "-listen", addr, "-data", dir, "-sync", "always", "-log-level", "warn")
	var logs bytes.Buffer
	cmd.Stdout = &logs
	cmd.Stderr = &logs
	t0 := time.Now()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	deadline := time.Now().Add(15 * time.Second)
	for {
		if conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond); err == nil {
			_ = conn.Close()
			cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
			if err == nil {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				resp, rerr := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
				cancel()
				cl.Close()
				if rerr == nil && len(resp.Brokers) > 0 {
					elapsed := time.Since(t0)
					return float64(elapsed) / 1e6, processRSSMiB(cmd.Process.Pid)
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("samsa did not become ready:\n%s", logs.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func measureKafkaStartup(t *testing.T, kafkaBin, javaHome string, flushEach bool) (ms, rssMiB float64) {
	t.Helper()
	kb, elapsed := startKafkaBrokerTimed(t, kafkaBin, javaHome, flushEach)
	return float64(elapsed) / 1e6, processRSSMiB(kb.cmd.Process.Pid)
}

func kafkaClient(t *testing.T, addr string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	all := append([]kgo.Opt{kgo.SeedBrokers(addr)}, opts...)
	cl, err := kgo.NewClient(all...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func benchTopicConfig(t *testing.T, cl *kgo.Client, topic string, partitions int32, cfg map[string]string) {
	t.Helper()
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = topic, partitions, -1
	for k, v := range cfg {
		val := v
		c := kmsg.NewCreateTopicsRequestTopicConfig()
		c.Name = k
		c.Value = &val
		rt.Configs = append(rt.Configs, c)
	}
	req.Topics = append(req.Topics, rt)
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := req.RequestWith(ctx, cl)
		cancel()
		if err == nil && len(resp.Topics) == 1 && (resp.Topics[0].ErrorCode == 0 || resp.Topics[0].ErrorCode == 36) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("create topic %s: %v %+v", topic, err, resp)
		}
	}
}

func produceExact(t *testing.T, cl *kgo.Client, topic string, partitions, total, size int) {
	t.Helper()
	val := make([]byte, size)
	var firstErr error
	errCh := make(chan error, 1)
	for i := range total {
		cl.Produce(context.Background(), &kgo.Record{
			Topic:     topic,
			Partition: int32(i % partitions),
			Value:     val,
		}, func(_ *kgo.Record, err error) {
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		})
	}
	if err := cl.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case firstErr = <-errCh:
	default:
	}
	if firstErr != nil {
		t.Fatalf("produceExact: %v", firstErr)
	}
}

func consumeExact(t *testing.T, addrs []string, topic string, total int) float64 {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addrs...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	got := 0
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for got < total {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("consumeExact fetch errors: %+v", errs)
		}
		fetches.EachRecord(func(_ *kgo.Record) { got++ })
	}
	return float64(got) / time.Since(start).Seconds()
}

func clusterAddrs(c *cluster) []string {
	var out []string
	for _, n := range c.nodes {
		out = append(out, n.addr)
	}
	return out
}

func processRSSMiB(pid int) float64 {
	// Sum RSS of pid and any direct children (kafka-server-start.sh execs java,
	// but pgrep handles wrapper processes if any).
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0
	}
	kb, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil {
		return 0
	}
	return kb / 1024.0
}

func dirSizeBytes(root string) int64 {
	var sum int64
	_ = filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			sum += info.Size()
		}
		return nil
	})
	return sum
}

func summariseNamed(name string, size int, runs []benchRun) string {
	slices.SortFunc(runs, func(a, b benchRun) int { return cmp.Compare(a.throughput, b.throughput) })
	m := runs[len(runs)/2]
	lat := slices.Clone(m.latencies)
	slices.Sort(lat)
	pct := func(p float64) string {
		if len(lat) == 0 {
			return "-"
		}
		return fmt.Sprintf("%.2f", float64(lat[min(len(lat)-1, int(p*float64(len(lat))))])/1e6)
	}
	rate := fmt.Sprintf("%.0f", m.rate)
	if m.achieved < 0.95*m.rate {
		rate += fmt.Sprintf(" (only %.0f achieved)", m.achieved)
	}
	return fmt.Sprintf("| %s | %d B | %.0f (%.0f–%.0f) | %.1f | %s | %s | %s | %s | %s |",
		name, size, m.throughput, runs[0].throughput, runs[len(runs)-1].throughput,
		m.throughput*float64(size)/1e6, rate, pct(0.50), pct(0.99), pct(0.999), pct(1))
}

func sanitizeName(s string) string {
	r := strings.NewReplacer(" ", "_", "(", "", ")", "", "=", "-", "/", "", ":", "")
	return r.Replace(s)
}
