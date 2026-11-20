package integration

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestDisklessTopicsCluster runs a 3-node cluster across 3 racks with an
// S3-compatible HTTP server and verifies:
//  1. A topic created with diskless.enabled=true writes payloads directly to S3
//     while Raft stores only compact references (~97 B per batch).
//  2. Any broker (including a non-leader follower) accepts a ProduceRequest for
//     a diskless topic and returns a valid BaseOffset.
//  3. After restarting all nodes (cold cache), franz-go reads every record back.
func TestDisklessTopicsCluster(t *testing.T) {
	var mu sync.Mutex
	objs := map[string][]byte{}
	var puts, gets atomic.Int64

	s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			b, _ := io.ReadAll(r.Body)
			objs[key] = b
			puts.Add(1)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			b, ok := objs[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			gets.Add(1)
			if rng := r.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
				parts := strings.Split(strings.TrimPrefix(rng, "bytes="), "-")
				start, _ := strconv.Atoi(parts[0])
				end, _ := strconv.Atoi(parts[1])
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(b[start : end+1])
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(b)
		case http.MethodDelete:
			delete(objs, key)
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer s3.Close()

	cl := startClusterWith(t, 3, []string{"r1", "r2", "r3"}, nil,
		"-object-store-endpoint", s3.URL,
		"-object-store-bucket", "dl-bkt",
	)
	addrs := []string{cl.nodes[1].addr, cl.nodes[2].addr, cl.nodes[3].addr}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	// Wait for cluster to elect a leader and reach format level 9.
	admin := franz(t, cl.nodes[1], kgo.SeedBrokers(addrs...))
	defer admin.Close()

	const topic = "dl-topic"
	deadline := time.Now().Add(15 * time.Second)
	for {
		creq := kmsg.NewCreateTopicsRequest()
		ct := kmsg.NewCreateTopicsRequestTopic()
		ct.Topic = topic
		ct.NumPartitions = 1
		ct.ReplicationFactor = 3
		ct.Configs = []kmsg.CreateTopicsRequestTopicConfig{{Name: "diskless.enabled", Value: kmsg.StringPtr("true")}}
		creq.Topics = append(creq.Topics, ct)
		cresp, err := creq.RequestWith(ctx, admin)
		if err == nil && len(cresp.Topics) == 1 && cresp.Topics[0].ErrorCode == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("create diskless topic failed: err=%v resp=%+v", err, cresp)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Discover leader vs follower via Metadata (poll until followers have applied topic creation).
	var leaderID int32
	mDeadline := time.Now().Add(5 * time.Second)
	for {
		mreq := kmsg.NewMetadataRequest()
		mreq.Topics = []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr(topic)}}
		mresp, err := mreq.RequestWith(ctx, admin)
		if err == nil && len(mresp.Topics) > 0 && len(mresp.Topics[0].Partitions) > 0 {
			leaderID = mresp.Topics[0].Partitions[0].Leader
			break
		}
		if time.Now().After(mDeadline) {
			t.Fatalf("metadata: %v %+v", err, mresp)
		}
		time.Sleep(50 * time.Millisecond)
	}
	followerIdx := 0
	for i := range 3 {
		if int32(i+1) != leaderID {
			followerIdx = i
			break
		}
	}
	// Give the chosen follower a moment to apply CreateTopics.
	time.Sleep(150 * time.Millisecond)

	// 1. Send a ProduceRequest directly to a NON-LEADER FOLLOWER broker (leaderless write!).
	conn, err := net.DialTimeout("tcp", addrs[followerIdx], 5*time.Second)
	if err != nil {
		t.Fatalf("dial follower %d: %v", followerIdx+1, err)
	}
	defer conn.Close()
	batch0 := wire.EncodeBatch([]wire.Record{{Key: []byte("k0000"), Value: []byte(strings.Repeat("f", 2048))}})
	preq := &wire.ProduceRequest{
		Acks: -1, TimeoutMs: 5000,
		Topics: []wire.ProduceTopic{{Name: topic, Partitions: []wire.ProducePartition{{Index: 0, Records: batch0}}}},
	}
	var w wire.Writer
	w.Int16(wire.KeyProduce)
	w.Int16(9)
	w.Int32(1)
	w.String(false, "dl-follower-test")
	w.Tags(true)
	preq.Encode(&w, 9)
	frame := make([]byte, 4+len(w.B))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(w.B)))
	copy(frame[4:], w.B)
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("write produce to follower: %v", err)
	}
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		t.Fatalf("read produce resp len: %v", err)
	}
	body := make([]byte, binary.BigEndian.Uint32(hdr))
	if _, err := io.ReadFull(conn, body); err != nil {
		t.Fatalf("read produce resp body: %v", err)
	}
	r := wire.NewReader(body)
	_ = r.Int32() // correlation ID
	r.Tags(true)  // v9 flexible response header
	var presp wire.ProduceResponse
	presp.Decode(r, 9)
	if presp.Topics[0].Partitions[0].ErrorCode != 0 || presp.Topics[0].Partitions[0].BaseOffset != 0 {
		t.Fatalf("follower diskless produce failed: %+v", presp)
	}
	t.Logf("non-leader follower %d accepted diskless produce -> BaseOffset=0", followerIdx+1)

	// 2. Produce 59 more 2 KiB records via franz-go and measure latency & Raft vs S3 bytes.
	const total = 60
	payload := strings.Repeat("x", 2048)
	prod := franz(t, cl.nodes[1], kgo.SeedBrokers(addrs...), kgo.ProducerBatchCompression(kgo.NoCompression()))
	t0 := time.Now()
	for i := 1; i < total; i++ {
		rec := &kgo.Record{
			Topic: topic,
			Key:   fmt.Appendf(nil, "k%04d", i),
			Value: fmt.Appendf(nil, "%04d:%s", i, payload),
		}
		if err := prod.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}
	elapsed := time.Since(t0)
	prod.Close()

	mu.Lock()
	var s3Bytes int64
	for _, b := range objs {
		s3Bytes += int64(len(b))
	}
	mu.Unlock()
	var raftBytes int64
	_ = filepath.Walk(cl.nodes[1].dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			raftBytes += info.Size()
		}
		return nil
	})
	t.Logf("diskless 60x2KiB: elapsed=%v avg=%v puts=%d s3Bytes=%d node1LocalBytes=%d",
		elapsed, elapsed/59, puts.Load(), s3Bytes, raftBytes)
	if raftBytes >= s3Bytes {
		t.Fatalf("expected node1 local Raft bytes (%d) < S3 bytes (%d)", raftBytes, s3Bytes)
	}

	// 3. Restart all 3 nodes (cold cache) and verify all 60 records read back via franz-go.
	for _, nd := range cl.nodes {
		nd.kill()
	}
	for _, nd := range cl.nodes {
		nd.start()
	}
	cons := franz(t, cl.nodes[1], kgo.SeedBrokers(addrs...), kgo.ConsumeTopics(topic))
	defer cons.Close()
	seen := 0
	rdeadline := time.Now().Add(20 * time.Second)
	for seen < total && time.Now().Before(rdeadline) {
		pctx, pc := context.WithTimeout(ctx, time.Second)
		fs := cons.PollFetches(pctx)
		pc()
		if errs := fs.Errors(); len(errs) > 0 {
			continue
		}
		fs.EachRecord(func(r *kgo.Record) {
			want := fmt.Sprintf("k%04d", seen)
			if string(r.Key) != want {
				t.Fatalf("record %d: key=%q want %q", seen, r.Key, want)
			}
			seen++
		})
	}
	if seen != total {
		t.Fatalf("read %d records after restart, want %d", seen, total)
	}
	if gets.Load() == 0 {
		t.Fatal("expected S3 ranged GETs after restart")
	}
	t.Logf("verified all %d diskless records after full cluster restart with %d S3 GETs", seen, gets.Load())
}
