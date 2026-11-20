package integration

import (
	"context"
	"fmt"
	"io"
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

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestTieredObjectStorage exercises real tiered object storage end-to-end against
// an in-process S3-compatible HTTP server (SigV4 signed PUT, ranged GET, DELETE):
// seals segments, verifies objects are uploaded and local disk is trimmed, restarts
// the broker (dropping all in-memory cache), and reads every record back via franz-go.
func TestTieredObjectStorage(t *testing.T) {
	var mu sync.Mutex
	objs := map[string][]byte{}
	var puts, gets, dels atomic.Int64

	s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
			w.WriteHeader(http.StatusForbidden)
			return
		}
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
				if start < 0 || end >= len(b) || start > end {
					w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
					return
				}
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(b[start : end+1])
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(b)
		case http.MethodDelete:
			delete(objs, key)
			dels.Add(1)
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer s3.Close()

	b := startBroker(t,
		"-segment-bytes", "65536",
		"-object-store-endpoint", s3.URL,
		"-object-store-bucket", "samsa-test",
		"-object-store-region", "us-east-1",
		"-object-store-access-key", "testkey",
		"-object-store-secret-key", "testsecret",
	)

	const topic = "tiered"
	const total = 120
	admin := franz(t, b)
	createTopic(t, admin, topic, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Produce ~240 KiB across 120 records (sealing ~3-4 segments of 64 KiB each).
	payload := strings.Repeat("a", 2000)
	prod := franz(t, b, kgo.ProducerBatchCompression(kgo.NoCompression()))
	for i := range total {
		rec := &kgo.Record{
			Topic: topic,
			Key:   fmt.Appendf(nil, "k%04d", i),
			Value: fmt.Appendf(nil, "%04d:%s", i, payload),
		}
		if err := prod.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}
	prod.Close()

	// Wait for tiering to upload closed segments and trim local disk.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && puts.Load() == 0 {
		time.Sleep(100 * time.Millisecond)
	}
	if puts.Load() == 0 {
		t.Fatal("expected at least one tiered object PUT to S3 server")
	}

	// Measure local disk vs object-store bytes.
	var localBytes int64
	_ = filepath.Walk(b.dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			localBytes += info.Size()
		}
		return nil
	})
	mu.Lock()
	var objBytes int64
	for _, v := range objs {
		objBytes += int64(len(v))
	}
	objCount := len(objs)
	mu.Unlock()
	t.Logf("after tiering: puts=%d objects=%d objBytes=%d localBytes=%d", puts.Load(), objCount, objBytes, localBytes)

	// Restart broker to clear in-memory object cache, forcing ranged GETs from S3.
	t0 := time.Now()
	b.kill()
	b.start()
	t.Logf("restart ready in %v", time.Since(t0))

	cons := franz(t, b, kgo.ConsumeTopics(topic))
	seen := 0
	readDeadline := time.Now().Add(15 * time.Second)
	for seen < total && time.Now().Before(readDeadline) {
		pctx, pc := context.WithTimeout(ctx, time.Second)
		fs := cons.PollFetches(pctx)
		pc()
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("fetch errors: %+v", errs)
		}
		fs.EachRecord(func(r *kgo.Record) {
			wantKey := fmt.Sprintf("k%04d", seen)
			if string(r.Key) != wantKey {
				t.Fatalf("record %d: key=%q want %q", seen, r.Key, wantKey)
			}
			seen++
		})
	}
	cons.Close()
	if seen != total {
		t.Fatalf("read %d records, want %d", seen, total)
	}
	if gets.Load() == 0 {
		t.Fatal("expected ranged GETs from S3 after restart")
	}
	t.Logf("verified all %d records with %d S3 ranged GETs", seen, gets.Load())
}
