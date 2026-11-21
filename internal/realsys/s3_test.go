package realsys

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/amritangshudey/samsa/internal/sys"
)

func TestS3StoreHTTPAndFile(t *testing.T) {
	var mu sync.Mutex
	objs := map[string][]byte{}
	var sawAuth bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
			sawAuth = true
		}
		key := strings.TrimPrefix(r.URL.Path, "/")
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			b, _ := io.ReadAll(r.Body)
			objs[key] = b
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			b, ok := objs[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
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
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer srv.Close()

	for _, tc := range []struct {
		name string
		cfg  S3Config
	}{
		{"http", S3Config{Endpoint: srv.URL, Bucket: "bkt", Region: "us-east-1", AccessKey: "ak", SecretKey: "sk"}},
		{"file", S3Config{Endpoint: "file://" + t.TempDir(), Bucket: "bkt"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := NewS3Store(tc.cfg, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			payload := []byte("0123456789abcdef")
			if err := st.putSync("tier/1-0-10.obj", payload); err != nil {
				t.Fatalf("put: %v", err)
			}
			got, err := st.getSync("tier/1-0-10.obj", 4, 6)
			if err != nil || !bytes.Equal(got, []byte("456789")) {
				t.Fatalf("ranged get: %q, %v", got, err)
			}
			if err := st.deleteSync("tier/1-0-10.obj"); err != nil {
				t.Fatalf("delete: %v", err)
			}
			if _, err := st.getSync("tier/1-0-10.obj", 0, 4); !errors.Is(err, sys.ErrNotExist) {
				t.Fatalf("get after delete: got %v, want ErrNotExist", err)
			}
		})
	}
	if !sawAuth {
		t.Fatal("expected SigV4 Authorization header on HTTP requests")
	}
}

// The file backend holds tiered records: private, like the data directory.
func TestS3StoreFileIsPrivate(t *testing.T) {
	root := t.TempDir()
	st, err := NewS3Store(S3Config{Endpoint: "file://" + root, Bucket: "bkt"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.putSync("tier/1-0-10.obj", []byte("data")); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]os.FileMode{
		filepath.Join(root, "bkt"):                       0o700,
		filepath.Join(root, "bkt", "tier"):               0o700,
		filepath.Join(root, "bkt", "tier", "1-0-10.obj"): 0o600,
	} {
		info, err := os.Stat(name)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Errorf("%s: mode %o, want %o", name, got, want)
		}
	}
}

// A server that ignores Range, or sends less, is refused without reading
// more than one byte past what was asked for (rule 4).
func TestS3StoreGetChecksTheSize(t *testing.T) {
	size := 1 << 20
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, size))
	}))
	defer srv.Close()
	st, err := NewS3Store(S3Config{Endpoint: srv.URL, Bucket: "bkt"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.getSync("k", 0, 4); err == nil {
		t.Fatal("a body larger than the range was accepted")
	}
	if _, err := st.getSync("k", 0, int64(size)+1); err == nil {
		t.Fatal("a body shorter than the range was accepted")
	}
	if got, err := st.getSync("k", 0, int64(size)); err != nil || len(got) != size {
		t.Fatalf("exact size: %d bytes, %v", len(got), err)
	}
	if _, err := st.getSync("k", 0, 0); err == nil {
		t.Fatal("an empty range was accepted")
	}
}
