package realsys

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
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
