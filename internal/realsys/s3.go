package realsys

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
)

// S3Config configures a zero-dependency S3-compatible ObjectStore (or a local
// directory backend when Endpoint starts with "file://").
type S3Config struct {
	Endpoint  string // e.g. "http://127.0.0.1:9000", "https://s3.amazonaws.com", or "file:///tmp/samsa-obj"
	Bucket    string
	Region    string // default "us-east-1"
	AccessKey string
	SecretKey string
	Workers   int           // concurrent HTTP workers; default 8
	Timeout   time.Duration // per-request HTTP timeout; default 30s
}

type objReq struct {
	kind   uint8 // 1=put, 2=get, 3=delete
	id     sys.ObjOpID
	key    string
	data   []byte
	offset int64
	length int64
}

// S3Store implements sys.ObjectStore using Go's standard library (AWS SigV4
// HTTP requests, or local files when Endpoint has a "file://" scheme).
// Completions are delivered asynchronously to the supplied Loop.
type S3Store struct {
	cfg      S3Config
	fileDir  string
	client   *http.Client
	endpoint string // trimmed trailing slash
	loop     *Loop
	ch       chan objReq
	done     chan struct{}
	wg       sync.WaitGroup
}

// NewS3Store starts an asynchronous ObjectStore posting completions to loop.
// Call Close when shutting down.
func NewS3Store(cfg S3Config, loop *Loop) (*S3Store, error) {
	if cfg.Endpoint == "" {
		return nil, errors.New("s3: endpoint is required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 8
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	s := &S3Store{
		cfg:      cfg,
		endpoint: strings.TrimRight(cfg.Endpoint, "/"),
		client:   &http.Client{Timeout: cfg.Timeout},
		loop:     loop,
		ch:       make(chan objReq, 256),
		done:     make(chan struct{}),
	}
	if strings.HasPrefix(cfg.Endpoint, "file://") {
		dir := strings.TrimPrefix(cfg.Endpoint, "file://")
		if cfg.Bucket != "" {
			dir = filepath.Join(dir, cfg.Bucket)
		}
		if err := os.MkdirAll(dir, 0o700); err != nil { // tiered records: private, like the data directory
			return nil, fmt.Errorf("s3 file backend: %w", err)
		}
		s.fileDir = dir
	} else if cfg.Bucket == "" {
		return nil, errors.New("s3: bucket is required")
	}
	for range cfg.Workers {
		s.wg.Add(1)
		go s.worker()
	}
	return s, nil
}

// Close stops workers and waits for in-flight requests to drain.
func (s *S3Store) Close() {
	close(s.done)
	s.wg.Wait()
}

// Put asynchronously uploads data under key and reports completion via onDone.
func (s *S3Store) Put(id sys.ObjOpID, key string, data []byte) {
	cp := append([]byte(nil), data...)
	s.enqueue(objReq{kind: 1, id: id, key: key, data: cp})
}

// Get asynchronously fetches the byte range [offset, offset+length) of key;
// length must be positive.
func (s *S3Store) Get(id sys.ObjOpID, key string, offset, length int64) {
	s.enqueue(objReq{kind: 2, id: id, key: key, offset: offset, length: length})
}

// Delete asynchronously removes key from the object store.
func (s *S3Store) Delete(id sys.ObjOpID, key string) {
	s.enqueue(objReq{kind: 3, id: id, key: key})
}

func (s *S3Store) enqueue(r objReq) {
	select {
	case s.ch <- r:
	case <-s.done:
	}
}

func (s *S3Store) worker() {
	defer s.wg.Done()
	for {
		select {
		case <-s.done:
			return
		case r := <-s.ch:
			var data []byte
			var err error
			switch r.kind {
			case 1:
				err = s.putSync(r.key, r.data)
			case 2:
				data, err = s.getSync(r.key, r.offset, r.length)
			case 3:
				err = s.deleteSync(r.key)
			}
			if s.loop != nil {
				s.loop.PostObjectDone(r.id, data, err)
			}
		}
	}
}

func (s *S3Store) objectURL(key string) string {
	// Path-style addressing works with MinIO, Ceph, LocalStack, AWS S3, and httptest.
	parts := strings.Split(strings.TrimLeft(key, "/"), "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return s.endpoint + "/" + url.PathEscape(s.cfg.Bucket) + "/" + strings.Join(parts, "/")
}

func (s *S3Store) putSync(key string, data []byte) error {
	if s.fileDir != "" {
		// The caller commits a reference to the object and may drop its
		// local copy once Put succeeds, so the object must be durable first:
		// the file synced before the rename, the directory after it.
		path := filepath.Join(s.fileDir, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return err
		}
		tmp := path + ".tmp"
		f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			return err
		}
		if _, err := f.Write(data); err != nil {
			f.Close() //nolint:errcheck // the write error is the one to report
			return err
		}
		if err := f.Sync(); err != nil {
			f.Close() //nolint:errcheck // the sync error is the one to report
			return fmt.Errorf("sync object %s: %w", key, err)
		}
		if err := f.Close(); err != nil {
			return err
		}
		if err := os.Rename(tmp, path); err != nil {
			return err
		}
		dir, err := os.Open(filepath.Dir(path))
		if err != nil {
			return err
		}
		defer dir.Close() //nolint:errcheck // nothing was written through this handle
		if err := dir.Sync(); err != nil {
			return fmt.Errorf("sync object directory of %s: %w", key, err)
		}
		return nil
	}
	req, err := http.NewRequest(http.MethodPut, s.objectURL(key), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(data))
	s.sign(req, sha256Hex(data), time.Now().UTC())
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("s3 put %s: HTTP %d", key, resp.StatusCode)
	}
	return nil
}

func (s *S3Store) getSync(key string, offset, length int64) ([]byte, error) {
	if length <= 0 || offset < 0 {
		return nil, fmt.Errorf("s3 get %s: bad range at %d of length %d", key, offset, length)
	}
	if s.fileDir != "" {
		path := filepath.Join(s.fileDir, filepath.FromSlash(key))
		f, err := os.Open(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("%w: %s", sys.ErrNotExist, key)
			}
			return nil, err
		}
		defer f.Close() //nolint:errcheck
		buf := make([]byte, length)
		if _, err := f.ReadAt(buf, offset); err != nil {
			return nil, err
		}
		return buf, nil
	}
	req, err := http.NewRequest(http.MethodGet, s.objectURL(key), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	s.sign(req, emptySHA256, time.Now().UTC())
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("%w: %s", sys.ErrNotExist, key)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("s3 get %s: HTTP %d", key, resp.StatusCode)
	}
	// Never trust the server's size (rule 4): one byte more than asked
	// shows that it sent too much.
	b, err := io.ReadAll(io.LimitReader(resp.Body, length+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != length {
		return nil, fmt.Errorf("s3 get %s: got %d bytes, want %d", key, len(b), length)
	}
	return b, nil
}

func (s *S3Store) deleteSync(key string) error {
	if s.fileDir != "" {
		path := filepath.Join(s.fileDir, filepath.FromSlash(key))
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	req, err := http.NewRequest(http.MethodDelete, s.objectURL(key), nil)
	if err != nil {
		return err
	}
	s.sign(req, emptySHA256, time.Now().UTC())
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("s3 delete %s: HTTP %d", key, resp.StatusCode)
	}
	return nil
}

const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

// sign adds AWS Signature Version 4 headers when AccessKey and SecretKey are set.
func (s *S3Store) sign(req *http.Request, payloadHash string, now time.Time) {
	if s.cfg.AccessKey == "" || s.cfg.SecretKey == "" {
		return
	}
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.Header.Set("X-Amz-Date", amzDate)

	canonicalURI := req.URL.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n",
		req.URL.Host, payloadHash, amzDate)
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalReq := strings.Join([]string{
		req.Method,
		canonicalURI,
		req.URL.RawQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	scope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, s.cfg.Region)
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + sha256Hex([]byte(canonicalReq))

	kDate := hmacSHA256([]byte("AWS4"+s.cfg.SecretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(s.cfg.Region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	sig := hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign)))

	req.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		s.cfg.AccessKey, scope, signedHeaders, sig))
}
