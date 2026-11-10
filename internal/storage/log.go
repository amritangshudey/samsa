package storage

import (
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/amritangshudey/samsa/internal/sys"
)

// Errors returned by a [Log].
var (
	// ErrOffsetOutOfRange is returned by Read for an offset before the
	// start or after the end of the log.
	ErrOffsetOutOfRange = errors.New("storage: offset out of range")
	// ErrTooLarge is returned by Append for a payload over MaxPayloadBytes.
	ErrTooLarge = errors.New("storage: payload too large")
	// ErrCorrupt is returned when data that should be intact fails its
	// checksum or structure checks. Bad data is never returned instead.
	ErrCorrupt = errors.New("storage: corrupt data")
)

// Config configures a [Log]. The zero value is usable.
type Config struct {
	// SegmentBytes is the size at which the active segment is sealed and a
	// new one started. Zero means 64 MiB.
	SegmentBytes int64
	// IndexIntervalBytes is roughly how many bytes of segment lie between
	// two index entries. Zero means 4096.
	IndexIntervalBytes int64
	// MaxPayloadBytes bounds a single payload, on append and when reading
	// a length from disk. Zero means 1 MiB.
	MaxPayloadBytes int
	// SyncBytes is the fsync policy. Zero syncs inside every Append. A
	// positive value syncs once that many unsynced bytes have accumulated.
	// A negative value never syncs on append; the caller calls Sync.
	SyncBytes int64

	// skipRollDirSync and skipRecoverySync remove one step each from the
	// crash-safety protocol. Tests set them to prove that the crash tests
	// notice.
	skipRollDirSync, skipRecoverySync bool
}

func (c Config) withDefaults() (Config, error) {
	if c.SegmentBytes == 0 {
		c.SegmentBytes = 64 << 20
	}
	if c.IndexIntervalBytes == 0 {
		c.IndexIntervalBytes = 4096
	}
	if c.MaxPayloadBytes == 0 {
		c.MaxPayloadBytes = 1 << 20
	}
	switch {
	case c.MaxPayloadBytes < 0 || c.IndexIntervalBytes < 0:
		return c, errors.New("storage: negative limit in config")
	case c.SegmentBytes > math.MaxUint32:
		return c, errors.New("storage: SegmentBytes must fit in 32 bits (index positions)")
	case int64(c.MaxPayloadBytes)+headerSize > c.SegmentBytes:
		return c, errors.New("storage: MaxPayloadBytes plus header must fit in one segment")
	}
	return c, nil
}

// segment is one file of the log. Sealed segments are immutable and fully
// durable; only the last, active segment is written.
type segment struct {
	base  int64 // offset of its first record; also its file name
	size  int64 // bytes of valid frames
	index []indexEntry
	file  sys.File // open only while the segment is active
}

// Log is a segmented append-only log. It is not safe for concurrent use:
// like all core code it is owned by one single-threaded node.
type Log struct {
	disk sys.Disk
	dir  string
	cfg  Config

	segs     []*segment // ascending by base; the last one is active
	end      int64      // next offset to assign
	durable  int64      // every offset below this has been synced
	unsynced int64      // bytes appended since the last sync
	err      error      // first write or sync failure; the log is dead after it
	buf      []byte     // scratch for encoding frames
}

func (l *Log) active() *segment { return l.segs[len(l.segs)-1] }

func segName(dir string, base int64, ext string) string {
	return path.Join(dir, fmt.Sprintf("%020d%s", base, ext))
}

// Open opens the log in dir on disk, creating it if it does not exist and
// recovering it if it was not closed cleanly. Recovery truncates a torn
// tail; it never discards a record that was synced.
func Open(disk sys.Disk, dir string, cfg Config) (*Log, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	l := &Log{disk: disk, dir: dir, cfg: cfg}
	if err := disk.MkdirAll(dir); err != nil {
		return nil, fmt.Errorf("create log directory %s: %w", dir, err)
	}
	names, err := disk.List(dir)
	if err != nil {
		return nil, fmt.Errorf("list log directory %s: %w", dir, err)
	}
	for _, name := range names { // sorted, and zero-padded, so ascending by base
		digits, isLog := strings.CutSuffix(name, ".log")
		if !isLog || len(digits) != 20 {
			continue
		}
		base, err := strconv.ParseInt(digits, 10, 64)
		if err != nil || base < 0 {
			continue
		}
		l.segs = append(l.segs, &segment{base: base})
	}

	if len(l.segs) == 0 {
		if err := l.addSegment(0); err != nil {
			return nil, err
		}
		// The directory itself must also survive, hence its parent.
		if err := disk.SyncDir(path.Dir(dir)); err != nil {
			return nil, fmt.Errorf("sync parent of %s: %w", dir, err)
		}
		return l, nil
	}

	for i, s := range l.segs[:len(l.segs)-1] {
		if err := l.loadSealed(s, l.segs[i+1].base); err != nil {
			return nil, err
		}
	}
	if err := l.recoverActive(); err != nil {
		return nil, err
	}
	return l, nil
}

// addSegment creates an empty segment starting at base, makes its name
// durable, and makes it the active segment.
func (l *Log) addSegment(base int64) error {
	name := segName(l.dir, base, ".log")
	f, err := l.disk.Open(name, sys.Create)
	if err != nil {
		return fmt.Errorf("create segment: %w", err)
	}
	// Without this, a crash can remove the file together with records that
	// were synced into it.
	if !l.cfg.skipRollDirSync {
		if err := l.disk.SyncDir(l.dir); err != nil {
			return fmt.Errorf("sync log directory %s: %w", l.dir, err)
		}
	}
	l.segs = append(l.segs, &segment{base: base, file: f})
	return nil
}

// scan walks the frames of s in f from the start, calling visit for each
// valid frame that continues the offset sequence. It returns the byte
// position and offset just past the last such frame. A frame that is
// invalid or out of sequence ends the scan without error; an I/O error is
// returned.
func (l *Log) scan(f sys.File, s *segment, size int64, visit func(pos int64, h frameHeader)) (pos, next int64, err error) {
	next = s.base
	for pos < size {
		h, err := readHeader(f, pos, size, l.cfg.MaxPayloadBytes)
		if err == nil && h.base != next {
			err = errBadFrame
		}
		if err == nil {
			_, err = readPayload(f, pos, h)
		}
		if errors.Is(err, errBadFrame) {
			break
		}
		if err != nil {
			return 0, 0, err
		}
		visit(pos, h)
		pos += headerSize + h.length
		next += int64(h.count)
	}
	return pos, next, nil
}

// indexer returns a visit function that builds s.index sparsely.
func (l *Log) indexer(s *segment) func(pos int64, h frameHeader) {
	s.index = s.index[:0]
	return func(pos int64, h frameHeader) { l.maybeIndex(s, pos, h.base) }
}

// maybeIndex adds an index entry for the frame at pos if the last entry is
// far enough behind.
func (l *Log) maybeIndex(s *segment, pos, base int64) {
	if n := len(s.index); n == 0 || pos-int64(s.index[n-1].pos) >= l.cfg.IndexIntervalBytes {
		s.index = append(s.index, indexEntry{rel: uint32(base - s.base), pos: uint32(pos)})
	}
}

// loadSealed prepares a sealed segment for reading: its size, and its index
// from the index file or, failing that, from a scan of the segment.
func (l *Log) loadSealed(s *segment, nextBase int64) error {
	f, err := l.disk.Open(segName(l.dir, s.base, ".log"), sys.ReadOnly)
	if err != nil {
		return fmt.Errorf("open segment: %w", err)
	}
	defer f.Close() //nolint:errcheck // read-only handle
	if s.size, err = f.Size(); err != nil {
		return fmt.Errorf("size of segment %d: %w", s.base, err)
	}
	if b, err := l.readFile(segName(l.dir, s.base, ".index")); err == nil {
		if s.index, err = decodeIndex(b, s.size); err == nil {
			return nil
		}
	}
	// Missing or unusable index: rebuild. A sealed segment was synced
	// before its successor was created, so every byte must be a valid frame.
	pos, next, err := l.scan(f, s, s.size, l.indexer(s))
	if err != nil {
		return err
	}
	if pos != s.size || next != nextBase {
		return fmt.Errorf("%w: sealed segment %d is valid up to byte %d of %d, offset %d, but the next segment starts at offset %d",
			ErrCorrupt, s.base, pos, s.size, next, nextBase)
	}
	return l.writeIndex(s)
}

// readFile returns the whole content of a small file.
func (l *Log) readFile(name string) ([]byte, error) {
	f, err := l.disk.Open(name, sys.ReadOnly)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck // read-only handle
	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	// An index has one entry per IndexIntervalBytes at most, so this bound
	// is generous; it keeps a damaged directory from costing memory.
	if size > 12+8*(l.cfg.SegmentBytes/headerSize+1) {
		return nil, errBadIndex
	}
	b := make([]byte, size)
	if _, err := f.ReadAt(b, 0); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return b, nil
}

// writeIndex persists s.index. The file needs no atomic replace: a torn
// index fails its checksum and is rebuilt.
func (l *Log) writeIndex(s *segment) error {
	name := segName(l.dir, s.base, ".index")
	f, err := l.disk.Open(name, sys.Create)
	if err != nil {
		return fmt.Errorf("create index: %w", err)
	}
	defer f.Close() //nolint:errcheck // synced below; close cannot lose data
	if err := f.Truncate(0); err != nil {
		return fmt.Errorf("truncate index %s: %w", name, err)
	}
	if _, err := f.WriteAt(encodeIndex(s.index), 0); err != nil {
		return fmt.Errorf("write index %s: %w", name, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync index %s: %w", name, err)
	}
	return nil
}

// recoverActive scans the last segment, cuts off everything after its last
// valid in-sequence frame, and makes that cut durable.
func (l *Log) recoverActive() error {
	s := l.active()
	f, err := l.disk.Open(segName(l.dir, s.base, ".log"), sys.ReadWrite)
	if err != nil {
		return fmt.Errorf("open segment: %w", err)
	}
	size, err := f.Size()
	if err != nil {
		return fmt.Errorf("size of segment %d: %w", s.base, err)
	}
	pos, next, err := l.scan(f, s, size, l.indexer(s))
	if err != nil {
		return err
	}
	if pos < size {
		if err := f.Truncate(pos); err != nil {
			return fmt.Errorf("truncate torn tail of segment %d: %w", s.base, err)
		}
	}
	// One sync does two jobs. It makes the cut durable before anything is
	// appended after it, or a later crash could bring the old bytes back
	// behind new frames. And it makes the surviving frames durable, which
	// they may not be if only the process died and the data is still in the
	// operating system's cache.
	if !l.cfg.skipRecoverySync {
		if err := f.Sync(); err != nil {
			return fmt.Errorf("sync segment %d after recovery: %w", s.base, err)
		}
	}
	s.file, s.size = f, pos
	l.end = next
	l.durable = next
	return nil
}

// fail records the first fatal error and returns it. Afterwards every
// operation returns that error: after a failed write or sync the state of
// the file is unknown, and the node must stop.
func (l *Log) fail(err error) error {
	if l.err == nil {
		l.err = err
	}
	return err
}

// Append adds one payload occupying count consecutive offsets and returns
// the first of them. Whether the payload is durable when Append returns
// depends on Config.SyncBytes; compare the result with DurableOffset.
func (l *Log) Append(payload []byte, count uint32) (int64, error) {
	switch {
	case l.err != nil:
		return 0, l.err
	case len(payload) > l.cfg.MaxPayloadBytes:
		return 0, fmt.Errorf("%w: %d bytes, limit %d", ErrTooLarge, len(payload), l.cfg.MaxPayloadBytes)
	case count == 0:
		return 0, errors.New("storage: append of zero records")
	case l.end > math.MaxInt64-int64(count):
		return 0, errors.New("storage: offset space exhausted")
	}
	frameLen := int64(headerSize + len(payload))
	// Roll when the frame does not fit, or when the frame's relative
	// offset would no longer fit the index's 32 bits.
	if s := l.active(); s.size > 0 && (s.size+frameLen > l.cfg.SegmentBytes || l.end-s.base > math.MaxUint32) {
		if err := l.roll(); err != nil {
			return 0, l.fail(err)
		}
	}
	s := l.active()
	l.buf = appendFrame(l.buf[:0], l.end, count, payload)
	if _, err := s.file.WriteAt(l.buf, s.size); err != nil {
		return 0, l.fail(fmt.Errorf("append at offset %d: %w", l.end, err))
	}
	l.maybeIndex(s, s.size, l.end)
	base := l.end
	s.size += frameLen
	l.end += int64(count)
	l.unsynced += frameLen
	if l.cfg.SyncBytes >= 0 && l.unsynced >= l.cfg.SyncBytes {
		if err := l.Sync(); err != nil {
			return 0, err
		}
	}
	return base, nil
}

// roll seals the active segment and starts a new one. The order matters:
// the old segment and its index are durable before the new file exists, so
// a sealed segment never needs recovery.
func (l *Log) roll() error {
	if err := l.Sync(); err != nil {
		return err
	}
	old := l.active()
	if err := l.writeIndex(old); err != nil {
		return err
	}
	if err := l.addSegment(l.end); err != nil {
		return err
	}
	file := old.file
	old.file = nil
	if err := file.Close(); err != nil {
		return fmt.Errorf("close segment %d: %w", old.base, err)
	}
	return nil
}

// Sync makes everything appended so far durable.
func (l *Log) Sync() error {
	if l.err != nil {
		return l.err
	}
	if err := l.active().file.Sync(); err != nil {
		return l.fail(fmt.Errorf("sync segment %d: %w", l.active().base, err))
	}
	l.durable, l.unsynced = l.end, 0
	return nil
}

// Read returns consecutive batches beginning with the one that contains
// offset, taken from a single segment, stopping before the total payload
// size would exceed maxBytes. The first batch is returned whatever its
// size, so a reader can always make progress. Reading at EndOffset returns
// no batches and no error.
func (l *Log) Read(offset int64, maxBytes int) ([]Batch, error) {
	switch {
	case l.err != nil:
		return nil, l.err
	case offset < l.StartOffset() || offset > l.end:
		return nil, fmt.Errorf("%w: %d not in [%d, %d]", ErrOffsetOutOfRange, offset, l.StartOffset(), l.end)
	case offset == l.end:
		return nil, nil
	}
	// The last segment whose base is <= offset.
	s := l.segs[sort.Search(len(l.segs), func(i int) bool { return l.segs[i].base > offset })-1]
	if s.size == 0 { // an empty active segment: offset can only be l.end, handled above
		return nil, nil
	}
	f := s.file
	if f == nil {
		var err error
		if f, err = l.disk.Open(segName(l.dir, s.base, ".log"), sys.ReadOnly); err != nil {
			return nil, fmt.Errorf("open segment: %w", err)
		}
		defer f.Close() //nolint:errcheck // read-only handle
	}

	// The last index entry at or before offset.
	var pos int64
	if i := sort.Search(len(s.index), func(i int) bool { return s.base+int64(s.index[i].rel) > offset }); i > 0 {
		pos = int64(s.index[i-1].pos)
	}

	var out []Batch
	total := 0
	for pos < s.size {
		h, err := readHeader(f, pos, s.size, l.cfg.MaxPayloadBytes)
		if err != nil {
			return nil, corrupt(err)
		}
		if offset >= h.base+int64(h.count) { // wholly before the offset wanted
			pos += headerSize + h.length
			continue
		}
		if len(out) > 0 && total+int(h.length) > maxBytes {
			break
		}
		payload, err := readPayload(f, pos, h)
		if err != nil {
			return nil, corrupt(err)
		}
		out = append(out, Batch{Base: h.base, Count: h.count, Payload: payload})
		total += len(payload)
		pos += headerSize + h.length
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: no frame holds offset %d in segment %d", ErrCorrupt, offset, s.base)
	}
	return out, nil
}

// corrupt turns a bad frame met outside recovery into ErrCorrupt.
func corrupt(err error) error {
	if errors.Is(err, errBadFrame) {
		return fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	return err
}

// StartOffset returns the offset of the first record in the log.
func (l *Log) StartOffset() int64 { return l.segs[0].base }

// EndOffset returns the offset the next appended record will get.
func (l *Log) EndOffset() int64 { return l.end }

// DurableOffset returns the offset below which every record has been
// synced. Records at or above it may be lost in a crash.
func (l *Log) DurableOffset() int64 { return l.durable }

// Close releases the log's file handle. It does not sync.
func (l *Log) Close() error {
	if f := l.active().file; f != nil {
		l.active().file = nil
		if err := f.Close(); err != nil {
			return fmt.Errorf("close segment: %w", err)
		}
	}
	if l.err == nil {
		l.err = errors.New("storage: log is closed")
	}
	return nil
}
