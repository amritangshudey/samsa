package simsys

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"sort"

	"github.com/amritangshudey/samsa/internal/sys"
)

// ErrCrashed is returned by every disk operation from the moment the
// simulated machine loses power until [Disk.Restart].
var ErrCrashed = errors.New("simsys: machine crashed")

// ErrInjected is the error returned by a sync that was made to fail.
var ErrInjected = errors.New("simsys: injected sync failure")

// DiskConfig tunes the fault model of a [Disk].
type DiskConfig struct {
	// BlockSize is the unit in which unsynced data survives or is lost.
	// Zero means 512.
	BlockSize int
	// SyncErrorRate is the probability that a Sync or SyncDir fails with
	// ErrInjected, having synced nothing.
	SyncErrorRate float64
}

// Disk is an in-memory sys.Disk that models the durability contract in the
// sys.Disk documentation and breaks everything that contract permits.
//
// File data: each file has its content as of its last successful Sync
// (durable) and its current content (live). Namespace: creates, removes,
// renames and mkdirs enter one ordered journal, and SyncDir(d) commits the
// journal up to the last entry touching d.
//
// Known kindness of the model: directory operations persist in order (as on
// a journalling filesystem), and synced data is never corrupted.
type Disk struct {
	rng *Rand
	cfg DiskConfig

	files, durableFiles map[string]*inode
	dirs, durableDirs   map[string]bool
	journal             []dirOp

	epoch   uint64 // bumped by Restart; handles from before it are dead
	ops     int    // operations performed, for choosing crash points
	crashIn int    // crash when this many more operations have started; 0 = disarmed
	crashed bool
}

type inode struct{ live, durable []byte }

type opKind uint8

const (
	opCreate opKind = iota
	opRemove
	opRename
	opMkdir
)

type dirOp struct {
	kind    opKind
	name    string // file or directory acted on
	newName string // rename target
	ino     *inode
}

// NewDisk returns an empty disk whose faults are drawn from rng.
func NewDisk(rng *Rand, cfg DiskConfig) *Disk {
	if cfg.BlockSize <= 0 {
		cfg.BlockSize = 512
	}
	return &Disk{
		rng:          rng,
		cfg:          cfg,
		files:        map[string]*inode{},
		durableFiles: map[string]*inode{},
		dirs:         map[string]bool{".": true},
		durableDirs:  map[string]bool{".": true},
	}
}

// CrashAfter arms the disk to lose power when the n-th operation from now
// starts (n >= 1). That operation may or may not take effect.
func (d *Disk) CrashAfter(n int) { d.crashIn = n }

// SetSyncErrorRate changes the probability that a sync fails.
func (d *Disk) SetSyncErrorRate(p float64) { d.cfg.SyncErrorRate = p }

// Crash makes the machine lose power now.
func (d *Disk) Crash() { d.crashed = true }

// Crashed reports whether the machine has lost power and not been restarted.
func (d *Disk) Crashed() bool { return d.crashed }

// Ops returns how many operations have been started on the disk.
func (d *Disk) Ops() int { return d.ops }

// step accounts for one operation. dead means the operation must fail with
// ErrCrashed; now means the crash happened at this very operation, so the
// caller may still let it take effect.
func (d *Disk) step() (dead, now bool) {
	d.ops++
	if d.crashed {
		return true, false
	}
	if d.crashIn > 0 {
		d.crashIn--
		if d.crashIn == 0 {
			d.crashed = true
			return true, true
		}
	}
	return false, false
}

// mutate runs apply for a state-changing operation. An operation interrupted
// by the crash takes effect half the time.
func (d *Disk) mutate(apply func()) error {
	dead, now := d.step()
	if dead && (!now || d.rng.Chance(0.5)) {
		return ErrCrashed
	}
	apply()
	if dead {
		return ErrCrashed
	}
	return nil
}

// Restart brings the machine back after a crash (crashing it first if it is
// still up): a random prefix of the uncommitted namespace journal survives,
// and every unsynced block of every file becomes its new content, its old
// content or garbage.
func (d *Disk) Restart() {
	n := d.rng.Intn(len(d.journal) + 1)
	for _, op := range d.journal[:n] {
		d.commit(op)
	}
	d.journal = nil

	names := make([]string, 0, len(d.durableFiles))
	for name := range d.durableFiles {
		names = append(names, name)
	}
	sort.Strings(names) // map order must not decide which random numbers a file gets
	d.files = make(map[string]*inode, len(names))
	for _, name := range names {
		ino := d.durableFiles[name]
		ino.durable = d.afterCrash(ino)
		ino.live = bytes.Clone(ino.durable)
		d.files[name] = ino
	}
	d.dirs = make(map[string]bool, len(d.durableDirs))
	for dir := range d.durableDirs {
		d.dirs[dir] = true
	}
	d.epoch++
	d.crashed = false
	d.crashIn = 0
}

// afterCrash decides what a file holds after power loss.
func (d *Disk) afterCrash(ino *inode) []byte {
	older, newer := ino.durable, ino.live
	if bytes.Equal(older, newer) {
		return older
	}
	switch d.rng.Intn(8) {
	case 0: // nothing unsynced reached the platter
		return older
	case 1: // everything did
		return bytes.Clone(newer)
	}
	lo, hi := min(len(older), len(newer)), max(len(older), len(newer))
	out := make([]byte, lo+d.rng.Intn(hi-lo+1))
	bs := d.cfg.BlockSize
	for b := 0; b < len(out); b += bs {
		fate := d.rng.Intn(5) // of this block's unsynced bytes: 0-1 new, 2-3 old, 4 garbage
		for i := b; i < min(b+bs, len(out)); i++ {
			// Only bytes written since the last sync are at risk. A synced
			// byte sharing a block with them keeps its value: rule 1 of the
			// contract has no exception for neighbours.
			dirty := i < len(newer) && (i >= len(older) || older[i] != newer[i])
			switch {
			case !dirty:
				out[i] = older[i]
			case fate <= 1:
				out[i] = newer[i]
			case fate <= 3 && i < len(older):
				out[i] = older[i]
			default:
				out[i] = byte(d.rng.Uint64())
			}
		}
	}
	return out
}

func (d *Disk) commit(op dirOp) {
	switch op.kind {
	case opCreate:
		d.durableFiles[op.name] = op.ino
	case opRemove:
		delete(d.durableFiles, op.name)
	case opRename:
		delete(d.durableFiles, op.name)
		d.durableFiles[op.newName] = op.ino
	case opMkdir:
		d.durableDirs[op.name] = true
	}
}

// clean normalises a path to the form used as a map key. Rooting it first
// means ".." can never climb out of the disk.
func clean(name string) string {
	if p := path.Clean("/" + name)[1:]; p != "" {
		return p
	}
	return "."
}

// Open implements sys.Disk.
func (d *Disk) Open(name string, flag sys.OpenFlag) (sys.File, error) {
	name = clean(name)
	var f *file
	var openErr error
	err := d.mutate(func() {
		ino, ok := d.files[name]
		if !ok {
			if flag != sys.Create || !d.dirs[path.Dir(name)] {
				openErr = fmt.Errorf("open %s: %w", name, sys.ErrNotExist)
				return
			}
			ino = &inode{}
			d.files[name] = ino
			d.journal = append(d.journal, dirOp{kind: opCreate, name: name, ino: ino})
		}
		f = &file{d: d, ino: ino, epoch: d.epoch, name: name, writable: flag != sys.ReadOnly}
	})
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", name, err)
	}
	if openErr != nil {
		return nil, openErr
	}
	return f, nil
}

// MkdirAll implements sys.Disk.
func (d *Disk) MkdirAll(dir string) error {
	dir = clean(dir)
	return d.mutate(func() {
		var missing []string
		for p := dir; !d.dirs[p]; p = path.Dir(p) {
			missing = append(missing, p)
		}
		slices.Reverse(missing)
		for _, p := range missing {
			d.dirs[p] = true
			d.journal = append(d.journal, dirOp{kind: opMkdir, name: p})
		}
	})
}

// Remove implements sys.Disk.
func (d *Disk) Remove(name string) error {
	name = clean(name)
	var opErr error
	err := d.mutate(func() {
		if _, ok := d.files[name]; !ok {
			opErr = fmt.Errorf("remove %s: %w", name, sys.ErrNotExist)
			return
		}
		delete(d.files, name)
		d.journal = append(d.journal, dirOp{kind: opRemove, name: name})
	})
	return errors.Join(err, opErr)
}

// Rename implements sys.Disk.
func (d *Disk) Rename(oldName, newName string) error {
	oldName, newName = clean(oldName), clean(newName)
	var opErr error
	err := d.mutate(func() {
		ino, ok := d.files[oldName]
		if !ok || !d.dirs[path.Dir(newName)] {
			opErr = fmt.Errorf("rename %s: %w", oldName, sys.ErrNotExist)
			return
		}
		delete(d.files, oldName)
		d.files[newName] = ino
		d.journal = append(d.journal, dirOp{kind: opRename, name: oldName, newName: newName, ino: ino})
	})
	return errors.Join(err, opErr)
}

// List implements sys.Disk.
func (d *Disk) List(dir string) ([]string, error) {
	dir = clean(dir)
	if dead, _ := d.step(); dead {
		return nil, ErrCrashed
	}
	if !d.dirs[dir] {
		return nil, fmt.Errorf("list %s: %w", dir, sys.ErrNotExist)
	}
	var names []string
	for name := range d.files {
		if path.Dir(name) == dir {
			names = append(names, path.Base(name))
		}
	}
	for sub := range d.dirs {
		if sub != "." && path.Dir(sub) == dir {
			names = append(names, path.Base(sub))
		}
	}
	sort.Strings(names)
	return names, nil
}

// SyncDir implements sys.Disk.
func (d *Disk) SyncDir(dir string) error {
	dir = clean(dir)
	if !d.crashed && d.rng.Chance(d.cfg.SyncErrorRate) {
		d.ops++
		return fmt.Errorf("syncdir %s: %w", dir, ErrInjected)
	}
	return d.mutate(func() {
		last := -1
		for i, op := range d.journal {
			if path.Dir(op.name) == dir || (op.kind == opRename && path.Dir(op.newName) == dir) {
				last = i
			}
		}
		for _, op := range d.journal[:last+1] {
			d.commit(op)
		}
		d.journal = d.journal[last+1:]
	})
}

type file struct {
	d        *Disk
	ino      *inode
	epoch    uint64
	name     string
	writable bool
	closed   bool
}

func (f *file) check() error {
	switch {
	case f.epoch != f.d.epoch:
		return ErrCrashed // a handle from before the last restart
	case f.closed:
		return fmt.Errorf("%s: file is closed", f.name)
	}
	return nil
}

// ReadAt implements sys.File.
func (f *file) ReadAt(p []byte, off int64) (int, error) {
	if err := f.check(); err != nil {
		return 0, err
	}
	if dead, _ := f.d.step(); dead {
		return 0, ErrCrashed
	}
	if off < 0 {
		return 0, fmt.Errorf("%s: negative offset", f.name)
	}
	if off >= int64(len(f.ino.live)) {
		return 0, io.EOF
	}
	n := copy(p, f.ino.live[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// WriteAt implements sys.File.
func (f *file) WriteAt(p []byte, off int64) (int, error) {
	if err := f.check(); err != nil {
		return 0, err
	}
	if !f.writable || off < 0 {
		return 0, fmt.Errorf("%s: write to read-only file or negative offset", f.name)
	}
	err := f.d.mutate(func() {
		if need := int(off) + len(p); need > len(f.ino.live) {
			f.ino.live = append(f.ino.live, make([]byte, need-len(f.ino.live))...)
		}
		copy(f.ino.live[off:], p)
	})
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

// Size implements sys.File.
func (f *file) Size() (int64, error) {
	if err := f.check(); err != nil {
		return 0, err
	}
	if dead, _ := f.d.step(); dead {
		return 0, ErrCrashed
	}
	return int64(len(f.ino.live)), nil
}

// Truncate implements sys.File.
func (f *file) Truncate(size int64) error {
	if err := f.check(); err != nil {
		return err
	}
	if !f.writable || size < 0 {
		return fmt.Errorf("%s: truncate of read-only file or negative size", f.name)
	}
	return f.d.mutate(func() {
		if int(size) <= len(f.ino.live) {
			f.ino.live = f.ino.live[:size]
		} else {
			f.ino.live = append(f.ino.live, make([]byte, int(size)-len(f.ino.live))...)
		}
	})
}

// Sync implements sys.File.
func (f *file) Sync() error {
	if err := f.check(); err != nil {
		return err
	}
	if !f.d.crashed && f.d.rng.Chance(f.d.cfg.SyncErrorRate) {
		f.d.ops++
		return fmt.Errorf("sync %s: %w", f.name, ErrInjected)
	}
	return f.d.mutate(func() { f.ino.durable = bytes.Clone(f.ino.live) })
}

// Close implements sys.File.
func (f *file) Close() error {
	if f.epoch != f.d.epoch {
		return ErrCrashed
	}
	f.closed = true
	return nil
}
