package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/amritangshudey/samsa/internal/broker"
	"github.com/amritangshudey/samsa/internal/realsys"
	"github.com/amritangshudey/samsa/internal/sys"
)

// runCheck is `samsa check -data dir`: it reads a stopped node's data
// directory as a start would, writes nothing, and exits 1 if anything is
// corrupt or a start would have to repair it.
func runCheck(args []string, out io.Writer) (ok bool, err error) {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	dataDir := fs.String("data", "data", "data directory of a stopped node")
	nodeID := fs.Int("node-id", 1, "the node's id")
	if err := fs.Parse(args); err != nil {
		return false, err
	}
	if _, err := os.Stat(*dataDir); err != nil {
		return false, err // OpenDisk would create it
	}
	disk, err := realsys.OpenDisk(*dataDir) // fails while a node holds it
	if err != nil {
		return false, err
	}
	defer disk.Close() //nolint:errcheck // read only
	say := func(format string, a ...any) {
		fmt.Fprintf(out, format, a...) //nolint:errcheck // nowhere to report a failed print
	}
	ro := &readOnlyDisk{Disk: disk}
	res, err := broker.Check(ro, broker.Config{NodeID: int32(*nodeID)})
	for _, r := range ro.repairs {
		say("repair needed: %s\n", r)
	}
	if err != nil {
		say("corrupt: %v\n", err)
		return false, nil
	}
	say("raft log: entries %d to %d, term %d, snapshot at %d\nstate: %d topics, %d groups\n",
		res.LogStart, res.LastIndex, res.Term, res.Snapshot, res.Topics, res.Groups)
	if len(ro.repairs) > 0 {
		return false, nil
	}
	say("ok\n")
	return true, nil
}

// readOnlyDisk passes reads through and records every write instead of
// making it, so that recovery runs to the end and says what it would
// repair. A written file reads back as it was on disk.
type readOnlyDisk struct {
	sys.Disk
	repairs []string
}

func (d *readOnlyDisk) note(format string, args ...any) {
	d.repairs = append(d.repairs, fmt.Sprintf(format, args...))
}

func (d *readOnlyDisk) Open(name string, flag sys.OpenFlag) (sys.File, error) {
	f, err := d.Disk.Open(name, sys.ReadOnly)
	if errors.Is(err, sys.ErrNotExist) && flag == sys.Create {
		d.note("create %s", name)
		return &roFile{d: d, name: name}, nil
	}
	if err != nil {
		return nil, err
	}
	return &roFile{File: f, d: d, name: name}, nil
}

func (d *readOnlyDisk) MkdirAll(dir string) error {
	if _, err := d.List(dir); err != nil {
		d.note("create directory %s", dir)
	}
	return nil
}

func (d *readOnlyDisk) Remove(name string) error { d.note("remove %s", name); return nil }

func (d *readOnlyDisk) Rename(from, to string) error { d.note("replace %s", to); return nil }

func (d *readOnlyDisk) SyncDir(string) error { return nil }

// roFile is a file of a readOnlyDisk; File is nil for one it would create.
type roFile struct {
	sys.File
	d       *readOnlyDisk
	name    string
	written bool
}

func (f *roFile) ReadAt(p []byte, off int64) (int, error) {
	if f.File == nil {
		return 0, io.EOF
	}
	return f.File.ReadAt(p, off)
}

func (f *roFile) Size() (int64, error) {
	if f.File == nil {
		return 0, nil
	}
	return f.File.Size()
}

func (f *roFile) WriteAt(p []byte, _ int64) (int, error) {
	if !f.written && f.File != nil {
		f.d.note("write %s", f.name)
	}
	f.written = true
	return len(p), nil
}

func (f *roFile) Truncate(size int64) error {
	old, _ := f.Size()
	f.d.note("cut %s from %d to %d bytes (%d bytes after the last valid record)", f.name, old, size, old-size)
	return nil
}

func (f *roFile) Sync() error { return nil }

func (f *roFile) Close() error {
	if f.File == nil {
		return nil
	}
	return f.File.Close()
}
