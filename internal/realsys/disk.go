// Package realsys holds the real implementations of the sys interfaces:
// files, TCP and the wall clock. It is the only package under internal/
// that may do I/O or use goroutines.
package realsys

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sort"

	"github.com/amritangshudey/samsa/internal/sys"
)

// Disk is a sys.Disk backed by a directory on the local filesystem.
//
// Every path is resolved inside that directory by os.Root, so no name can
// reach outside it. Sync is os.File.Sync, which on macOS issues F_FULLFSYNC
// (plain fsync there does not force the drive to flush).
type Disk struct{ root *os.Root }

// OpenDisk returns a Disk rooted at dir, creating dir if needed.
func OpenDisk(dir string) (*Disk, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("open data directory: %w", err)
	}
	return &Disk{root: root}, nil
}

// Close releases the data directory.
func (d *Disk) Close() error { return d.root.Close() }

// notExist makes a missing-file error also match sys.ErrNotExist.
func notExist(err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%w: %w", sys.ErrNotExist, err)
	}
	return err
}

// Open implements sys.Disk.
func (d *Disk) Open(name string, flag sys.OpenFlag) (sys.File, error) {
	mode := os.O_RDONLY
	switch flag {
	case sys.ReadWrite:
		mode = os.O_RDWR
	case sys.Create:
		mode = os.O_RDWR | os.O_CREATE
	}
	f, err := d.root.OpenFile(name, mode, 0o644)
	if err != nil {
		return nil, notExist(err)
	}
	return file{f}, nil
}

// MkdirAll implements sys.Disk.
func (d *Disk) MkdirAll(dir string) error { return d.root.MkdirAll(dir, 0o755) }

// Remove implements sys.Disk.
func (d *Disk) Remove(name string) error { return notExist(d.root.Remove(name)) }

// Rename implements sys.Disk.
func (d *Disk) Rename(oldName, newName string) error {
	return notExist(d.root.Rename(oldName, newName))
}

// List implements sys.Disk.
func (d *Disk) List(dir string) ([]string, error) {
	f, err := d.root.Open(dir)
	if err != nil {
		return nil, notExist(err)
	}
	defer f.Close() //nolint:errcheck // read-only handle
	names, err := f.Readdirnames(-1)
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", dir, err)
	}
	sort.Strings(names)
	return names, nil
}

// SyncDir implements sys.Disk.
func (d *Disk) SyncDir(dir string) error {
	f, err := d.root.Open(dir)
	if err != nil {
		return notExist(err)
	}
	defer f.Close() //nolint:errcheck // nothing was written through this handle
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync directory %s: %w", dir, err)
	}
	return nil
}

// file adds Size to *os.File, which already has the rest of sys.File.
type file struct{ *os.File }

// Size implements sys.File.
func (f file) Size() (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
