// Package sys defines the boundary between the deterministic core and the
// outside world.
//
// A node is single-threaded. A driver calls its [Handler] methods one at a
// time; while handling an event the node calls [Clock], [Network], [Disk] and
// [Random]. Nothing else in the core may observe time, do I/O or generate
// randomness. There are two sets of implementations: real ones (TCP, files,
// the wall clock) and simulated ones driven by a seed.
//
// This package contains only the contract. See docs/adr/0001 and 0002.
package sys

import (
	"errors"
	"time"
)

// ErrNotExist is returned, possibly wrapped, by [Disk] operations on a name
// that does not exist. Test for it with errors.Is.
var ErrNotExist = errors.New("sys: file does not exist")

// TimerID names a timer. IDs are chosen by the node.
type TimerID uint64

// ConnID names one client connection for its lifetime. IDs are chosen by the
// driver and never reused within a process.
type ConnID uint64

// NodeID names one node of a cluster. IDs are configured, not negative,
// and never change.
type NodeID int32

// Instant is a point in time, in nanoseconds since the Unix epoch.
//
// It is deliberately not time.Time, whose methods can read the real clock.
type Instant int64

// Handler is implemented by a node and called by its driver.
//
// Calls are never concurrent, and each runs to completion before the next
// begins. A non-nil error means the node can no longer be trusted, for
// example after a failed sync: the driver must not call the node again. The
// real driver logs the error and exits; the simulator treats it as a crash.
type Handler interface {
	// OnStart is called once, before any other method. Recovery happens here.
	OnStart() error

	// OnTimer reports that the timer armed with [Clock.SetTimer] has expired.
	OnTimer(id TimerID) error

	// OnConnOpen reports a new inbound connection.
	OnConnOpen(c ConnID) error

	// OnConnData delivers the next bytes received on c. p is non-empty and
	// may be any fragment of the stream: message boundaries are not
	// preserved. p is valid only during the call and must be copied if kept.
	OnConnData(c ConnID, p []byte) error

	// OnConnClose reports that c is gone, whether the peer closed it, it
	// failed, or the node called [Network.Close]. It is the last call for c.
	OnConnClose(c ConnID) error

	// OnPeerMessage delivers one whole message that the node named from
	// passed to [Network.SendPeer]. The driver vouches for nothing about
	// it but its boundaries: the content is untrusted input. msg is valid
	// only during the call.
	OnPeerMessage(from NodeID, msg []byte) error
}

// Stopper is what a Handler may also offer: a way to hand its work over
// before the process exits. The driver calls OnStop once, keeps delivering
// events until Stopped reports true (or its own limit passes), and then
// exits.
type Stopper interface {
	OnStop() error
	Stopped() bool
}

// Clock is the node's only source of time.
type Clock interface {
	// Now returns the current time. Within one process lifetime it never
	// goes backwards. Across a restart it may.
	Now() Instant

	// SetTimer arranges for [Handler.OnTimer] to be called with id once,
	// no sooner than after from now. Arming an id that is already armed
	// replaces the earlier arming. A timer never fires during the handler
	// call that armed it, so a zero delay means "after the events that are
	// already waiting".
	SetTimer(id TimerID, after time.Duration)

	// CancelTimer disarms id. After it returns, OnTimer is not called for
	// that arming. Cancelling an unarmed id does nothing.
	CancelTimer(id TimerID)
}

// Network sends on connections that were announced by [Handler.OnConnOpen],
// and to the other nodes of the cluster.
type Network interface {
	// Send queues p for delivery on c, in order, and returns immediately.
	// p may be reused by the caller afterwards. Delivery is not guaranteed:
	// if the connection fails, or its bounded outbound buffer overflows
	// because the peer is not reading, the connection is closed and the
	// node receives OnConnClose. Sending on a closed connection does nothing.
	Send(c ConnID, p []byte)

	// Close closes c after sending what is already queued. The node then
	// receives OnConnClose.
	Close(c ConnID)

	// SendPeer sends msg to another node of the cluster, whole or not at
	// all, and returns immediately. Nothing else is promised: it may be
	// lost, delayed, repeated or overtaken by a later message, and the
	// sender is not told. msg may be reused by the caller afterwards.
	SendPeer(to NodeID, msg []byte)
}

// Random is the node's only source of randomness.
type Random interface {
	// Uint64 returns a uniformly distributed value.
	Uint64() uint64
}

// OpenFlag selects how [Disk.Open] opens a file.
type OpenFlag uint8

// The modes accepted by [Disk.Open].
const (
	// ReadOnly opens an existing file for reading.
	ReadOnly OpenFlag = iota
	// ReadWrite opens an existing file for reading and writing.
	ReadWrite
	// Create opens a file for reading and writing, creating it empty if it
	// does not exist.
	Create
)

// Disk is the node's only access to persistent storage.
//
// Durability contract. The storage engine may assume exactly this and no
// more, and the simulated disk exercises everything it permits.
//
//  1. Bytes written to a file are durable only once a later [File.Sync] on
//     that file has returned nil.
//  2. After a crash, each unsynced region of a file holds, block by block,
//     the new bytes, the old bytes, or arbitrary bytes. Writes are not
//     atomic and unsynced writes are not ordered: a later write may survive
//     where an earlier one is lost. The file's length may be anything
//     between its last synced length and its largest unsynced length, and
//     an unsynced [File.Truncate] may or may not have taken effect.
//  3. A name (after Create, Rename, Remove or MkdirAll) is durable only once
//     [Disk.SyncDir] on its parent directory has returned nil. Until then a
//     crash may leave either the old or the new directory entry.
//  4. Rename is atomic across a crash: the new name refers to the old file
//     or the new one, never to neither.
//  5. If Sync or SyncDir returns an error, nothing may be assumed about
//     unsynced data, and retrying proves nothing. The node must stop by
//     returning the error from its handler.
//
// Paths use forward slashes and are relative to the node's data directory.
type Disk interface {
	// Open opens the named file according to flag. Opening a missing file
	// without Create returns an error wrapping [ErrNotExist].
	Open(name string, flag OpenFlag) (File, error)

	// MkdirAll creates dir and any missing parents. It succeeds if dir
	// already exists.
	MkdirAll(dir string) error

	// Remove deletes the named file.
	Remove(name string) error

	// Rename moves oldName to newName, replacing newName if it exists.
	Rename(oldName, newName string) error

	// List returns the names of the entries in dir, sorted.
	List(dir string) ([]string, error)

	// SyncDir makes the directory entries of dir durable.
	SyncDir(dir string) error
}

// File is an open file on a [Disk]. See [Disk] for what survives a crash.
type File interface {
	// ReadAt reads len(p) bytes starting at off. It follows io.ReaderAt: a
	// short read returns a non-nil error, io.EOF at end of file.
	ReadAt(p []byte, off int64) (n int, err error)

	// WriteAt writes p starting at off, extending the file if needed.
	WriteAt(p []byte, off int64) (n int, err error)

	// Size returns the current length of the file in bytes.
	Size() (int64, error)

	// Truncate sets the length of the file to size.
	Truncate(size int64) error

	// Sync makes all data written so far, and the file's length, durable.
	Sync() error

	// Close releases the file. It does not sync.
	Close() error
}
