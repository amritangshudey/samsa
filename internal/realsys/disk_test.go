package realsys

import (
	"errors"
	"testing"
)

func TestOpenDiskLocksTheDirectory(t *testing.T) {
	dir := t.TempDir()
	d, err := OpenDisk(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDisk(dir); !errors.Is(err, ErrInUse) {
		t.Fatalf("second open: got %v, want ErrInUse", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = OpenDisk(dir)
	if err != nil {
		t.Fatalf("open after close: %v", err)
	}
	d.Close() //nolint:errcheck // test end
}
