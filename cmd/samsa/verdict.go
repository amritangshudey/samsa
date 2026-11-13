package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/amritangshudey/samsa/internal/broker"
	"github.com/amritangshudey/samsa/internal/realsys"
)

// faultsFile, in the data directory, holds the -faults spec of the last
// start, so that a verdict can say which seed to replay.
const faultsFile = "FAULTS"

// recordFaults writes the spec of this start to the data directory, or
// removes the file if there are no faults.
func recordFaults(dir string, f broker.Faults) error {
	name := filepath.Join(dir, faultsFile)
	if f.Rate <= 0 {
		if err := os.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("remove %s: %w", name, err)
		}
		return nil
	}
	if err := os.WriteFile(name, []byte(f.String()+"\n"), 0o644); err != nil {
		return fmt.Errorf("record faults: %w", err)
	}
	return nil
}

// runVerdict is `samsa verdict -data dir -in T -out T [-group G]`: it reads
// a stopped node's data and says what the application did with each input
// record. It returns false (exit 1) if any record was lost, any was not
// yet processed, or, with -expect exactly-once, any was duplicated.
func runVerdict(args []string, out io.Writer) (bool, error) {
	fl := flag.NewFlagSet("verdict", flag.ContinueOnError)
	dataDir := fl.String("data", "data", "data directory of a stopped node")
	nodeID := fl.Int("node-id", 1, "the node's id")
	in := fl.String("in", "", "the topic the application reads (required)")
	outTopic := fl.String("out", "", "the topic the application writes, one record or more per input record, with its key (required)")
	group := fl.String("group", "", "the application's consumer group: records it never committed past are pending, not lost")
	expect := fl.String("expect", "at-least-once", "at-least-once or exactly-once: whether duplicates fail the verdict")
	if err := fl.Parse(args); err != nil {
		return false, err
	}
	if *in == "" || *outTopic == "" {
		return false, errors.New("-in and -out are required")
	}
	if *expect != "at-least-once" && *expect != "exactly-once" {
		return false, fmt.Errorf("-expect: %q is not at-least-once or exactly-once", *expect)
	}
	if _, err := os.Stat(*dataDir); err != nil {
		return false, err // OpenDisk would create it
	}
	disk, err := realsys.OpenDisk(*dataDir) // fails while a node holds it
	if err != nil {
		return false, err
	}
	defer disk.Close() //nolint:errcheck // read only
	ro := &readOnlyDisk{Disk: disk}
	v, err := broker.Judge(ro, broker.Config{NodeID: int32(*nodeID)}, broker.Pipeline{In: *in, Out: *outTopic, Group: *group})
	if err != nil {
		return false, err
	}
	faults := "none"
	if b, err := os.ReadFile(filepath.Join(*dataDir, faultsFile)); err == nil {
		faults = strings.TrimSpace(string(b))
	}
	say := func(format string, a ...any) {
		fmt.Fprintf(out, format, a...) //nolint:errcheck // nowhere to report a failed print
	}
	ok := v.Lost == 0 && v.Pending == 0 && (v.Duplicated == 0 || *expect == "at-least-once")
	result := "PASS"
	if !ok {
		result = "FAIL"
	}
	say("%s: %d input records of %q, %d output records in %q\n", result, v.Inputs, *in, v.Outputs, *outTopic)
	say("  processed once %d, duplicated %d, lost %d, not yet processed %d; output records matching no input %d\n",
		v.Once, v.Duplicated, v.Lost, v.Pending, v.Unmatched)
	say("  faults: %s\n", faults)
	list := func(what string, n int, recs []broker.RecordRef) {
		if n == 0 {
			return
		}
		say("%s (%d; the first %d):\n", what, n, len(recs))
		for _, r := range recs {
			say("  %s/%d offset %d key %q", *in, r.Partition, r.Offset, r.Key)
			if r.Outputs > 1 {
				say(" written %d times", r.Outputs)
			}
			say("\n")
		}
	}
	lostWhat := "lost: never written to the output"
	if *group != "" {
		lostWhat = fmt.Sprintf("lost: group %q committed past them, but they were never written to the output", *group)
	}
	list(lostWhat, v.Lost, v.LostRecords)
	list("not yet processed: never written, and not committed past (did the application finish?)", v.Pending, v.PendingRecords)
	list("duplicated", v.Duplicated, v.DuplicatedRecords)
	if !ok && faults != "none" {
		say("replay the same faults: samsa -faults %s (the faults repeat when the requests arrive in the same order)\n", faults)
	}
	return ok, nil
}
