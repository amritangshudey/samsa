//go:build unsafe

package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/amritangshudey/samsa/internal/broker"
	"github.com/amritangshudey/samsa/internal/raft"
)

// A build with -tags unsafe has the hidden flag -unsafe, which switches on
// the broker's deliberate bugs by name, so that the chaos test can prove
// it catches a broken build (docs/design/a4-chaos.md). Release builds do
// not have it.
func init() {
	switches := map[string]func(*broker.Config){
		"no-lease":          func(c *broker.Config) { c.UnsafeNoLease = true },
		"serve-early":       func(c *broker.Config) { c.UnsafeServeEarly = true },
		"no-check-quorum":   func(c *broker.Config) { c.UnsafeRaft |= raft.MutNoCheckQuorum },
		"no-pre-vote":       func(c *broker.Config) { c.UnsafeRaft |= raft.MutNoPreVote },
		"no-sequence-check": func(c *broker.Config) { c.UnsafeNoSequenceCheck = true },
	}
	names := flag.String("unsafe", "", "comma-separated deliberate bugs to switch on (test builds only)")
	unsafeConfig = func(c *broker.Config) (string, error) {
		if *names == "" {
			return "", nil
		}
		for _, name := range strings.Split(*names, ",") {
			set, ok := switches[name]
			if !ok {
				return "", fmt.Errorf("-unsafe: unknown switch %q", name)
			}
			set(c)
		}
		return *names, nil
	}
}
