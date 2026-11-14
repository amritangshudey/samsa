package broker

import (
	"flag"
	"fmt"
	"strings"
	"testing"
)

// A model of copy-forward retention in one shared log, for the C2
// decision (docs/design/c2-retention-compaction.md). It measures a policy,
// not the broker: records are unit-sized, time is ticks.
//
// The log can only be cut at its start (Raft truncates a prefix), so space
// is reclaimed only from the head segment. Before a head segment is
// dropped, its live records are appended again at the tail. Live means:
// within its topic's retention, or, for a compacted topic, the newest
// record of its key.

var retentionModel = flag.Bool("retention-model", false, "print the copy-forward retention model's table")

type modelTopic struct {
	share     float64 // of all records written
	retention int     // ticks; 0 for compacted
	keys      int     // compacted topics: distinct keys, updated uniformly
}

type modelRecord struct {
	topic, written, key, id int
}

type modelResult struct {
	writes, copies int     // records appended by clients, by the cleaner, while measured
	space          float64 // log size over live size, averaged over measured ticks
}

// runModel writes rate records per tick for ticks ticks, cleaning head
// segments while the log holds more than slack times the live records,
// and measures the second half.
func runModel(topics []modelTopic, rate, ticks, segment int, slack float64) modelResult {
	var (
		log      []modelRecord                // head first
		latest   = map[[2]int]int{}           // (topic, key) -> id of its newest record
		perTick  = make([][]int, len(topics)) // client writes per tick, retention topics
		res      modelResult
		spaceSum float64
		acc      = make([]float64, len(topics))
		id, seq  int
	)
	live := func(r modelRecord, now int) bool {
		if t := topics[r.topic]; t.retention > 0 {
			return now-r.written < t.retention
		}
		return latest[[2]int{r.topic, r.key}] == r.id
	}
	for now := range ticks {
		measure := now >= ticks/2
		for i, t := range topics {
			acc[i] += t.share * float64(rate)
			n := 0
			for ; acc[i] >= 1; acc[i]-- {
				r := modelRecord{topic: i, written: now, id: id}
				id++
				if t.retention == 0 {
					seq = (seq*1103515245 + 12345) & 0x7fffffff
					r.key = seq % t.keys
					latest[[2]int{i, r.key}] = r.id
				}
				log = append(log, r)
				n++
				if measure {
					res.writes++
				}
			}
			perTick[i] = append(perTick[i], n)
		}
		liveNow := len(latest)
		for i, t := range topics {
			if t.retention > 0 {
				for _, n := range perTick[i][max(0, now+1-t.retention):] {
					liveNow += n
				}
			}
		}
		for float64(len(log)) > slack*float64(max(liveNow, segment)) && len(log) > segment {
			head := log[:segment:segment]
			log = log[segment:]
			for _, r := range head {
				if live(r, now) {
					log = append(log, r)
					if measure {
						res.copies++
					}
				}
			}
		}
		if measure && liveNow > 0 {
			spaceSum += float64(len(log)) / float64(liveNow)
		}
	}
	res.space = spaceSum / float64(ticks-ticks/2)
	return res
}

func TestRetentionModel(t *testing.T) {
	if !*retentionModel {
		t.Skip("-retention-model not set")
	}
	const rate, segment = 100, 1000
	// One retention unit is 100 ticks, 10,000 records.
	scenarios := []struct {
		name   string
		topics []modelTopic
	}{
		{"one retention for every topic (today)", []modelTopic{{1, 700, 0}}},
		{"90% kept 1 unit, 10% kept 7", []modelTopic{{0.9, 100, 0}, {0.1, 700, 0}}},
		{"50% kept 1 unit, 50% kept 7", []modelTopic{{0.5, 100, 0}, {0.5, 700, 0}}},
		{"99% kept 1 unit, 1% kept 30", []modelTopic{{0.99, 100, 0}, {0.01, 3000, 0}}},
		{"compacted only, 10k keys", []modelTopic{{1, 0, 10000}}},
		{"90% kept 1 unit, 10% compacted (10k keys)", []modelTopic{{0.9, 100, 0}, {0.1, 0, 10000}}},
		{"90% kept 1 unit, 10% compacted (100k keys)", []modelTopic{{0.9, 100, 0}, {0.1, 0, 100000}}},
	}
	var b strings.Builder
	fmt.Fprintf(&b, "\n| Workload | Slack | Write amplification | Space over live |\n|---|---|---|---|\n")
	for _, sc := range scenarios {
		longest := 100
		for _, tp := range sc.topics {
			longest = max(longest, tp.retention)
		}
		for _, slack := range []float64{1.5, 2, 4} {
			r := runModel(sc.topics, rate, 6*longest, segment, slack)
			fmt.Fprintf(&b, "| %s | %.1f | %.2f | %.2f |\n", sc.name, slack, float64(r.writes+r.copies)/float64(r.writes), r.space)
		}
	}
	t.Log(b.String())
}

// The model's own checks. With one retention, records leave in the order
// they came, so nothing live reaches the head and nothing is copied. With
// two and a tight budget, the long-kept records are copied, and the log
// stays in budget.
func TestRetentionModelChecks(t *testing.T) {
	r := runModel([]modelTopic{{1, 100, 0}}, 10, 1000, 100, 2)
	if r.copies != 0 || r.writes == 0 || r.space < 1 || r.space > 2.1 {
		t.Fatalf("one retention: %+v, want no copies and space within the slack", r)
	}
	// At slack 2 the budget outlasts the longest retention and nothing
	// live reaches the head; at 1.5 it does.
	r = runModel([]modelTopic{{0.5, 100, 0}, {0.5, 700, 0}}, 10, 4200, 100, 1.5)
	if r.copies == 0 || r.space > 1.6 {
		t.Fatalf("two retentions: %+v, want copies and space within the slack", r)
	}
}
