package sim

import (
	"fmt"
	"math"
	"strings"
)

// Latent damage to data at rest (docs/design/b1-storage-faults.md). One node
// at a time has a sealed segment of its Raft log damaged on its disk. The
// scrub finds it, the node stops, and its next start cuts the log at the
// damage and fetches the rest from the others, without voting meanwhile.
// Nothing acknowledged may be lost: the checks that judge the whole run
// judge this too, and S7 adds that a recovering node casts no vote.

// rotInfo is the damage in flight: where it was made, to see whether it is
// still there.
type rotInfo struct {
	state rotState
	node  *simNode
	file  string
	at    int
	data  []byte
}

// rotOne damages one sealed segment of nd's log, if no damage is in flight,
// all the nodes are up, and the cluster is large enough to repair a node.
func (w *world) rotOne(nd *simNode) {
	if w.rot.state == rotInjected && !w.rot.node.disk.Holds(w.rot.file, w.rot.at, w.rot.data) {
		// A trim, an install or a truncation deleted it before any read or
		// scrub found it. Nothing is left to repair; waiting for it to be
		// found stopped rot for the rest of the run (BUGS.md M27).
		w.rot = rotInfo{}
	}
	if w.rot.state != rotNone || w.opts.nodes < 3 || nd.inc == nil {
		return
	}
	for _, o := range w.nodes {
		if o.inc == nil || o.inc.n.Raft().Recovering() {
			return // keep the damage to a minority
		}
	}
	names, err := nd.disk.List("raft")
	if err != nil {
		return
	}
	// Any sealed segment: below the snapshot too, where the data lives that
	// clients still read.
	var segs []string
	for i, name := range names {
		if strings.HasSuffix(name, ".log") && i < len(names)-1 {
			segs = append(segs, name)
		}
	}
	if len(segs) < 2 { // the last of them may be the active segment
		return
	}
	name := "raft/" + segs[w.rng.Intn(len(segs)-1)]
	size := nd.disk.FileSize(name)
	if size < 64 {
		return
	}
	at := w.rng.Intn(size - 1)
	data := make([]byte, 1+w.rng.Intn(min(64, size-at)))
	for i := range data {
		data[i] = byte(w.rng.Uint64())
	}
	if w.rng.Chance(0.3) { // a zeroed block
		data = make([]byte, min(512, size-at))
	}
	if !nd.disk.Corrupt(name, at, data) {
		return
	}
	w.note(nd.tag(fmt.Sprintf("rot: %d bytes of %s at %d", len(data), name, at)), 0, nil)
	w.stats.DamageInjected++
	w.rot = rotInfo{state: rotInjected, node: nd, file: name, at: at, data: data}
}

// checkRot runs with every monitor call: S7, and the end of recovery.
func (w *world) checkRot(nd *simNode, recovering bool) {
	if w.rot.node == nd && w.rot.state == rotRecovered && !recovering {
		w.rot.state = rotNone
	}
}

// findRot is the scrub, run once more when the run is over: damage that was
// made but not yet found is found now, so that it is repaired, not read.
func (w *world) findRot() {
	nd := w.rot.node
	if w.rot.state != rotInjected || nd == nil || nd.inc == nil {
		return
	}
	r := nd.inc.n.Raft()
	if _, _, _, found, err := r.Scrub(r.LogStart(), math.MaxInt); err == nil && found {
		w.stats.DamageFound++
		w.rot.state = rotFound
		w.down(nd, false, "damage found")
	}
}
