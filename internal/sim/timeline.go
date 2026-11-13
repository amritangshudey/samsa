package sim

import (
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/amritangshudey/samsa/internal/sys"
)

// timeline collects what a run did, for one HTML page with an SVG per
// panel (docs/design/a3-sim-tools.md, 2.2). It is filled only when
// Config.Timeline is set; it draws nothing and feeds no hash, so a run is
// the same with or without it. Every method is a no-op on nil.
type timeline struct {
	nodes, clients int
	spans          []tlSpan
	marks          []tlMark
	msgs           []tlMsg
	downAt         map[int32]sys.Instant
	leader         struct {
		id    int32
		term  int64
		since sys.Instant
		have  bool
	}
	cutSince  sys.Instant
	cutWhat   string
	cutOpen   bool
	end       sys.Instant
	violation string
	vt        sys.Instant
}

type tlSpan struct {
	lane     int // node id - 1, or -1 for the network lane
	from, to sys.Instant
	kind     string // "down", "leader", "cut"
	text     string
}

type tlMark struct {
	lane int // node id - 1, nodes+i for client i, -1 for the network lane
	at   sys.Instant
	kind string // "fault", "skipped", "ack", "read"
	text string
}

type tlMsg struct {
	from, to  int32
	sent, got sys.Instant
}

func newTimeline(nodes int) *timeline {
	return &timeline{nodes: nodes, downAt: map[int32]sys.Instant{}}
}

func (t *timeline) down(id int32, now sys.Instant, why string) {
	if t == nil {
		return
	}
	t.downAt[id] = now
	t.mark(int(id-1), now, "fault", why)
	if t.leader.have && t.leader.id == id {
		t.closeLeader(now)
	}
}

func (t *timeline) up(id int32, now sys.Instant) {
	if t == nil {
		return
	}
	if at, ok := t.downAt[id]; ok {
		t.spans = append(t.spans, tlSpan{lane: int(id - 1), from: at, to: now, kind: "down"})
		delete(t.downAt, id)
	}
}

func (t *timeline) newLeader(id int32, term int64, now sys.Instant) {
	if t == nil {
		return
	}
	t.closeLeader(now)
	t.leader.id, t.leader.term, t.leader.since, t.leader.have = id, term, now, true
}

func (t *timeline) closeLeader(now sys.Instant) {
	if t.leader.have {
		t.spans = append(t.spans, tlSpan{lane: int(t.leader.id - 1), from: t.leader.since, to: now, kind: "leader", text: fmt.Sprintf("term %d", t.leader.term)})
		t.leader.have = false
	}
}

func (t *timeline) cut(now sys.Instant, what string) {
	if t == nil {
		return
	}
	t.cutSince, t.cutWhat, t.cutOpen = now, what, true
}

func (t *timeline) heal(now sys.Instant) {
	if t == nil || !t.cutOpen {
		return
	}
	t.spans = append(t.spans, tlSpan{lane: -1, from: t.cutSince, to: now, kind: "cut", text: t.cutWhat})
	t.cutOpen = false
}

func (t *timeline) mark(lane int, now sys.Instant, kind, text string) {
	if t == nil {
		return
	}
	t.marks = append(t.marks, tlMark{lane: lane, at: now, kind: kind, text: text})
}

func (t *timeline) msg(from, to int32, sent, got sys.Instant) {
	if t == nil {
		return
	}
	t.msgs = append(t.msgs, tlMsg{from: from, to: to, sent: sent, got: got})
}

func (t *timeline) fail(now sys.Instant, err error) {
	if t == nil || t.violation != "" {
		return
	}
	t.violation, t.vt = err.Error(), now
}

// window is how much virtual time before the violation (or the end) the
// second panel shows, with every peer message.
const window = 2e9

// write renders the page. title and setup head it; faults is the schedule.
func (t *timeline) write(out io.Writer, title, setup string, faults []Fault) error {
	end := t.end
	ids := make([]int32, 0, len(t.downAt))
	for id := range t.downAt {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		t.spans = append(t.spans, tlSpan{lane: int(id - 1), from: t.downAt[id], to: end, kind: "down"})
	}
	t.closeLeader(end)
	t.heal(end)
	focus := end
	if t.violation != "" {
		focus = t.vt
	}
	var b strings.Builder
	fmt.Fprintf(&b, `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>%s</title>
<style>
:root{--bg:#fff;--fg:#1d1d1f;--muted:#6b6b70;--line:#d8d8dc;--down:#c7c7cc;--leader:#2f7d4f;--cut:#b26b00;--fault:#c62828;--ack:#2f7d4f;--read:#2a5db0;--msg:#8a8a92}
@media (prefers-color-scheme: dark){:root{--bg:#161618;--fg:#ececf0;--muted:#9a9aa2;--line:#3a3a40;--down:#4a4a52;--leader:#4fae78;--cut:#e0a040;--fault:#ff6b6b;--ack:#4fae78;--read:#6f9ef0;--msg:#7a7a84}}
body{background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,sans-serif;margin:16px}
h1{font-size:18px;margin:0 0 4px}p,li{color:var(--muted)}.v{color:var(--fault);font-weight:600}
svg{display:block;width:100%%;height:auto;margin:8px 0 20px}text{fill:var(--fg);font-size:11px}
.down{fill:var(--down)}.leader{fill:var(--leader);opacity:.35}.cut{fill:var(--cut);opacity:.25}
.fault{fill:var(--fault)}.skipped{fill:none;stroke:var(--muted)}.ack{fill:var(--ack)}.read{fill:var(--read)}
.msg{stroke:var(--msg);stroke-width:.6;opacity:.7}.vl{stroke:var(--fault);stroke-width:1.5}.grid{stroke:var(--line)}
ol{font-family:ui-monospace,monospace;font-size:12px}
</style></head><body><h1>%s</h1><p>%s</p>`, esc(title), esc(title), esc(setup))
	if t.violation != "" {
		fmt.Fprintf(&b, `<p class="v">Violation at %.3f s: %s</p>`, secs(t.vt), esc(t.violation))
	}
	b.WriteString(`<p>Lanes: nodes, then the network (partitions), then clients. Grey: node down. Green band: leader, with its term. Red: a fault; hollow: a fault the run skipped. Client ticks: green acknowledged writes, blue checked reads.</p>`)
	fmt.Fprintf(&b, "<h2>The whole run</h2>")
	t.panel(&b, 0, end, false, focus)
	fmt.Fprintf(&b, "<h2>The last %.0f s before %s, with every message between nodes</h2>", window/1e9, map[bool]string{true: "the violation", false: "the end"}[t.violation != ""])
	t.panel(&b, max(0, focus-window), focus, true, focus)
	b.WriteString("<h2>Faults</h2><ol>")
	for _, f := range faults {
		skipped := ""
		if f.Skipped {
			skipped = " (skipped)"
		}
		fmt.Fprintf(&b, "<li>#%d %.3f s %s%s</li>", f.Seq, f.At.Seconds(), esc(f.What), skipped)
	}
	b.WriteString("</ol></body></html>\n")
	_, err := io.WriteString(out, b.String())
	return err
}

// esc escapes text for HTML.
var esc = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;").Replace

func secs(t sys.Instant) float64 { return float64(t-epochStart) / 1e9 }

// panel draws lanes from t0 to t1 (instants), with messages if asked.
func (t *timeline) panel(b *strings.Builder, t0, t1 sys.Instant, msgs bool, focus sys.Instant) {
	if t0 < epochStart {
		t0 = epochStart
	}
	const left, width, laneH = 70.0, 1100.0, 22.0
	lanes := t.nodes + 1 + t.clients
	height := float64(lanes)*laneH + 30
	span := float64(t1 - t0)
	if span <= 0 {
		span = 1
	}
	x := func(at sys.Instant) float64 { return left + width*float64(at-t0)/span }
	y := func(lane int) float64 {
		if lane < 0 {
			lane = t.nodes // the network lane
		} else if lane >= t.nodes {
			lane++
		}
		return float64(lane)*laneH + 4
	}
	in := func(a, z sys.Instant) bool { return z >= t0 && a <= t1 }
	clip := func(a sys.Instant) sys.Instant { return min(max(a, t0), t1) }
	fmt.Fprintf(b, `<svg viewBox="0 0 %.0f %.0f" role="img" aria-label="timeline">`, left+width+10, height)
	for lane := range lanes {
		name := ""
		switch {
		case lane < t.nodes:
			name = fmt.Sprintf("node %d", lane+1)
		case lane == t.nodes:
			name = "network"
		default:
			name = fmt.Sprintf("client %d", lane-t.nodes-1)
		}
		yy := float64(lane)*laneH + 4
		fmt.Fprintf(b, `<text x="4" y="%.1f">%s</text><line class="grid" x1="%.0f" x2="%.0f" y1="%.1f" y2="%.1f"/>`, yy+14, name, left, left+width, yy+laneH-2, yy+laneH-2)
	}
	for _, s := range t.spans {
		if !in(s.from, s.to) {
			continue
		}
		fmt.Fprintf(b, `<rect class="%s" x="%.1f" y="%.1f" width="%.1f" height="%.0f"><title>%s %s %.3f–%.3f s</title></rect>`,
			s.kind, x(clip(s.from)), y(s.lane)+2, max(x(clip(s.to))-x(clip(s.from)), 1), laneH-6, s.kind, esc(s.text), secs(s.from), secs(s.to))
		if s.kind == "leader" && x(clip(s.to))-x(clip(s.from)) > 40 {
			fmt.Fprintf(b, `<text x="%.1f" y="%.1f">%s</text>`, x(clip(s.from))+3, y(s.lane)+14, s.text)
		}
	}
	if msgs {
		for _, m := range t.msgs {
			if in(m.sent, m.got) {
				fmt.Fprintf(b, `<line class="msg" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"/>`, x(clip(m.sent)), y(int(m.from-1))+laneH/2-2, x(clip(m.got)), y(int(m.to-1))+laneH/2-2)
			}
		}
	}
	// Client ticks are many: one per pixel column and kind at most.
	seen := map[[3]int]bool{}
	for _, m := range t.marks {
		if !in(m.at, m.at) {
			continue
		}
		px := x(m.at)
		switch m.kind {
		case "ack", "read":
			k := [3]int{m.lane, int(px), len(m.kind)}
			if seen[k] {
				continue
			}
			seen[k] = true
			dy := 3.0
			if m.kind == "read" {
				dy = 10
			}
			fmt.Fprintf(b, `<rect class="%s" x="%.1f" y="%.1f" width="1" height="6"/>`, m.kind, px, y(m.lane)+dy)
		default:
			fmt.Fprintf(b, `<path class="%s" d="M%.1f %.1fl4 8h-8z"><title>%.3f s %s</title></path>`, m.kind, px, y(m.lane)+3, secs(m.at), esc(m.text))
		}
	}
	if t.violation != "" && focus >= t0 && focus <= t1 {
		fmt.Fprintf(b, `<line class="vl" x1="%.1f" x2="%.1f" y1="0" y2="%.0f"><title>%s</title></line>`, x(focus), x(focus), height-24, esc(t.violation))
	}
	for i := 0; i <= 10; i++ {
		at := t0 + sys.Instant(float64(i)*span/10)
		fmt.Fprintf(b, `<text x="%.1f" y="%.0f">%.2fs</text>`, x(at)-12, height-6, secs(at))
	}
	b.WriteString("</svg>")
}
