package main

import (
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/amritangshudey/samsa/internal/broker"
)

// published is the node's latest Metrics, taken on the loop's goroutine;
// the HTTP handlers read only this copy, never the node.
type published struct {
	m  broker.Metrics
	at time.Time
}

// publishEvery is how often the loop publishes; healthz allows staleLimit.
const (
	publishEvery = 250 * time.Millisecond
	staleLimit   = time.Second
)

// metricsHandler serves /metrics (Prometheus text format, version 0.0.4),
// /healthz (the loop is running) and /readyz (the node can serve).
func metricsHandler(latest *atomic.Pointer[published]) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		writeMetrics(w, latest.Load().m)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		if age := time.Since(latest.Load().at); age > staleLimit {
			http.Error(w, fmt.Sprintf("event loop stalled: last report %v ago", age.Round(time.Millisecond)), http.StatusServiceUnavailable)
			return
		}
		io.WriteString(w, "ok\n") //nolint:errcheck // the client went away
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		p := latest.Load()
		switch {
		case time.Since(p.at) > staleLimit:
			http.Error(w, "event loop stalled", http.StatusServiceUnavailable)
		case !p.m.Ready:
			http.Error(w, p.m.NotReady, http.StatusServiceUnavailable)
		default:
			io.WriteString(w, "ok\n") //nolint:errcheck // the client went away
		}
	})
	return mux
}

// writeMetrics writes m in the Prometheus text format. Every name is listed
// in the README.
func writeMetrics(w io.Writer, m broker.Metrics) {
	say := func(format string, a ...any) {
		fmt.Fprintf(w, format, a...) //nolint:errcheck // the scraper went away
	}
	metric := func(name, typ, help string, v any) {
		say("# HELP %s %s\n# TYPE %s %s\n%s %v\n", name, help, name, typ, name, v)
	}
	b := func(v bool) int {
		if v {
			return 1
		}
		return 0
	}
	say("# HELP samsa_requests_total Requests decoded, by Kafka API key.\n# TYPE samsa_requests_total counter\n")
	for key, n := range m.Requests {
		if n > 0 {
			say("samsa_requests_total{api_key=\"%d\"} %d\n", key, n)
		}
	}
	metric("samsa_received_bytes_total", "counter", "Bytes received from clients.", m.BytesIn)
	metric("samsa_sent_bytes_total", "counter", "Response bytes sent to clients.", m.BytesOut)
	metric("samsa_connections_total", "counter", "Client connections accepted.", m.Connections)
	metric("samsa_connections_dropped_total", "counter", "Client connections closed by the node for bad input or limits.", m.Dropped)
	metric("samsa_connections", "gauge", "Client connections open.", m.Open)
	metric("samsa_raft_leader", "gauge", "1 if this node is the Raft leader.", b(m.Leader))
	metric("samsa_serving", "gauge", "1 if this node serves as leader.", b(m.Serving))
	metric("samsa_ready", "gauge", "1 if /readyz answers 200.", b(m.Ready))
	metric("samsa_raft_term", "gauge", "Current Raft term.", m.Term)
	metric("samsa_raft_committed_entries", "gauge", "Log entries known committed.", m.Committed)
	metric("samsa_raft_applied_entries", "gauge", "Log entries applied.", m.Applied)
	metric("samsa_raft_log_start", "gauge", "Index of the first entry kept in the log.", m.LogStart)
	metric("samsa_raft_last_index", "gauge", "Index of the last entry in the log.", m.LastIndex)
	metric("samsa_raft_snapshot_index", "gauge", "Index the newest snapshot covers, -1 if none.", m.Snapshot)
	metric("samsa_topics", "gauge", "Topics.", m.Topics)
	metric("samsa_partitions", "gauge", "Partitions, over all topics.", m.Partitions)
	metric("samsa_groups", "gauge", "Consumer groups.", m.Groups)
}
