// Command samsa is a Kafka-compatible message broker.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/amritangshudey/samsa/internal/broker"
	"github.com/amritangshudey/samsa/internal/realsys"
	"github.com/amritangshudey/samsa/internal/sys"
)

// version is set by release builds: -ldflags "-X main.version=v0.1.0".
var version string

// versionString is version, or else what Go stamped into the build: the
// commit, and "+dirty" if the tree had uncommitted changes.
func versionString() string {
	if version != "" {
		return version
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	return info.Main.Version
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "check" {
		ok, err := runCheck(os.Args[2:], os.Stdout)
		if err != nil {
			fmt.Fprintln(os.Stderr, "samsa check:", err)
		}
		if !ok {
			os.Exit(1)
		}
		return
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "samsa:", err)
		os.Exit(1)
	}
}

func run() error {
	listen := flag.String("listen", "localhost:9092", "address to accept Kafka clients on")
	advertise := flag.String("advertise", "", "host:port that clients are told to connect to (default: the listen address)")
	dataDir := flag.String("data", "data", "data directory")
	syncMode := flag.String("sync", "always", "when a write is acknowledged: always (after fsync), interval (before fsync, synced on a timer), never")
	syncInterval := flag.Duration("sync-interval", time.Second, "fsync period for -sync=interval")
	nodeID := flag.Int("node-id", 1, "this node's id")
	peersText := flag.String("peers", "", "every node of the cluster, this one included, as id=clientHost:port/peerHost:port,... (default: a cluster of this node alone)")
	retention := flag.Int64("retention-bytes", 0, "keep about this many bytes of log, deleting the oldest segments (0: keep everything)")
	producerExpiry := flag.Duration("producer-expiry", 24*time.Hour, "forget an idempotent producer after it writes nothing for this long (negative: never)")
	logLevel := flag.String("log-level", "info", "debug, info, warn or error")
	metricsListen := flag.String("metrics-listen", "", "address to serve /metrics, /healthz and /readyz on over HTTP (default: none)")
	configFile := flag.String("config", "", "file of key = value lines, one per flag; flags on the command line win")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *configFile != "" {
		if err := loadConfig(flag.CommandLine, *configFile); err != nil {
			return err
		}
	}
	if *showVersion {
		fmt.Println("samsa", versionString())
		return nil
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("-log-level: %w", err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	policy, ok := map[string]broker.SyncPolicy{
		"always": broker.SyncAlways, "interval": broker.SyncInterval, "never": broker.SyncNever,
	}[*syncMode]
	if !ok {
		return fmt.Errorf("-sync: unknown policy %q", *syncMode)
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	if *advertise == "" {
		*advertise = ln.Addr().String() // resolves a ":0" port
	}
	host, portText, err := net.SplitHostPort(*advertise)
	if err != nil {
		return fmt.Errorf("-advertise: %w", err)
	}
	port, err := strconv.ParseInt(portText, 10, 32)
	if err != nil {
		return fmt.Errorf("-advertise: %w", err)
	}

	disk, err := realsys.OpenDisk(*dataDir)
	if err != nil {
		return err
	}
	defer disk.Close() //nolint:errcheck // exiting

	loop := realsys.NewLoop(ln, 0)
	var peers []broker.Peer
	if *peersText != "" {
		var addrs map[sys.NodeID]string
		if peers, addrs, err = parsePeers(*peersText); err != nil {
			return fmt.Errorf("-peers: %w", err)
		}
		// A node that forgets acknowledged entries breaks Raft (ADR 0012).
		if len(peers) > 1 && policy != broker.SyncAlways {
			return fmt.Errorf("-sync=%s: a cluster of several nodes needs -sync=always", *syncMode)
		}
		own, ok := addrs[sys.NodeID(*nodeID)]
		if !ok {
			return fmt.Errorf("-peers: node %d (-node-id) is not in the list", *nodeID)
		}
		peerLn, err := net.Listen("tcp", own)
		if err != nil {
			return fmt.Errorf("listen for peers: %w", err)
		}
		loop.Peers(sys.NodeID(*nodeID), peerLn, addrs)
	}
	node := broker.New(broker.Config{
		NodeID: int32(*nodeID), Host: host, Port: int32(port), Peers: peers,
		Sync: policy, SyncInterval: *syncInterval, Logger: logger, RetentionBytes: *retention, ProducerExpiry: *producerExpiry,
	}, loop, loop, disk, realsys.Random{})

	// The first signal starts a graceful stop (the node hands leadership
	// over); a second one, once the handler is unregistered, kills at once.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	context.AfterFunc(ctx, stop)
	if *metricsListen != "" {
		mln, err := net.Listen("tcp", *metricsListen)
		if err != nil {
			return fmt.Errorf("-metrics-listen: %w", err)
		}
		latest := &atomic.Pointer[published]{}
		latest.Store(&published{m: broker.Metrics{NotReady: "starting"}, at: time.Now()})
		loop.Every(publishEvery, func() { latest.Store(&published{m: node.Metrics(), at: time.Now()}) })
		srv := &http.Server{Handler: metricsHandler(latest), ReadHeaderTimeout: 5 * time.Second}
		go srv.Serve(mln) //nolint:errcheck // ends with the process
		defer srv.Close() //nolint:errcheck // exiting
		logger.Info("serving metrics", "address", mln.Addr().String())
	}
	logger.Info("listening", "address", ln.Addr().String(), "advertised", *advertise, "data", *dataDir, "sync", *syncMode)
	return loop.Run(ctx, node)
}

// parsePeers reads "id=clientHost:port/peerHost:port,...": for each node,
// where Kafka clients reach it and where the other nodes do.
func parsePeers(text string) ([]broker.Peer, map[sys.NodeID]string, error) {
	var peers []broker.Peer
	addrs := map[sys.NodeID]string{}
	for _, item := range strings.Split(text, ",") {
		idText, rest, ok := strings.Cut(item, "=")
		client, peer, ok2 := strings.Cut(rest, "/")
		id, err := strconv.ParseInt(idText, 10, 32)
		if !ok || !ok2 || err != nil || id < 0 {
			return nil, nil, fmt.Errorf("%q is not id=clientHost:port/peerHost:port", item)
		}
		host, portText, err := net.SplitHostPort(client)
		if err != nil {
			return nil, nil, fmt.Errorf("%q: %w", item, err)
		}
		port, err := strconv.ParseInt(portText, 10, 32)
		if err != nil {
			return nil, nil, fmt.Errorf("%q: %w", item, err)
		}
		if _, _, err := net.SplitHostPort(peer); err != nil {
			return nil, nil, fmt.Errorf("%q: %w", item, err)
		}
		if _, dup := addrs[sys.NodeID(id)]; dup {
			return nil, nil, fmt.Errorf("node %d appears twice", id)
		}
		peers = append(peers, broker.Peer{ID: int32(id), Host: host, Port: int32(port)})
		addrs[sys.NodeID(id)] = peer
	}
	return peers, addrs, nil
}

// loadConfig sets each flag named in file that the command line did not.
// Lines are "key = value"; blank lines and lines starting with # are
// skipped. An unknown key is an error: a typo must not run with a default.
func loadConfig(fs *flag.FlagSet, file string) error {
	b, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("-config: %w", err)
	}
	given, seen := map[string]bool{}, map[string]bool{}
	fs.Visit(func(f *flag.Flag) { given[f.Name] = true })
	for i, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		switch {
		case !ok:
			return fmt.Errorf("%s:%d: want key = value", file, i+1)
		case fs.Lookup(key) == nil || key == "config":
			return fmt.Errorf("%s:%d: unknown key %q", file, i+1, key)
		case seen[key]:
			return fmt.Errorf("%s:%d: %q is set twice", file, i+1, key)
		}
		seen[key] = true
		if given[key] {
			continue
		}
		if err := fs.Set(key, value); err != nil {
			return fmt.Errorf("%s:%d: %s: %w", file, i+1, key, err)
		}
	}
	return nil
}
