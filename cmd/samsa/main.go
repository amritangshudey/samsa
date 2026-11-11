// Command samsa is a Kafka-compatible message broker.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/amritangshudey/samsa/internal/broker"
	"github.com/amritangshudey/samsa/internal/realsys"
	"github.com/amritangshudey/samsa/internal/sys"
)

func main() {
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
	logLevel := flag.String("log-level", "info", "debug, info, warn or error")
	flag.Parse()

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
		Sync: policy, SyncInterval: *syncInterval, Logger: logger,
	}, loop, loop, disk, realsys.Random{})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
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
