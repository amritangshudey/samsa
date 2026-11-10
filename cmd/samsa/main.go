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
	"syscall"
	"time"

	"github.com/amritangshudey/samsa/internal/broker"
	"github.com/amritangshudey/samsa/internal/realsys"
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
	nodeID := flag.Int("node-id", 1, "broker id reported to clients")
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
	node := broker.New(broker.Config{
		NodeID: int32(*nodeID), Host: host, Port: int32(port),
		Sync: policy, SyncInterval: *syncInterval, Logger: logger,
	}, loop, loop, disk)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger.Info("listening", "address", ln.Addr().String(), "advertised", *advertise, "data", *dataDir, "sync", *syncMode)
	return loop.Run(ctx, node)
}
