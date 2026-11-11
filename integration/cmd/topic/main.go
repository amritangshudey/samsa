// Command topic creates a topic on a samsa (or any Kafka) broker, for the
// README's quick start: samsa never creates topics on its own.
//
//	go run ./cmd/topic -b localhost:9092 -partitions 3 demo
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func main() {
	brokers := flag.String("b", "localhost:9092", "a broker address")
	partitions := flag.Int("partitions", 1, "number of partitions")
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: topic [-b host:port] [-partitions n] name")
		os.Exit(2)
	}
	if err := create(*brokers, flag.Arg(0), int32(*partitions)); err != nil {
		fmt.Fprintln(os.Stderr, "topic:", err)
		os.Exit(1)
	}
}

func create(brokers, name string, partitions int32) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	if err != nil {
		return err
	}
	defer cl.Close()
	req := kmsg.NewPtrCreateTopicsRequest()
	t := kmsg.NewCreateTopicsRequestTopic()
	t.Topic, t.NumPartitions, t.ReplicationFactor = name, partitions, -1 // -1: every node
	req.Topics = append(req.Topics, t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	return kerr.ErrorForCode(resp.Topics[0].ErrorCode)
}
