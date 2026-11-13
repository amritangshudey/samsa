package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/amritangshudey/samsa/internal/wire"
)

// runNode is `samsa node add|remove`: it asks the cluster's leader to add
// or remove a voter with AddRaftVoter or RemoveRaftVoter
// (docs/design/c1b-membership-binary.md). Kafka's add-controller cannot
// describe a Samsa node, so this tool stands in for it.
//
// shortcut: plain TCP without SASL, so with -acl only ANONYMOUS can be
// granted ALTER on the cluster; add TLS and SASL flags when someone needs
// to change membership on a secured cluster.
func runNode(args []string, out io.Writer) (bool, error) {
	if len(args) == 0 || (args[0] != "add" && args[0] != "remove") {
		return false, errors.New("usage: samsa node add|remove -bootstrap host:port[,...] -id N [-client host:port -peer host:port]")
	}
	add := args[0] == "add"
	fs := flag.NewFlagSet("node "+args[0], flag.ContinueOnError)
	bootstrap := fs.String("bootstrap", "localhost:9092", "client addresses of cluster nodes, comma-separated; the leader is found among them")
	id := fs.Int("id", 0, "the node's id")
	client := fs.String("client", "", "add: host:port where Kafka clients reach the node (its -listen or -advertise)")
	peer := fs.String("peer", "", "add: host:port where other nodes reach it (its address in -peers)")
	timeout := fs.Duration("timeout", time.Minute, "give up after this long")
	if err := fs.Parse(args[1:]); err != nil {
		return false, err
	}
	if *id <= 0 {
		return false, errors.New("-id must be a positive node id")
	}
	var body interface{ Encode(*wire.Writer, int16) }
	key, version := wire.KeyRemoveRaftVoter, int16(0)
	if add {
		var ls []wire.Listener
		for _, l := range []struct{ name, addr string }{{"CLIENT", *client}, {"PEER", *peer}} {
			host, portText, err := net.SplitHostPort(l.addr)
			port, err2 := strconv.ParseUint(portText, 10, 16)
			if err != nil || err2 != nil || host == "" || port == 0 {
				return false, fmt.Errorf("-%s %q is not host:port", strings.ToLower(l.name), l.addr)
			}
			ls = append(ls, wire.Listener{Name: l.name, Host: host, Port: uint16(port)})
		}
		key, version = wire.KeyAddRaftVoter, 1
		body = &wire.AddRaftVoterRequest{VoterID: int32(*id), TimeoutMs: int32(min(*timeout, 5*time.Minute).Milliseconds()), Listeners: ls}
	} else {
		body = &wire.RemoveRaftVoterRequest{VoterID: int32(*id)}
	}

	deadline := time.Now().Add(*timeout)
	addrs := strings.Split(*bootstrap, ",")
	var last string
	for time.Now().Before(deadline) {
		for i := 0; i < len(addrs); i++ { // grows when a node names the leader
			addr := addrs[i]
			resp := &wire.VoterChangeResponse{}
			err := request(addr, key, version, body, resp, deadline)
			switch {
			case err != nil:
				last = err.Error()
			case resp.ErrorCode == wire.ErrNone:
				fmt.Fprintf(out, "node %d %s\n", *id, map[bool]string{true: "added", false: "removed"}[add]) //nolint:errcheck // nowhere to report it
				return true, nil
			case resp.ErrorCode == wire.ErrNotController:
				last = fmt.Sprintf("%s: error %d: %s", addr, resp.ErrorCode, resp.ErrorMessage)
				if lead := leaderAddr(addr, deadline); lead != "" && !slices.Contains(addrs, lead) {
					addrs = append(addrs, lead)
				}
			case resp.ErrorCode == wire.ErrRequestTimedOut:
				last = fmt.Sprintf("%s: error %d: %s", addr, resp.ErrorCode, resp.ErrorMessage) // try again
			default:
				fmt.Fprintf(out, "refused: error %d: %s\n", resp.ErrorCode, resp.ErrorMessage) //nolint:errcheck // as above
				return false, nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false, fmt.Errorf("gave up after %v; last: %s", *timeout, last)
}

// leaderAddr asks the node at addr where the leader takes client requests;
// "" if it does not know.
func leaderAddr(addr string, deadline time.Time) string {
	resp := &wire.MetadataResponse{}
	if request(addr, wire.KeyMetadata, 1, &wire.MetadataRequest{}, resp, deadline) != nil {
		return ""
	}
	for _, b := range resp.Brokers {
		if b.NodeID == resp.ControllerID && resp.ControllerID > 0 {
			return net.JoinHostPort(b.Host, strconv.Itoa(int(b.Port)))
		}
	}
	return ""
}

// request sends one request on a fresh connection and reads its answer
// into resp.
func request(addr string, key, version int16, body interface{ Encode(*wire.Writer, int16) }, resp interface{ Decode(*wire.Reader, int16) }, deadline time.Time) error {
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	defer c.Close()          //nolint:errcheck // one request per connection
	c.SetDeadline(deadline) //nolint:errcheck // a failed deadline shows up as a failed read
	h := wire.RequestHeader{Key: key, Version: version, CorrelationID: 1, ClientID: "samsa-node"}
	w := wire.Writer{B: make([]byte, 4)}
	h.Encode(&w)
	body.Encode(&w, version)
	binary.BigEndian.PutUint32(w.B, uint32(len(w.B)-4))
	if _, err := c.Write(w.B); err != nil {
		return err
	}
	var size [4]byte
	if _, err := io.ReadFull(c, size[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(size[:])
	if n > 1<<20 {
		return fmt.Errorf("%s: response of %d bytes", addr, n)
	}
	frame := make([]byte, n)
	if _, err := io.ReadFull(c, frame); err != nil {
		return err
	}
	r := wire.NewReader(frame)
	if corr := wire.DecodeResponseHeader(r, key, version); corr != 1 {
		return fmt.Errorf("%s: correlation id %d", addr, corr)
	}
	resp.Decode(r, version)
	if r.Err() != nil {
		return fmt.Errorf("%s: malformed response: %w", addr, r.Err())
	}
	return nil
}
