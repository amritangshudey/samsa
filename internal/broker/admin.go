package broker

import (
	"slices"
	"strconv"
	"strings"

	"github.com/amritangshudey/samsa/internal/wire"
)

// The admin APIs of docs/design/b4-operations.md. Those that change state
// propose a command and answer once it is applied, as CreateTopics does;
// the others read applied state.

// collect answers a request once every proposal it made has been applied.
// The count starts at 1 for the caller's own loop, so that a proposal
// applied at once cannot answer while later parts are still unseen; the
// caller calls the returned done when its loop ends.
func collect(answer func()) (add func(), done func()) {
	pending := 1
	done = func() {
		if pending--; pending == 0 {
			answer()
		}
	}
	return func() { pending++ }, done
}

func (n *Node) deleteTopics(c *conn, h wire.RequestHeader, req *wire.DeleteTopicsRequest) {
	resp := &wire.DeleteTopicsResponse{Topics: make([]wire.DeleteTopicResult, len(req.Topics))}
	add, done := collect(func() { n.respond(c, h, resp) })
	seen := map[string]bool{}
	for i, t := range req.Topics {
		res := &resp.Topics[i]
		res.Name = t.Name
		switch {
		case t.Name == "":
			res.TopicID = t.TopicID
			res.ErrorCode, res.ErrorMessage = wire.ErrUnknownTopicID, "topics are named by name only (ADR 0016)"
		case seen[t.Name]:
			res.ErrorCode, res.ErrorMessage = wire.ErrInvalidRequest, "topic appears twice in the request"
		case n.st.topics[t.Name] == nil:
			res.ErrorCode = wire.ErrUnknownTopicOrPartition
		}
		seen[t.Name] = true
		if res.ErrorCode != wire.ErrNone {
			continue
		}
		add()
		n.propose(encodeDeleteTopic(t.Name), func(r result) {
			res.ErrorCode = r.errCode
			done()
		})
	}
	done()
}

func (n *Node) createPartitions(c *conn, h wire.RequestHeader, req *wire.CreatePartitionsRequest) {
	resp := &wire.CreatePartitionsResponse{Results: make([]wire.CreatePartitionsResult, len(req.Topics))}
	add, done := collect(func() { n.respond(c, h, resp) })
	seen := map[string]bool{}
	for i, t := range req.Topics {
		res := &resp.Results[i]
		res.Name = t.Name
		switch tp := n.st.topics[t.Name]; {
		case seen[t.Name]:
			res.ErrorCode, res.ErrorMessage = wire.ErrInvalidRequest, "topic appears twice in the request"
		case tp == nil:
			res.ErrorCode = wire.ErrUnknownTopicOrPartition
		case t.HasAssignments:
			res.ErrorCode, res.ErrorMessage = wire.ErrInvalidReplicaAssignment, "every node holds every partition: there is nothing to assign"
		case int(t.Count) <= len(tp.parts):
			res.ErrorCode, res.ErrorMessage = wire.ErrInvalidPartitions, "a topic can only grow; it has "+strconv.Itoa(len(tp.parts))+" partitions"
		case t.Count > n.cfg.MaxPartitions:
			res.ErrorCode, res.ErrorMessage = wire.ErrInvalidPartitions, "partition count out of range"
		}
		seen[t.Name] = true
		if res.ErrorCode != wire.ErrNone || req.ValidateOnly {
			continue
		}
		add()
		n.propose(encodeCreatePartitions(t.Name, t.Count), func(r result) {
			res.ErrorCode = r.errCode
			if r.errCode == wire.ErrInvalidPartitions {
				res.ErrorMessage = "another request grew the topic first"
			}
			done()
		})
	}
	done()
}

func (n *Node) deleteRecords(c *conn, h wire.RequestHeader, req *wire.DeleteRecordsRequest) {
	resp := &wire.DeleteRecordsResponse{Topics: make([]wire.DeleteRecordsTopicResult, len(req.Topics))}
	add, done := collect(func() { n.respond(c, h, resp) })
	for i, t := range req.Topics {
		resp.Topics[i] = wire.DeleteRecordsTopicResult{Name: t.Name, Partitions: make([]wire.DeleteRecordsPartitionResult, len(t.Partitions))}
		for j, q := range t.Partitions {
			res := &resp.Topics[i].Partitions[j]
			res.Index, res.LowWatermark = q.Index, -1
			switch p := n.st.partition(t.Name, q.Index); {
			case p == nil:
				res.ErrorCode = wire.ErrUnknownTopicOrPartition
			case q.Offset < -1 || q.Offset > p.next:
				res.ErrorCode = wire.ErrOffsetOutOfRange
			}
			if res.ErrorCode != wire.ErrNone {
				continue
			}
			add()
			n.propose(encodeDeleteRecords(t.Name, q.Index, q.Offset), func(r result) {
				res.ErrorCode, res.LowWatermark = r.errCode, r.baseOffset
				done()
			})
		}
	}
	done()
}

// describeConfigs is answered by every node from its own configuration and
// applied state, like Metadata: nothing in it is ever changed by a client,
// and a follower is at most a little behind on which topics exist.
func (n *Node) describeConfigs(req *wire.DescribeConfigsRequest) *wire.DescribeConfigsResponse {
	retention := strconv.FormatInt(n.cfg.RetentionBytes, 10)
	if n.cfg.RetentionBytes <= 0 {
		retention = "-1"
	}
	segment := strconv.FormatInt(n.cfg.SegmentBytes, 10)
	if n.cfg.SegmentBytes == 0 {
		segment = strconv.FormatInt(64<<20, 10) // the storage default
	}
	maxBatch := strconv.Itoa(n.cfg.MaxBatchBytes)
	sync := [...]string{SyncAlways: "always", SyncInterval: "interval", SyncNever: "never"}[n.cfg.Sync]
	const str, integer, long, list = 2, 3, 5, 7 // ConfigType ids
	static := wire.ConfigSourceStaticBroker
	topicConfigs := []wire.ConfigEntry{
		{Name: "cleanup.policy", Value: "delete", Source: wire.ConfigSourceDefault, Type: list},
		{Name: "max.message.bytes", Value: maxBatch, Source: static, Type: integer},
		{Name: "retention.bytes", Value: retention, Source: static, Type: long},
		{Name: "retention.ms", Value: "-1", Source: wire.ConfigSourceDefault, Type: long},
		{Name: "segment.bytes", Value: segment, Source: static, Type: integer},
	}
	brokerConfigs := []wire.ConfigEntry{
		{Name: "log.retention.bytes", Value: retention, Source: static, Type: long},
		{Name: "log.segment.bytes", Value: segment, Source: static, Type: integer},
		{Name: "message.max.bytes", Value: maxBatch, Source: static, Type: integer},
		{Name: "samsa.sync", Value: sync, Source: static, Type: str},
	}
	resp := &wire.DescribeConfigsResponse{}
	for _, res := range req.Resources {
		out := wire.DescribeConfigsResult{Type: res.Type, Name: res.Name}
		var all []wire.ConfigEntry
		switch {
		case res.Type == wire.ResourceTopic && n.st.topics[res.Name] != nil:
			all = topicConfigs
		case res.Type == wire.ResourceTopic:
			out.ErrorCode, out.ErrorMessage = wire.ErrUnknownTopicOrPartition, "no such topic"
		case res.Type == wire.ResourceBroker && (res.Name == "" || res.Name == strconv.Itoa(int(n.cfg.NodeID))):
			all = brokerConfigs // "" is the cluster-wide default, which is the same
		case res.Type == wire.ResourceBroker:
			out.ErrorCode, out.ErrorMessage = wire.ErrInvalidRequest, "ask broker "+res.Name+" itself"
		default:
			out.ErrorCode, out.ErrorMessage = wire.ErrInvalidRequest, "only topic and broker configs exist"
		}
		for _, e := range all {
			if res.Keys == nil || slices.Contains(res.Keys, e.Name) {
				e.ReadOnly = true // there is no AlterConfigs
				out.Configs = append(out.Configs, e)
			}
		}
		resp.Results = append(resp.Results, out)
	}
	return resp
}

// groupState names a group's state as Kafka's ClassicGroupState does.
func (n *Node) groupState(name string) string {
	g := n.st.groups[name]
	switch {
	case g == nil:
		return "Dead"
	case n.groups[name] != nil && n.groups[name].round != nil:
		return "PreparingRebalance"
	case len(g.members) == 0:
		return "Empty"
	case !g.assigned:
		return "CompletingRebalance"
	}
	return "Stable"
}

// listGroups is sent by the admin client to every node, which merges the
// answers. Only the serving leader coordinates groups, so every other node
// answers an empty list, without error.
func (n *Node) listGroups(req *wire.ListGroupsRequest) *wire.ListGroupsResponse {
	resp := &wire.ListGroupsResponse{}
	if !n.canRead() {
		return resp
	}
	for _, name := range sortedKeys(n.st.groups) {
		lg := wire.ListedGroup{ID: name, ProtocolType: n.st.groups[name].protocolType, State: n.groupState(name), Type: "classic"}
		if len(req.States) > 0 && !slices.ContainsFunc(req.States, func(s string) bool { return strings.EqualFold(s, lg.State) }) ||
			len(req.Types) > 0 && !slices.ContainsFunc(req.Types, func(s string) bool { return strings.EqualFold(s, lg.Type) }) {
			continue
		}
		resp.Groups = append(resp.Groups, lg)
	}
	return resp
}

func (n *Node) describeGroups(req *wire.DescribeGroupsRequest) *wire.DescribeGroupsResponse {
	resp := &wire.DescribeGroupsResponse{}
	for _, name := range req.Groups {
		dg := wire.DescribedGroup{ID: name, State: n.groupState(name)}
		if g := n.st.groups[name]; g != nil {
			dg.ProtocolType = g.protocolType
			if dg.State == "Stable" || dg.State == "CompletingRebalance" {
				dg.ProtocolData = g.protocol
			}
			for _, m := range g.members {
				// Every member id is "<client id>-<random>" (newMemberID).
				// shortcut: the client's host is not applied state, so it is
				// left empty; add it to GroupGeneration if tools need it.
				dm := wire.DescribedMember{ID: m.id, Assignment: m.assignment}
				if i := strings.LastIndexByte(m.id, '-'); i >= 0 {
					dm.ClientID = m.id[:i]
				}
				for _, p := range m.protocols {
					if p.Name == g.protocol {
						dm.Metadata = p.Metadata
					}
				}
				dg.Members = append(dg.Members, dm)
			}
		}
		resp.Groups = append(resp.Groups, dg)
	}
	return resp
}

// describeCluster is answered by every node, like Metadata.
func (n *Node) describeCluster(req *wire.DescribeClusterRequest) *wire.DescribeClusterResponse {
	if req.EndpointType != wire.EndpointBrokers {
		return &wire.DescribeClusterResponse{ErrorCode: wire.ErrUnsupportedEndpointType, ErrorMessage: "Samsa has no separate controllers",
			EndpointType: req.EndpointType, ClusterID: n.cfg.ClusterID, ControllerID: -1}
	}
	leader, _ := n.leader()
	resp := &wire.DescribeClusterResponse{EndpointType: wire.EndpointBrokers, ClusterID: n.cfg.ClusterID, ControllerID: leader.ID}
	for _, p := range n.cfg.Peers {
		resp.Brokers = append(resp.Brokers, wire.Broker{NodeID: p.ID, Host: p.Host, Port: p.Port})
	}
	return resp
}
