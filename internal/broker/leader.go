package broker

import "github.com/amritangshudey/samsa/internal/wire"

// canRead reports whether this node may answer requests that only the
// leader answers: it serves, and holds the lease, so no other node can
// have been elected and accepted writes it has not seen (ADR 0013).
//
// Writes need it too. A write is decided by the log, but its refusals are
// not: "unknown topic" for a produce, or for an offset commit, is read
// from applied state before anything is proposed (BUGS.md S4).
func (n *Node) canRead() bool {
	return n.serving && (n.cfg.UnsafeNoLease || n.raft.Lease())
}

// leader returns the node that this one believes leads the cluster. A
// node names itself only while it can answer reads: otherwise it would
// refuse the requests a client sends to a leader.
func (n *Node) leader() (Peer, bool) {
	if id, ok := n.raft.Leader(); ok && (int32(id) != n.cfg.NodeID || n.canRead()) {
		for _, p := range n.cfg.Peers {
			if p.ID == int32(id) {
				return p, true
			}
		}
	}
	return Peer{ID: -1}, false
}

// refusal is the answer of a node that is not serving as leader: for each
// API, the retriable error that makes a client look for the leader again
// (ADR 0011). It returns nil for the requests any node answers.
func refusal(req wire.Message) wire.Message {
	switch m := req.(type) {
	case *wire.CreateTopicsRequest:
		resp := &wire.CreateTopicsResponse{}
		for _, t := range m.Topics {
			resp.Topics = append(resp.Topics, wire.CreateTopicResult{Name: t.Name, ErrorCode: wire.ErrNotController, ErrorMessage: "this node is not the leader"})
		}
		return resp
	case *wire.ProduceRequest:
		resp := &wire.ProduceResponse{}
		for _, t := range m.Topics {
			tr := wire.ProduceTopicResponse{Name: t.Name}
			for _, p := range t.Partitions {
				tr.Partitions = append(tr.Partitions, wire.ProducePartitionResponse{Index: p.Index, ErrorCode: wire.ErrNotLeaderOrFollower, BaseOffset: -1})
			}
			resp.Topics = append(resp.Topics, tr)
		}
		return resp
	case *wire.FetchRequest:
		resp := &wire.FetchResponse{}
		for _, t := range m.Topics {
			tr := wire.FetchTopicResponse{Name: t.Name}
			for _, p := range t.Partitions {
				tr.Partitions = append(tr.Partitions, wire.FetchPartitionResponse{Index: p.Index, ErrorCode: wire.ErrNotLeaderOrFollower, HighWatermark: -1})
			}
			resp.Topics = append(resp.Topics, tr)
		}
		return resp
	case *wire.ListOffsetsRequest:
		resp := &wire.ListOffsetsResponse{}
		for _, t := range m.Topics {
			tr := wire.ListOffsetsTopicResponse{Name: t.Name}
			for _, p := range t.Partitions {
				tr.Partitions = append(tr.Partitions, wire.ListOffsetsPartitionResponse{Index: p.Index, ErrorCode: wire.ErrNotLeaderOrFollower, Timestamp: -1, Offset: -1})
			}
			resp.Topics = append(resp.Topics, tr)
		}
		return resp
	case *wire.JoinGroupRequest:
		return &wire.JoinGroupResponse{ErrorCode: wire.ErrNotCoordinator, GenerationID: -1}
	case *wire.SyncGroupRequest:
		return &wire.SyncGroupResponse{ErrorCode: wire.ErrNotCoordinator}
	case *wire.HeartbeatRequest:
		return &wire.HeartbeatResponse{ErrorCode: wire.ErrNotCoordinator}
	case *wire.LeaveGroupRequest:
		return &wire.LeaveGroupResponse{ErrorCode: wire.ErrNotCoordinator}
	case *wire.OffsetCommitRequest:
		resp := &wire.OffsetCommitResponse{}
		for _, t := range m.Topics {
			tr := wire.OffsetCommitTopicResponse{Name: t.Name}
			for _, p := range t.Partitions {
				tr.Partitions = append(tr.Partitions, wire.PartitionError{Index: p.Index, ErrorCode: wire.ErrNotCoordinator})
			}
			resp.Topics = append(resp.Topics, tr)
		}
		return resp
	case *wire.OffsetFetchRequest:
		// Version 1 has no top-level error, so each partition carries it too.
		resp := &wire.OffsetFetchResponse{ErrorCode: wire.ErrNotCoordinator}
		for _, t := range m.Topics {
			tr := wire.OffsetFetchTopicResponse{Name: t.Name}
			for _, index := range t.Partitions {
				tr.Partitions = append(tr.Partitions, wire.OffsetFetchPartition{Index: index, Offset: -1, LeaderEpoch: -1, ErrorCode: wire.ErrNotCoordinator})
			}
			resp.Topics = append(resp.Topics, tr)
		}
		return resp
	case *wire.DeleteTopicsRequest:
		resp := &wire.DeleteTopicsResponse{}
		for _, t := range m.Topics {
			resp.Topics = append(resp.Topics, wire.DeleteTopicResult{Name: t.Name, TopicID: t.TopicID, ErrorCode: wire.ErrNotController, ErrorMessage: "this node is not the leader"})
		}
		return resp
	case *wire.CreatePartitionsRequest:
		resp := &wire.CreatePartitionsResponse{}
		for _, t := range m.Topics {
			resp.Results = append(resp.Results, wire.CreatePartitionsResult{Name: t.Name, ErrorCode: wire.ErrNotController, ErrorMessage: "this node is not the leader"})
		}
		return resp
	case *wire.DeleteRecordsRequest:
		resp := &wire.DeleteRecordsResponse{}
		for _, t := range m.Topics {
			tr := wire.DeleteRecordsTopicResult{Name: t.Name}
			for _, p := range t.Partitions {
				tr.Partitions = append(tr.Partitions, wire.DeleteRecordsPartitionResult{Index: p.Index, LowWatermark: -1, ErrorCode: wire.ErrNotLeaderOrFollower})
			}
			resp.Topics = append(resp.Topics, tr)
		}
		return resp
	case *wire.AlterUserScramCredentialsRequest:
		resp := &wire.AlterUserScramCredentialsResponse{}
		seen := map[string]bool{}
		for _, d := range m.Deletions {
			if !seen[d.Name] {
				seen[d.Name] = true
				resp.Results = append(resp.Results, wire.UserResult{User: d.Name, ErrorCode: wire.ErrNotController, ErrorMessage: "this node is not the leader"})
			}
		}
		for _, u := range m.Upsertions {
			if !seen[u.Name] {
				seen[u.Name] = true
				resp.Results = append(resp.Results, wire.UserResult{User: u.Name, ErrorCode: wire.ErrNotController, ErrorMessage: "this node is not the leader"})
			}
		}
		return resp
	case *wire.DescribeGroupsRequest:
		resp := &wire.DescribeGroupsResponse{}
		for _, g := range m.Groups {
			resp.Groups = append(resp.Groups, wire.DescribedGroup{ErrorCode: wire.ErrNotCoordinator, ID: g})
		}
		return resp
	}
	// APIVersions, Metadata, FindCoordinator, DescribeConfigs,
	// DescribeCluster and DescribeAcls are answered by every node, and
	// CreateAcls and DeleteAcls forwarded to the leader (forward.go); ListGroups by every node,
	// empty unless it serves.
	return nil
}
