package broker

import (
	"slices"

	"github.com/amritangshudey/samsa/internal/scram"
	"github.com/amritangshudey/samsa/internal/wire"
)

// SCRAM credentials managed through the Kafka APIs
// (docs/design/c8b-credentials-forwarding.md, section 2). The semantics are
// ScramControlManager.alterCredentials and ScramImage.describe.

// mechanismName is the name of Kafka's ScramMechanism id, or "".
func mechanismName(id int8) string {
	switch id {
	case 1:
		return scram.SHA256
	case 2:
		return scram.SHA512
	}
	return ""
}

func mechanismID(name string) int8 {
	switch name {
	case scram.SHA256:
		return 1
	case scram.SHA512:
		return 2
	}
	return 0
}

// fileUser reports whether the -sasl-users file defines user. The file
// owns its names: the API cannot change them.
func (n *Node) fileUser(user string) bool {
	if n.cfg.SASL == nil {
		return false
	}
	_, ok := n.cfg.SASL.Users[user]
	return ok
}

func (n *Node) describeCredentials(c *conn, req *wire.DescribeUserScramCredentialsRequest) *wire.DescribeUserScramCredentialsResponse {
	resp := &wire.DescribeUserScramCredentialsResponse{}
	if !n.may(c, opDescribe, resCluster, clusterName) {
		resp.ErrorCode, resp.ErrorMessage = wire.ErrClusterAuthorizationFailed, "needs DESCRIBE permission on the cluster"
		return resp
	}
	users := req.Users
	if len(users) == 0 { // null or empty: every user
		users = sortedKeys(n.st.creds)
		if n.cfg.SASL != nil {
			users = append(users, sortedKeys(n.cfg.SASL.Users)...)
			slices.Sort(users)
			users = slices.Compact(users)
		}
	}
	seen := map[string]int{}
	for _, u := range users {
		seen[u]++
	}
	for _, u := range users {
		if seen[u] < 0 {
			continue // a duplicate, already answered
		}
		res := wire.UserCredentials{User: u}
		switch creds := n.credentials(u); {
		case seen[u] > 1:
			res.ErrorCode, res.ErrorMessage = wire.ErrDuplicateResource, "Cannot describe SCRAM credentials for the same user twice in a single request: "+u
		case len(creds) == 0:
			res.ErrorCode, res.ErrorMessage = wire.ErrResourceNotFound, "Attempt to describe a user credential that does not exist: "+u
		default:
			for _, c := range creds {
				res.Infos = append(res.Infos, wire.CredentialInfo{Mechanism: mechanismID(c.Mechanism), Iterations: int32(c.Iterations)})
			}
		}
		seen[u] = -1
		resp.Results = append(resp.Results, res)
	}
	return resp
}

// alterCredentials validates every change, then proposes the valid ones as
// one command: they apply together, as Kafka applies its batch.
func (n *Node) alterCredentials(c *conn, h wire.RequestHeader, req *wire.AlterUserScramCredentialsRequest) {
	resp := &wire.AlterUserScramCredentialsResponse{}
	fail := map[string]wire.UserResult{}
	var order []string // users in the order first named, for a stable answer
	named := map[string]int{}
	note := func(user string) {
		if named[user]++; named[user] == 1 {
			order = append(order, user)
		}
	}
	for _, d := range req.Deletions {
		note(d.Name)
	}
	for _, u := range req.Upsertions {
		note(u.Name)
	}
	refuse := func(user string, code int16, msg string) {
		if _, done := fail[user]; !done {
			fail[user] = wire.UserResult{User: user, ErrorCode: code, ErrorMessage: msg}
		}
	}
	check := func(user string, mech int8) {
		switch {
		case !n.may(c, opAlter, resCluster, clusterName):
			refuse(user, wire.ErrClusterAuthorizationFailed, "needs ALTER permission on the cluster")
		case named[user] > 1:
			refuse(user, wire.ErrDuplicateResource, "A user credential cannot be altered twice in the same request")
		case user == "":
			refuse(user, wire.ErrUnacceptableCredential, "Username must not be empty")
		case mechanismName(mech) == "":
			refuse(user, wire.ErrUnsupportedSaslMechanism, "Unknown SCRAM mechanism")
		case n.fileUser(user):
			refuse(user, wire.ErrUnacceptableCredential, "user is defined in -sasl-users; change it there")
		}
	}
	var dels [][2]string
	for _, d := range req.Deletions {
		check(d.Name, d.Mechanism)
		if _, bad := fail[d.Name]; bad {
			continue
		}
		mech := mechanismName(d.Mechanism)
		if !slices.ContainsFunc(n.st.creds[d.Name], func(c scram.Credential) bool { return c.Mechanism == mech }) {
			refuse(d.Name, wire.ErrResourceNotFound, "Attempt to delete a user credential that does not exist")
			continue
		}
		dels = append(dels, [2]string{d.Name, mech})
	}
	var ups []upsert
	for _, u := range req.Upsertions {
		check(u.Name, u.Mechanism)
		if _, bad := fail[u.Name]; bad {
			continue
		}
		switch {
		case u.Iterations < scram.MinIterations:
			refuse(u.Name, wire.ErrUnacceptableCredential, "Too few iterations")
			continue
		case u.Iterations > scram.MaxIterations:
			refuse(u.Name, wire.ErrUnacceptableCredential, "Too many iterations")
			continue
		}
		cred, err := scram.FromSalted(mechanismName(u.Mechanism), u.Salt, u.SaltedPassword, int(u.Iterations))
		if err != nil {
			refuse(u.Name, wire.ErrUnacceptableCredential, err.Error())
			continue
		}
		ups = append(ups, upsert{u.Name, cred})
	}
	answer := func(code int16) {
		for _, user := range order {
			if f, bad := fail[user]; bad {
				resp.Results = append(resp.Results, f)
			} else {
				resp.Results = append(resp.Results, wire.UserResult{User: user, ErrorCode: code})
			}
		}
		n.respond(c, h, resp)
	}
	if len(dels) == 0 && len(ups) == 0 {
		answer(wire.ErrNone)
		return
	}
	n.propose(encodeAlterCredentials(dels, ups), func(r result) { answer(r.errCode) })
}

// describeQuotas answers DescribeClientQuotas, which kafka-configs.sh
// sends with every --describe of users. Samsa has no quotas, so the list
// is empty; authorization is Kafka's (DESCRIBE_CONFIGS on the cluster).
func (n *Node) describeQuotas(c *conn) *wire.DescribeClientQuotasResponse {
	if !n.may(c, opDescribeConfigs, resCluster, clusterName) {
		return &wire.DescribeClientQuotasResponse{ErrorCode: wire.ErrClusterAuthorizationFailed, ErrorMessage: "needs DESCRIBE_CONFIGS permission on the cluster"}
	}
	return &wire.DescribeClientQuotasResponse{}
}
