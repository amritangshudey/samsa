package broker

import (
	"net/netip"
	"slices"
	"strings"

	"github.com/amritangshudey/samsa/internal/wire"
)

// ACL enums, from AclOperation, AclPermissionType, ResourceType and
// PatternType in kafka-clients (docs/design/c8-acls.md, section 1).
const (
	opAny             int8 = 1
	opAll             int8 = 2
	opRead            int8 = 3
	opWrite           int8 = 4
	opCreate          int8 = 5
	opDelete          int8 = 6
	opAlter           int8 = 7
	opDescribe        int8 = 8
	opClusterAction   int8 = 9
	opDescribeConfigs int8 = 10
	opAlterConfigs    int8 = 11
	opIdempotentWrite int8 = 12
	opMax             int8 = 15 // TWO_PHASE_COMMIT

	permAny   int8 = 1
	permDeny  int8 = 2
	permAllow int8 = 3

	resAny     int8 = 1
	resTopic   int8 = 2
	resGroup   int8 = 3
	resCluster int8 = 4
	resMax     int8 = 7 // USER

	patAny      int8 = 1
	patMatch    int8 = 2
	patLiteral  int8 = 3
	patPrefixed int8 = 4

	// clusterName is the only name of the CLUSTER resource.
	clusterName = "kafka-cluster"
	wildcard    = "*"
)

// Authorization configures ACLs (ADR 0020). With a nil *Authorization in
// Config, every authenticated request is allowed, as before.
type Authorization struct {
	// SuperUsers are principals ("User:admin") allowed everything.
	SuperUsers []string
	// AllowIfNoACL allows a request on a resource that has no ACL at all
	// (Kafka's allow.everyone.if.no.acl.found). The default denies it.
	AllowIfNoACL bool
}

// subject is who is asking: a principal and the client's address.
type subject struct{ principal, host string }

// validACL returns why Kafka would refuse to create a, or "" if it is
// valid (AclControlManager.validateNewAcl).
func validACL(a wire.ACL) string {
	switch {
	case a.ResourceType <= resAny || a.ResourceType > resMax:
		return "invalid resource type"
	case a.PatternType != patLiteral && a.PatternType != patPrefixed:
		return "invalid pattern type"
	case a.Operation <= opAny || a.Operation > opMax:
		return "invalid operation"
	case a.Permission != permAllow && a.Permission != permDeny:
		return "invalid permission type"
	case a.ResourceName == "":
		return "resource name should not be empty"
	case !strings.Contains(a.Principal, ":"):
		return "could not parse principal " + a.Principal + " (no colon separating the type from the name)"
	case a.Host == "":
		return "host pattern cannot be empty"
	}
	if strings.Contains(a.Host, "/") {
		if _, err := netip.ParsePrefix(a.Host); err != nil {
			return "invalid CIDR notation " + a.Host
		}
	}
	return ""
}

// validFilter reports whether a DeleteAcls or DescribeAcls filter is
// usable: no UNKNOWN (0) or out-of-range value (validateFilter).
func validFilter(f wire.ACLFilter) bool {
	return f.ResourceType > 0 && f.ResourceType <= resMax && f.PatternType > 0 && f.PatternType <= patPrefixed &&
		f.Operation > 0 && f.Operation <= opMax && f.Permission > 0 && f.Permission <= permAllow
}

// filterMatches is AclBindingFilter.matches: ResourcePatternFilter and
// AccessControlEntryFilter.
func filterMatches(f wire.ACLFilter, a wire.ACL) bool {
	if f.ResourceType != resAny && f.ResourceType != a.ResourceType {
		return false
	}
	if f.PatternType != patAny && f.PatternType != patMatch && f.PatternType != a.PatternType {
		return false
	}
	if f.ResourceName != nil {
		name := *f.ResourceName
		switch {
		case f.PatternType == patAny || f.PatternType == a.PatternType:
			if name != a.ResourceName {
				return false
			}
		case a.PatternType == patLiteral: // MATCH
			if name != a.ResourceName && a.ResourceName != wildcard {
				return false
			}
		case !strings.HasPrefix(name, a.ResourceName): // MATCH, PREFIXED
			return false
		}
	}
	return (f.Principal == nil || *f.Principal == a.Principal) &&
		(f.Host == nil || *f.Host == a.Host) &&
		(f.Operation == opAny || f.Operation == a.Operation) &&
		(f.Permission == permAny || f.Permission == a.Permission)
}

// entryApplies is StandardAuthorizerData.findResult without the resource:
// whether a speaks about this subject and operation.
func entryApplies(a wire.ACL, who subject, op int8) bool {
	if a.Principal != who.principal && a.Principal != "User:*" {
		return false
	}
	if a.Host != wildcard && a.Host != who.host && !inRange(who.host, a.Host) {
		return false
	}
	if a.Operation == opAll || a.Operation == op {
		return true
	}
	if a.Permission != permAllow {
		return false // a DENY READ does not deny DESCRIBE
	}
	switch op {
	case opDescribe:
		return a.Operation == opRead || a.Operation == opWrite || a.Operation == opDelete || a.Operation == opAlter
	case opDescribeConfigs:
		return a.Operation == opAlterConfigs
	}
	return false
}

func inRange(host, cidr string) bool {
	p, err := netip.ParsePrefix(cidr)
	if err != nil {
		return false
	}
	addr, err := netip.ParseAddr(host)
	return err == nil && p.Contains(addr.Unmap())
}

// nameMatches is whether the pattern of a covers the resource name.
func nameMatches(a wire.ACL, name string) bool {
	if a.PatternType == patPrefixed {
		return strings.HasPrefix(name, a.ResourceName)
	}
	return a.ResourceName == name || a.ResourceName == wildcard
}

// authorize decides one action, as StandardAuthorizerData does: super users
// pass; any matching DENY wins; else any matching ALLOW; else, if the
// resource has no ACL at all, the configured default; else denied.
func authorize(acls []wire.ACL, cfg *Authorization, who subject, op, resType int8, name string) bool {
	if cfg == nil {
		return true
	}
	for _, s := range cfg.SuperUsers {
		if s == who.principal {
			return true
		}
	}
	hasACLs, allowed := false, false
	for _, a := range acls {
		if a.ResourceType != resType || !nameMatches(a, name) {
			continue
		}
		hasACLs = true
		if entryApplies(a, who, op) {
			if a.Permission == permDeny {
				return false
			}
			allowed = true
		}
	}
	return allowed || (!hasACLs && cfg.AllowIfNoACL)
}

// authorizeAny is Authorizer.authorizeByResourceType: whether the subject
// may do op on at least one resource of the type. InitProducerId uses it
// with WRITE on topics.
func authorizeAny(acls []wire.ACL, cfg *Authorization, who subject, op, resType int8) bool {
	if authorize(acls, cfg, who, op, resType, "hardcode") { // super users and the no-ACL default, as Kafka probes
		return true
	}
	var denyLit, denyPre, allowLit, allowPre []string
	wildAllow := false
	for _, a := range acls {
		if a.ResourceType != resType || (a.Principal != who.principal && a.Principal != "User:*") ||
			(a.Host != wildcard && a.Host != who.host && !inRange(who.host, a.Host)) ||
			(a.Operation != op && a.Operation != opAll) {
			continue
		}
		lit := a.PatternType == patLiteral
		switch {
		case a.Permission == permDeny && lit && a.ResourceName == wildcard:
			return false
		case a.Permission == permDeny && lit:
			denyLit = append(denyLit, a.ResourceName)
		case a.Permission == permDeny:
			denyPre = append(denyPre, a.ResourceName)
		case lit && a.ResourceName == wildcard:
			wildAllow = true // decided after the loop: a wildcard DENY anywhere still wins
		case lit:
			allowLit = append(allowLit, a.ResourceName)
		default:
			allowPre = append(allowPre, a.ResourceName)
		}
	}
	if wildAllow {
		return true
	}
	dominated := func(name string) bool {
		for _, d := range denyPre {
			if strings.HasPrefix(name, d) {
				return true
			}
		}
		return false
	}
	for _, name := range allowLit {
		if !slices.Contains(denyLit, name) && !dominated(name) {
			return true
		}
	}
	for _, name := range allowPre {
		if !dominated(name) {
			return true
		}
	}
	return false
}

// principal is who the connection authenticated as: "User:<name>" after
// SASL, else "User:ANONYMOUS", as Kafka names an unauthenticated client.
func (c *conn) principal() string {
	if c.auth.user == "" {
		return "User:ANONYMOUS"
	}
	return "User:" + c.auth.user
}

// may reports whether the client on c may do op on the resource.
func (n *Node) may(c *conn, op, resType int8, name string) bool {
	return authorize(n.st.acls, n.cfg.Authorization, subject{c.principal(), c.host}, op, resType, name)
}

// describeACLs answers DescribeAcls from this node's applied ACLs, like
// Metadata: a follower may be a little behind (AclApis.handleDescribeAcls).
func (n *Node) describeACLs(c *conn, req *wire.DescribeAclsRequest) *wire.DescribeAclsResponse {
	resp := &wire.DescribeAclsResponse{}
	switch {
	case n.cfg.Authorization == nil:
		resp.ErrorCode, resp.ErrorMessage = wire.ErrSecurityDisabled, "No Authorizer is configured on the broker"
		return resp
	case !n.may(c, opDescribe, resCluster, clusterName):
		resp.ErrorCode, resp.ErrorMessage = wire.ErrClusterAuthorizationFailed, "needs DESCRIBE permission on the cluster"
		return resp
	case !validFilter(req.ACLFilter):
		resp.ErrorCode, resp.ErrorMessage = wire.ErrInvalidRequest, "invalid filter"
		return resp
	}
	for _, a := range n.st.acls { // in creation order: deterministic
		if !filterMatches(req.ACLFilter, a) {
			continue
		}
		i := slices.IndexFunc(resp.Resources, func(r wire.ACLResource) bool {
			return r.ResourceType == a.ResourceType && r.ResourceName == a.ResourceName && r.PatternType == a.PatternType
		})
		if i < 0 {
			resp.Resources = append(resp.Resources, wire.ACLResource{ResourceType: a.ResourceType, ResourceName: a.ResourceName, PatternType: a.PatternType})
			i = len(resp.Resources) - 1
		}
		resp.Resources[i].ACLs = append(resp.Resources[i].ACLs, wire.ACLEntry{Principal: a.Principal, Host: a.Host, Operation: a.Operation, Permission: a.Permission})
	}
	return resp
}

// aclChangeRefused is why a CreateAcls or DeleteAcls is refused as a whole,
// or ErrNone.
func (n *Node) aclChangeRefused(c *conn) (int16, string) {
	switch {
	case n.cfg.Authorization == nil:
		return wire.ErrSecurityDisabled, "No Authorizer is configured."
	case !n.may(c, opAlter, resCluster, clusterName):
		return wire.ErrClusterAuthorizationFailed, "needs ALTER permission on the cluster"
	}
	return wire.ErrNone, ""
}

// createACLs proposes one command per valid creation (AclApis.handleCreateAcls).
func (n *Node) createACLs(c *conn, h wire.RequestHeader, req *wire.CreateAclsRequest) {
	resp := &wire.CreateAclsResponse{Results: make([]wire.ACLResult, len(req.Creations))}
	add, done := collect(func() { n.respond(c, h, resp) })
	code, msg := n.aclChangeRefused(c)
	for i, a := range req.Creations {
		res := &resp.Results[i]
		why := validACL(a)
		switch {
		case code != wire.ErrNone:
			res.ErrorCode, res.ErrorMessage = code, msg
		case a.ResourceType == resCluster && a.ResourceName != clusterName:
			res.ErrorCode, res.ErrorMessage = wire.ErrInvalidRequest, "The only valid name for the CLUSTER resource is "+clusterName
		case why != "":
			res.ErrorCode, res.ErrorMessage = wire.ErrInvalidRequest, why
		default:
			add()
			n.propose(encodeCreateACL(a), func(r result) {
				res.ErrorCode = r.errCode
				done()
			})
		}
	}
	done()
}

// deleteACLs proposes one command per filter; each answers with the ACLs it
// removed (AclApis.handleDeleteAcls).
func (n *Node) deleteACLs(c *conn, h wire.RequestHeader, req *wire.DeleteAclsRequest) {
	resp := &wire.DeleteAclsResponse{Results: make([]wire.DeleteAclsResult, len(req.Filters))}
	add, done := collect(func() { n.respond(c, h, resp) })
	code, msg := n.aclChangeRefused(c)
	for i, f := range req.Filters {
		res := &resp.Results[i]
		switch {
		case code != wire.ErrNone:
			res.ErrorCode, res.ErrorMessage = code, msg
		case !validFilter(f):
			res.ErrorCode, res.ErrorMessage = wire.ErrInvalidRequest, "invalid filter"
		default:
			add()
			n.propose(encodeDeleteACLs(f), func(r result) {
				res.ErrorCode = r.errCode
				for _, a := range r.acls {
					res.Matching = append(res.Matching, wire.DeleteAclsMatch{ACL: a})
				}
				done()
			})
		}
	}
	done()
}
