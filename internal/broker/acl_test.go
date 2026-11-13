package broker

import (
	"testing"

	"github.com/amritangshudey/samsa/internal/wire"
)

func acl(res int8, name string, pat int8, principal, host string, op, perm int8) wire.ACL {
	return wire.ACL{ResourceType: res, ResourceName: name, PatternType: pat, Principal: principal, Host: host, Operation: op, Permission: perm}
}

var alice = subject{"User:alice", "10.0.0.5"}

// Each case is one rule of StandardAuthorizerData (docs/design/c8-acls.md).
func TestAuthorize(t *testing.T) {
	deny := &Authorization{}
	allowNoACL := &Authorization{AllowIfNoACL: true}
	for _, c := range []struct {
		name string
		cfg  *Authorization
		acls []wire.ACL
		who  subject
		op   int8
		res  int8
		on   string
		want bool
	}{
		{"authorization off", nil, nil, alice, opWrite, resTopic, "t", true},
		{"no ACL, default deny", deny, nil, alice, opWrite, resTopic, "t", false},
		{"no ACL, allow if none", allowNoACL, nil, alice, opWrite, resTopic, "t", true},
		{"ACLs exist for someone else: denied whatever the default", allowNoACL,
			[]wire.ACL{acl(resTopic, "t", patLiteral, "User:bob", "*", opWrite, permAllow)}, alice, opWrite, resTopic, "t", false},
		{"ACL on another topic leaves the default", allowNoACL,
			[]wire.ACL{acl(resTopic, "u", patLiteral, "User:bob", "*", opWrite, permAllow)}, alice, opWrite, resTopic, "t", true},
		{"super user", &Authorization{SuperUsers: []string{"User:alice"}},
			[]wire.ACL{acl(resTopic, "t", patLiteral, "User:alice", "*", opAll, permDeny)}, alice, opWrite, resTopic, "t", true},
		{"literal allow", deny, []wire.ACL{acl(resTopic, "t", patLiteral, "User:alice", "*", opWrite, permAllow)}, alice, opWrite, resTopic, "t", true},
		{"other operation", deny, []wire.ACL{acl(resTopic, "t", patLiteral, "User:alice", "*", opRead, permAllow)}, alice, opWrite, resTopic, "t", false},
		{"deny wins over allow", deny, []wire.ACL{
			acl(resTopic, "t", patLiteral, "User:alice", "*", opWrite, permAllow),
			acl(resTopic, "t", patPrefixed, "User:*", "*", opWrite, permDeny)}, alice, opWrite, resTopic, "t", false},
		{"ALL allows every operation", deny, []wire.ACL{acl(resTopic, "t", patLiteral, "User:alice", "*", opAll, permAllow)}, alice, opDelete, resTopic, "t", true},
		{"READ implies DESCRIBE", deny, []wire.ACL{acl(resTopic, "t", patLiteral, "User:alice", "*", opRead, permAllow)}, alice, opDescribe, resTopic, "t", true},
		{"CREATE does not imply DESCRIBE", deny, []wire.ACL{acl(resTopic, "t", patLiteral, "User:alice", "*", opCreate, permAllow)}, alice, opDescribe, resTopic, "t", false},
		{"DENY READ does not deny DESCRIBE", deny, []wire.ACL{
			acl(resTopic, "t", patLiteral, "User:alice", "*", opDescribe, permAllow),
			acl(resTopic, "t", patLiteral, "User:alice", "*", opRead, permDeny)}, alice, opDescribe, resTopic, "t", true},
		{"ALTER_CONFIGS implies DESCRIBE_CONFIGS", deny, []wire.ACL{acl(resTopic, "t", patLiteral, "User:alice", "*", opAlterConfigs, permAllow)}, alice, opDescribeConfigs, resTopic, "t", true},
		{"User:* matches anyone", deny, []wire.ACL{acl(resTopic, "t", patLiteral, "User:*", "*", opWrite, permAllow)}, alice, opWrite, resTopic, "t", true},
		{"host must match", deny, []wire.ACL{acl(resTopic, "t", patLiteral, "User:alice", "10.0.0.6", opWrite, permAllow)}, alice, opWrite, resTopic, "t", false},
		{"exact host", deny, []wire.ACL{acl(resTopic, "t", patLiteral, "User:alice", "10.0.0.5", opWrite, permAllow)}, alice, opWrite, resTopic, "t", true},
		{"CIDR host", deny, []wire.ACL{acl(resTopic, "t", patLiteral, "User:alice", "10.0.0.0/24", opWrite, permAllow)}, alice, opWrite, resTopic, "t", true},
		{"CIDR outside", deny, []wire.ACL{acl(resTopic, "t", patLiteral, "User:alice", "10.0.1.0/24", opWrite, permAllow)}, alice, opWrite, resTopic, "t", false},
		{"literal * is every name", deny, []wire.ACL{acl(resTopic, "*", patLiteral, "User:alice", "*", opWrite, permAllow)}, alice, opWrite, resTopic, "t", true},
		{"prefixed", deny, []wire.ACL{acl(resTopic, "orders-", patPrefixed, "User:alice", "*", opWrite, permAllow)}, alice, opWrite, resTopic, "orders-eu", true},
		{"literal is not a prefix", deny, []wire.ACL{acl(resTopic, "orders-", patLiteral, "User:alice", "*", opWrite, permAllow)}, alice, opWrite, resTopic, "orders-eu", false},
		{"resource type must match", deny, []wire.ACL{acl(resGroup, "t", patLiteral, "User:alice", "*", opRead, permAllow)}, alice, opRead, resTopic, "t", false},
	} {
		if got := authorize(c.acls, c.cfg, c.who, c.op, c.res, c.on); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

func TestAuthorizeAny(t *testing.T) {
	deny := &Authorization{}
	for _, c := range []struct {
		name string
		acls []wire.ACL
		want bool
	}{
		{"nothing", nil, false},
		{"one literal allow", []wire.ACL{acl(resTopic, "t", patLiteral, "User:alice", "*", opWrite, permAllow)}, true},
		{"literal allow, same literal deny", []wire.ACL{
			acl(resTopic, "t", patLiteral, "User:alice", "*", opWrite, permAllow),
			acl(resTopic, "t", patLiteral, "User:alice", "*", opWrite, permDeny)}, false},
		{"prefix allow dominated by a shorter prefix deny", []wire.ACL{
			acl(resTopic, "ab", patPrefixed, "User:alice", "*", opWrite, permAllow),
			acl(resTopic, "a", patPrefixed, "User:alice", "*", opWrite, permDeny)}, false},
		{"wildcard allow, wildcard deny later", []wire.ACL{
			acl(resTopic, "*", patLiteral, "User:alice", "*", opWrite, permAllow),
			acl(resTopic, "*", patLiteral, "User:*", "*", opAll, permDeny)}, false},
		{"another principal's allow", []wire.ACL{acl(resTopic, "t", patLiteral, "User:bob", "*", opWrite, permAllow)}, false},
	} {
		if got := authorizeAny(c.acls, deny, alice, opWrite, resTopic); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
	if !authorizeAny(nil, &Authorization{AllowIfNoACL: true}, alice, opWrite, resTopic) {
		t.Error("no ACLs with AllowIfNoACL must allow")
	}
}

func TestFilterMatches(t *testing.T) {
	str := func(s string) *string { return &s }
	lit := acl(resTopic, "orders", patLiteral, "User:alice", "*", opRead, permAllow)
	pre := acl(resTopic, "ord", patPrefixed, "User:alice", "*", opRead, permAllow)
	star := acl(resTopic, "*", patLiteral, "User:alice", "*", opRead, permAllow)
	any := wire.ACLFilter{ResourceType: resAny, PatternType: patAny, Operation: opAny, Permission: permAny}
	f := func(mod func(*wire.ACLFilter)) wire.ACLFilter { g := any; mod(&g); return g }
	for _, c := range []struct {
		name string
		f    wire.ACLFilter
		a    wire.ACL
		want bool
	}{
		{"any matches", any, lit, true},
		{"type", f(func(g *wire.ACLFilter) { g.ResourceType = resGroup }), lit, false},
		{"name exact under ANY", f(func(g *wire.ACLFilter) { g.ResourceName = str("orders") }), lit, true},
		{"name under ANY is not a prefix match", f(func(g *wire.ACLFilter) { g.ResourceName = str("orders") }), pre, false},
		{"MATCH finds the prefix ACL", f(func(g *wire.ACLFilter) { g.PatternType = patMatch; g.ResourceName = str("orders") }), pre, true},
		{"MATCH finds the wildcard", f(func(g *wire.ACLFilter) { g.PatternType = patMatch; g.ResourceName = str("orders") }), star, true},
		{"LITERAL pattern skips PREFIXED", f(func(g *wire.ACLFilter) { g.PatternType = patLiteral }), pre, false},
		{"principal", f(func(g *wire.ACLFilter) { g.Principal = str("User:bob") }), lit, false},
		{"host", f(func(g *wire.ACLFilter) { g.Host = str("*") }), lit, true},
		{"operation", f(func(g *wire.ACLFilter) { g.Operation = opWrite }), lit, false},
		{"permission", f(func(g *wire.ACLFilter) { g.Permission = permDeny }), lit, false},
	} {
		if got := filterMatches(c.f, c.a); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

func TestValidACL(t *testing.T) {
	good := acl(resTopic, "t", patLiteral, "User:alice", "*", opRead, permAllow)
	if why := validACL(good); why != "" {
		t.Fatalf("valid ACL refused: %s", why)
	}
	for name, mod := range map[string]func(*wire.ACL){
		"resource ANY":   func(a *wire.ACL) { a.ResourceType = resAny },
		"pattern MATCH":  func(a *wire.ACL) { a.PatternType = patMatch },
		"operation ANY":  func(a *wire.ACL) { a.Operation = opAny },
		"permission ANY": func(a *wire.ACL) { a.Permission = permAny },
		"empty name":     func(a *wire.ACL) { a.ResourceName = "" },
		"no colon":       func(a *wire.ACL) { a.Principal = "alice" },
		"empty host":     func(a *wire.ACL) { a.Host = "" },
		"bad CIDR":       func(a *wire.ACL) { a.Host = "10.0.0.0/99" },
	} {
		a := good
		mod(&a)
		if validACL(a) == "" {
			t.Errorf("%s: accepted", name)
		}
	}
}
