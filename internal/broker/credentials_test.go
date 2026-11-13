package broker

import (
	"crypto/pbkdf2"
	"crypto/sha256"
	"testing"

	"github.com/amritangshudey/samsa/internal/scram"
	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// upsertion is what a client sends to set user's SCRAM-SHA-256 password.
func upsertion(user, password string, iterations int32) wire.CredentialUpsertion {
	salt := []byte("salt-of-" + user)
	salted, err := pbkdf2.Key(sha256.New, password, salt, int(iterations), sha256.Size)
	if err != nil {
		panic(err)
	}
	return wire.CredentialUpsertion{Name: user, Mechanism: 1, Iterations: iterations, Salt: salt, SaltedPassword: salted}
}

func (h *harness) alterCredentials(c sys.ConnID, req *wire.AlterUserScramCredentialsRequest) []wire.UserResult {
	h.t.Helper()
	var resp wire.AlterUserScramCredentialsResponse
	h.call(c, wire.KeyAlterUserScramCredentials, 0, req, &resp)
	return resp.Results
}

func (h *harness) describeCredentials(c sys.ConnID, users ...string) wire.DescribeUserScramCredentialsResponse {
	h.t.Helper()
	var resp wire.DescribeUserScramCredentialsResponse
	h.call(c, wire.KeyDescribeUserScramCredentials, 0, &wire.DescribeUserScramCredentialsRequest{Users: users}, &resp)
	return resp
}

// login opens connection c and logs in with SCRAM-SHA-256.
func (h *harness) login(c sys.ConnID, user, password string) bool {
	h.t.Helper()
	h.open(c)
	return h.scramLogin(c, scram.SHA256, user, password)
}

// A user created through the API logs in without a restart; a new
// password replaces the old; a deleted user is locked out; all of it
// survives a restart.
func TestCredentialsLifecycle(t *testing.T) {
	h := aclHarness(t, &Authorization{})
	if r := h.alterCredentials(admin, &wire.AlterUserScramCredentialsRequest{Upsertions: []wire.CredentialUpsertion{upsertion("carol", "one", 4096)}}); len(r) != 1 || r[0].ErrorCode != 0 {
		t.Fatalf("create carol: %+v", r)
	}
	if !h.login(10, "carol", "one") {
		t.Fatal("carol cannot log in")
	}
	d := h.describeCredentials(admin, "carol")
	if d.ErrorCode != 0 || len(d.Results) != 1 || len(d.Results[0].Infos) != 1 || d.Results[0].Infos[0] != (wire.CredentialInfo{Mechanism: 1, Iterations: 4096}) {
		t.Fatalf("describe carol: %+v", d)
	}
	if all := h.describeCredentials(admin); len(all.Results) != 3 { // admin and alice from the file, carol
		t.Fatalf("describe all: %+v", all)
	}
	h.alterCredentials(admin, &wire.AlterUserScramCredentialsRequest{Upsertions: []wire.CredentialUpsertion{upsertion("carol", "two", 8192)}})
	if h.login(11, "carol", "one") || !h.login(12, "carol", "two") {
		t.Fatal("the new password did not replace the old")
	}
	h.start() // from the log
	if !h.login(13, "carol", "two") {
		t.Fatal("carol lost in a restart")
	}
	h.open(admin)
	h.scramLogin(admin, scram.SHA256, "admin", "admin-secret")
	if r := h.alterCredentials(admin, &wire.AlterUserScramCredentialsRequest{Deletions: []wire.CredentialDeletion{{Name: "carol", Mechanism: 1}}}); r[0].ErrorCode != 0 {
		t.Fatalf("delete carol: %+v", r)
	}
	if h.login(14, "carol", "two") {
		t.Fatal("a deleted user logged in")
	}
	if d := h.describeCredentials(admin, "carol"); d.Results[0].ErrorCode != wire.ErrResourceNotFound {
		t.Fatalf("describe after delete: %+v", d)
	}
}

// Every refusal of ScramControlManager, and the file's names.
func TestCredentialsRefused(t *testing.T) {
	h := aclHarness(t, &Authorization{})
	h.alterCredentials(admin, &wire.AlterUserScramCredentialsRequest{Upsertions: []wire.CredentialUpsertion{upsertion("dave", "d", 4096)}})
	short := upsertion("erin", "e", 4096)
	short.SaltedPassword = short.SaltedPassword[:10]
	r := h.alterCredentials(admin, &wire.AlterUserScramCredentialsRequest{
		Deletions: []wire.CredentialDeletion{{Name: "nobody", Mechanism: 1}, {Name: "dave", Mechanism: 1}},
		Upsertions: []wire.CredentialUpsertion{
			upsertion("dave", "again", 4096), // named twice
			upsertion("frank", "f", 1000),    // too few iterations
			upsertion("gina", "g", 20000),    // too many
			{Name: "hank", Mechanism: 3},     // unknown mechanism
			upsertion("", "x", 4096),         // no name
			upsertion("alice", "new", 4096),  // owned by the file
			short,                            // not a SHA-256 salted password
			upsertion("ivy", "i", 4096),      // valid: applied
		}})
	want := map[string]int16{"nobody": wire.ErrResourceNotFound, "dave": wire.ErrDuplicateResource, "frank": wire.ErrUnacceptableCredential,
		"gina": wire.ErrUnacceptableCredential, "hank": wire.ErrUnsupportedSaslMechanism, "": wire.ErrUnacceptableCredential,
		"alice": wire.ErrUnacceptableCredential, "erin": wire.ErrUnacceptableCredential, "ivy": 0}
	if len(r) != len(want) {
		t.Fatalf("results %+v", r)
	}
	for _, res := range r {
		if code, ok := want[res.User]; !ok || code != res.ErrorCode {
			t.Errorf("%q: %d (%s), want %d", res.User, res.ErrorCode, res.ErrorMessage, code)
		}
	}
	if !h.login(10, "ivy", "i") || !h.login(11, "dave", "d") {
		t.Fatal("ivy was not created, or dave was changed despite the duplicate")
	}
	if d := h.describeCredentials(admin, "ivy", "ivy"); d.Results[0].ErrorCode != wire.ErrDuplicateResource || len(d.Results) != 1 {
		t.Fatalf("describe ivy twice: %+v", d)
	}
}

// ALTER and DESCRIBE on the cluster, as Kafka requires.
func TestCredentialsAllowedAndDenied(t *testing.T) {
	h := aclHarness(t, &Authorization{})
	alter := &wire.AlterUserScramCredentialsRequest{Upsertions: []wire.CredentialUpsertion{upsertion("carol", "c", 4096)}}
	if r := h.alterCredentials(user, alter); r[0].ErrorCode != wire.ErrClusterAuthorizationFailed {
		t.Fatalf("alice altering: %+v", r)
	}
	if d := h.describeCredentials(user); d.ErrorCode != wire.ErrClusterAuthorizationFailed {
		t.Fatalf("alice describing: %+v", d)
	}
	h.createACLs(admin, allowAlice(resCluster, clusterName, opAlter))
	if r := h.alterCredentials(user, alter); r[0].ErrorCode != 0 {
		t.Fatalf("alice with ALTER: %+v", r)
	}
	if d := h.describeCredentials(user); d.ErrorCode != 0 { // ALTER implies DESCRIBE
		t.Fatalf("alice with ALTER describing: %+v", d)
	}
}

// Credentials are commands in the log: a change on the leader holds on a
// follower, which needs no restart.
func TestCredentialsReplicate(t *testing.T) {
	tr := newTrio(t)
	tr.elect(1, 1, 2, 3)
	lead, follower := tr.h[1], tr.h[2]
	lead.open(1)
	corr := lead.send(1, wire.KeyAlterUserScramCredentials, 0, &wire.AlterUserScramCredentialsRequest{Upsertions: []wire.CredentialUpsertion{upsertion("carol", "c", 4096)}})
	tr.deliver(1, 2, 3)
	lead.must(lead.node.OnTimer(timerHeartbeat))
	tr.deliver(1, 2, 3)
	var resp wire.AlterUserScramCredentialsResponse
	if !lead.recv(1, wire.KeyAlterUserScramCredentials, 0, corr, &resp) || resp.Results[0].ErrorCode != 0 {
		t.Fatalf("alter on the leader: %+v", resp)
	}
	creds := follower.node.credentials("carol")
	if len(creds) != 1 || !creds[0].CheckPassword("c") {
		t.Fatalf("the follower has %+v", creds)
	}
	follower.open(1)
	var refused wire.AlterUserScramCredentialsResponse
	follower.call(1, wire.KeyAlterUserScramCredentials, 0, &wire.AlterUserScramCredentialsRequest{Upsertions: []wire.CredentialUpsertion{upsertion("carol", "x", 4096)}}, &refused)
	if refused.Results[0].ErrorCode != wire.ErrNotController {
		t.Fatalf("alter on a follower: %+v", refused)
	}
}

// DescribeClientQuotas: refused without DESCRIBE_CONFIGS on the cluster,
// and an empty list with it (Samsa has no quotas).
func TestDescribeQuotasAllowedAndDenied(t *testing.T) {
	h := aclHarness(t, &Authorization{})
	req := &wire.DescribeClientQuotasRequest{Components: []wire.QuotaComponent{{EntityType: "user", Match: "carol"}}}
	var resp wire.DescribeClientQuotasResponse
	h.call(user, wire.KeyDescribeClientQuotas, 1, req, &resp)
	if resp.ErrorCode != wire.ErrClusterAuthorizationFailed {
		t.Fatalf("alice without DESCRIBE_CONFIGS: %+v", resp)
	}
	h.createACLs(admin, allowAlice(resCluster, clusterName, opDescribeConfigs))
	resp = wire.DescribeClientQuotasResponse{}
	h.call(user, wire.KeyDescribeClientQuotas, 1, req, &resp)
	if resp.ErrorCode != 0 {
		t.Fatalf("alice with DESCRIBE_CONFIGS: %+v", resp)
	}
}
