package integration

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// On a three-process cluster with SASL and ACLs, through Kafka's own
// tools aimed at a follower: kafka-acls.sh changes ACLs (forwarded to the
// leader), kafka-configs.sh creates a user who then logs in on every node
// without a restart, describes her, and deletes her, which locks her out
// everywhere (docs/design/c8b-credentials-forwarding.md).
func TestCredentialsAndForwarding(t *testing.T) {
	bin, javaHome := kafkaTools(t)
	users := usersFile(t, "admin=admin-secret")
	c := startCluster(t, 3, "-sasl-mechanisms", "SCRAM-SHA-256", "-sasl-users", users, "-acl", "-super-users", "User:admin")
	admin := c.client(kgo.SASL(scram.Auth{User: "admin", Pass: "admin-secret"}.AsSha256Mechanism()))
	leader := c.leader(admin)
	follower := leader%3 + 1
	createTopic(t, admin, "orders", 1)

	props := filepath.Join(t.TempDir(), "admin.properties")
	if err := os.WriteFile(props, []byte("security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-256\n"+
		`sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="admin-secret";`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	run := func(tool string, args ...string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, filepath.Join(bin, tool), append(args, "--bootstrap-server", c.nodes[follower].addr, "--command-config", props)...)
		cmd.Env = append(os.Environ(), "JAVA_HOME="+javaHome)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s %s: %v\n%s", tool, strings.Join(args, " "), err, out)
		}
		return string(out)
	}

	// A CreateAcls sent to the follower itself is forwarded and succeeds.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req := kmsg.NewPtrCreateACLsRequest()
	req.Creations = append(req.Creations, allow("carol", kmsg.ACLResourceTypeTopic, "orders", kmsg.ACLOperationRead))
	resp, err := req.RequestWith(ctx, admin.Broker(int(follower)))
	if err != nil || kerr.ErrorForCode(resp.Results[0].ErrorCode) != nil {
		t.Fatalf("CreateAcls on follower %d: %v, %+v", follower, err, resp)
	}
	run("kafka-acls.sh", "--add", "--allow-principal", "User:carol", "--operation", "Describe", "--topic", "orders")
	if out := run("kafka-acls.sh", "--list", "--topic", "orders"); !strings.Contains(out, "operation=READ") || !strings.Contains(out, "operation=DESCRIBE") {
		t.Fatalf("kafka-acls --list:\n%s", out)
	}

	run("kafka-configs.sh", "--alter", "--add-config", "SCRAM-SHA-256=[iterations=4096,password=carol-secret]", "--entity-type", "users", "--entity-name", "carol")
	if out := run("kafka-configs.sh", "--describe", "--entity-type", "users", "--entity-name", "carol"); !strings.Contains(out, "SCRAM-SHA-256=iterations=4096") {
		t.Fatalf("kafka-configs --describe:\n%s", out)
	}
	// carol logs in on every node: the credential is replicated state.
	loginEverywhere := func(want bool) {
		t.Helper()
		for id, b := range c.nodes {
			ok := false
			for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
				carol, err := kgo.NewClient(kgo.SeedBrokers(b.addr), kgo.SASL(scram.Auth{User: "carol", Pass: "carol-secret"}.AsSha256Mechanism()), kgo.RequestRetries(0))
				if err != nil {
					t.Fatal(err)
				}
				rctx, rcancel := context.WithTimeout(context.Background(), 3*time.Second)
				md, err := kmsg.NewPtrMetadataRequest().RequestWith(rctx, carol)
				rcancel()
				carol.Close()
				if (err == nil) == want {
					ok = true
					if want && (len(md.Topics) != 1 || *md.Topics[0].Topic != "orders") {
						t.Fatalf("node %d: carol sees %+v, want only orders (her ACLs)", id, md.Topics)
					}
					break
				}
			}
			if !ok {
				t.Fatalf("node %d: carol logging in is %v, want %v", id, !want, want)
			}
		}
	}
	loginEverywhere(true)
	run("kafka-configs.sh", "--alter", "--delete-config", "SCRAM-SHA-256", "--entity-type", "users", "--entity-name", "carol")
	loginEverywhere(false)
	if out := run("kafka-configs.sh", "--describe", "--entity-type", "users"); strings.Contains(out, "carol") {
		t.Fatalf("carol still listed:\n%s", out)
	}
}
