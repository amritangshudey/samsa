package integration

import (
	"context"
	"errors"
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

// aclBroker runs a broker with SASL SCRAM for admin, alice and bob, ACLs
// on, admin a super user, and topic orders.
func aclBroker(t *testing.T) (b *broker, admin *kgo.Client) {
	users := usersFile(t, "admin=admin-secret", "alice=alice-secret", "bob=bob-secret")
	b = startBroker(t, "-sasl-mechanisms", "SCRAM-SHA-256", "-sasl-users", users, "-acl", "-super-users", "User:admin")
	admin = as(t, b, "admin")
	createTopic(t, admin, "orders", 1)
	return b, admin
}

func as(t *testing.T, b *broker, user string, opts ...kgo.Opt) *kgo.Client {
	return franz(t, b, append(opts, kgo.SASL(scram.Auth{User: user, Pass: user + "-secret"}.AsSha256Mechanism()))...)
}

func createACLs(t *testing.T, cl *kgo.Client, creations ...kmsg.CreateACLsRequestCreation) {
	t.Helper()
	req := kmsg.NewPtrCreateACLsRequest()
	req.Creations = creations
	resp, err := req.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range resp.Results {
		if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
			t.Fatalf("create ACL: %v", err)
		}
	}
}

func allow(user string, res kmsg.ACLResourceType, name string, op kmsg.ACLOperation) kmsg.CreateACLsRequestCreation {
	c := kmsg.NewCreateACLsRequestCreation()
	c.ResourceType, c.ResourceName, c.ResourcePatternType = res, name, kmsg.ACLResourcePatternTypeLiteral
	c.Principal, c.Host, c.Operation, c.PermissionType = "User:"+user, "*", op, kmsg.ACLPermissionTypeAllow
	return c
}

// franz-go as alice: refused without ACLs, allowed with exactly Kafka's,
// and refused again when they are deleted. Admin is a super user.
func TestACLsFranzGo(t *testing.T) {
	b, admin := aclBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	alice := as(t, b, "alice", kgo.DefaultProduceTopic("orders"), kgo.RecordRetries(1))
	err := alice.ProduceSync(ctx, kgo.StringRecord("x")).FirstErr()
	if !errors.Is(err, kerr.TopicAuthorizationFailed) {
		t.Fatalf("alice without ACLs: %v, want TOPIC_AUTHORIZATION_FAILED", err)
	}

	createACLs(t, admin,
		allow("alice", kmsg.ACLResourceTypeTopic, "orders", kmsg.ACLOperationWrite),
		allow("alice", kmsg.ACLResourceTypeTopic, "orders", kmsg.ACLOperationRead),
		allow("alice", kmsg.ACLResourceTypeGroup, "readers", kmsg.ACLOperationRead),
		allow("alice", kmsg.ACLResourceTypeCluster, "kafka-cluster", kmsg.ACLOperationIdempotentWrite))
	alice = as(t, b, "alice", kgo.DefaultProduceTopic("orders"))
	for _, v := range []string{"a", "b", "c"} {
		if err := alice.ProduceSync(ctx, kgo.StringRecord(v)).FirstErr(); err != nil {
			t.Fatalf("alice with ACLs, idempotent producer: %v", err)
		}
	}
	consumer := as(t, b, "alice", kgo.ConsumeTopics("orders"), kgo.ConsumerGroup("readers"), kgo.FetchMaxWait(500*time.Millisecond))
	var got []string
	for len(got) < 3 && ctx.Err() == nil {
		fs := consumer.PollFetches(ctx)
		if err := fs.Err0(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("consume: %v", err)
		}
		fs.EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	}
	if strings.Join(got, "") != "abc" {
		t.Fatalf("alice consumed %q", got)
	}
	if err := consumer.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("commit in group readers: %v", err)
	}
	consumer.Close()

	describe := kmsg.NewPtrDescribeACLsRequest()
	describe.ResourceType, describe.ResourcePatternType = kmsg.ACLResourceTypeAny, kmsg.ACLResourcePatternTypeAny
	describe.Operation, describe.PermissionType = kmsg.ACLOperationAny, kmsg.ACLPermissionTypeAny
	dr, err := describe.RequestWith(ctx, admin)
	if err != nil || dr.ErrorCode != 0 || len(dr.Resources) != 3 {
		t.Fatalf("describe: %v, %+v", err, dr)
	}
	if r, err := describe.RequestWith(ctx, alice); err != nil || r.ErrorCode != kerr.ClusterAuthorizationFailed.Code {
		t.Fatalf("alice describing ACLs: %v, code %d", err, r.ErrorCode)
	}

	del := kmsg.NewPtrDeleteACLsRequest()
	f := kmsg.NewDeleteACLsRequestFilter()
	principal := "User:alice"
	f.ResourceType, f.ResourcePatternType, f.Principal = kmsg.ACLResourceTypeAny, kmsg.ACLResourcePatternTypeAny, &principal
	f.Operation, f.PermissionType = kmsg.ACLOperationAny, kmsg.ACLPermissionTypeAny
	del.Filters = append(del.Filters, f)
	delr, err := del.RequestWith(ctx, admin)
	if err != nil || delr.Results[0].ErrorCode != 0 || len(delr.Results[0].MatchingACLs) != 4 {
		t.Fatalf("delete: %v, %+v", err, delr)
	}
	alice = as(t, b, "alice", kgo.DefaultProduceTopic("orders"), kgo.RecordRetries(1), kgo.DisableIdempotentWrite())
	if err := alice.ProduceSync(ctx, kgo.StringRecord("x")).FirstErr(); !errors.Is(err, kerr.TopicAuthorizationFailed) {
		t.Fatalf("alice after the ACLs were deleted: %v", err)
	}
}

// Kafka's own kafka-acls.sh adds, lists and removes ACLs; the console
// consumer as bob is refused, then allowed; kcat as bob is refused a
// produce he has no ACL for.
func TestACLsOtherClients(t *testing.T) {
	b, admin := aclBroker(t)
	if err := admin.ProduceSync(context.Background(), &kgo.Record{Topic: "orders", Value: []byte("hello")}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	t.Run("java", func(t *testing.T) {
		bin, javaHome := kafkaTools(t)
		props := func(user string) string {
			f := filepath.Join(t.TempDir(), user+".properties")
			text := "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-256\n" +
				`sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="` + user + `" password="` + user + "-secret\";\n"
			if err := os.WriteFile(f, []byte(text), 0o600); err != nil {
				t.Fatal(err)
			}
			return f
		}
		run := func(tool string, args ...string) (string, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, filepath.Join(bin, tool), append(args, "--bootstrap-server", b.addr)...)
			cmd.Env = append(os.Environ(), "JAVA_HOME="+javaHome)
			out, err := cmd.CombinedOutput()
			return string(out), err
		}
		consume := func() (string, error) {
			return run("kafka-console-consumer.sh", "--topic", "orders", "--group", "bobs", "--from-beginning",
				"--max-messages", "1", "--timeout-ms", "20000", "--command-config", props("bob"))
		}
		if out, _ := consume(); !strings.Contains(out, "AuthorizationException") {
			t.Fatalf("bob without ACLs was not refused:\n%s", out)
		}
		adminCfg := props("admin")
		if out, err := run("kafka-acls.sh", "--command-config", adminCfg, "--add", "--allow-principal", "User:bob",
			"--operation", "Read", "--topic", "orders", "--group", "bobs"); err != nil {
			t.Fatalf("kafka-acls --add: %v\n%s", err, out)
		}
		out, err := run("kafka-acls.sh", "--command-config", adminCfg, "--list", "--topic", "orders")
		if err != nil || !strings.Contains(out, "principal=User:bob") || !strings.Contains(out, "operation=READ") {
			t.Fatalf("kafka-acls --list: %v\n%s", err, out)
		}
		if out, err := consume(); err != nil || !strings.Contains(out, "hello") {
			t.Fatalf("bob with READ on orders and bobs: %v\n%s", err, out)
		}
		if out, err := run("kafka-acls.sh", "--command-config", adminCfg, "--remove", "--force", "--allow-principal", "User:bob",
			"--operation", "Read", "--topic", "orders", "--group", "bobs"); err != nil {
			t.Fatalf("kafka-acls --remove: %v\n%s", err, out)
		}
		if out, _ := run("kafka-acls.sh", "--command-config", adminCfg, "--list", "--topic", "orders"); strings.Contains(out, "User:bob") {
			t.Fatalf("still listed after --remove:\n%s", out)
		}
	})

	t.Run("kcat", func(t *testing.T) {
		kcat, err := exec.LookPath("kcat")
		if err != nil {
			missing(t, "kcat")
		}
		produce := exec.Command(kcat, "-b", b.addr, "-X", "security.protocol=SASL_PLAINTEXT", "-X", "sasl.mechanisms=SCRAM-SHA-256",
			"-X", "sasl.username=bob", "-X", "sasl.password=bob-secret", "-P", "-t", "orders", "-p", "0")
		produce.Stdin = strings.NewReader("x\n")
		out, err := produce.CombinedOutput()
		if err == nil || !strings.Contains(strings.ToLower(string(out)), "authoriz") {
			t.Fatalf("kcat produce as bob: %v\n%s", err, out)
		}
	})
}
