//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"wallet/internal/config"
	"wallet/internal/messaging"
	"wallet/internal/observability"
	"wallet/internal/postgres"
	"wallet/internal/wiring"
)

var (
	pgHost      = env("IT_PG_HOST", "localhost:55432")
	pgOwner     = env("IT_PG_OWNER", "wallet:wallet")
	pgApp       = env("IT_PG_APP", "wallet_app:wallet_app")
	keycloakURL = env("IT_KEYCLOAK_URL", "http://localhost:8180")
	oidcIssuer  = env("IT_OIDC_ISSUER", "http://keycloak:8080/realms/wallet")
	sqsEndpoint = env("IT_SQS_ENDPOINT", "http://localhost:4566")

	runID        = fmt.Sprintf("%d", time.Now().Unix()%1_000_000)
	templateDB   = "wallet_it_template_" + runID
	binaryPath   string
	raceBuild    bool
	sqsClient    *sqs.Client
	adminPool    *pgxpool.Pool
	dbCounter    atomic.Int64
	queueCounter atomic.Int64
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func pgURL(user, db string) string {
	return fmt.Sprintf("postgres://%s@%s/%s?sslmode=disable", user, pgHost, db)
}

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	var err error
	adminPool, err = pgxpool.New(ctx, pgURL(pgOwner, "wallet"))
	if err == nil {
		err = adminPool.Ping(ctx)
	}
	if err != nil {
		fmt.Println("postgres not reachable, start it with docker compose up -d postgres keycloak ministack:", err)
		return 1
	}
	defer adminPool.Close()

	if _, err := adminPool.Exec(ctx, "CREATE DATABASE "+templateDB); err != nil {
		fmt.Println("create template:", err)
		return 1
	}
	defer adminPool.Exec(context.Background(), "DROP DATABASE IF EXISTS "+templateDB+" WITH (FORCE)")
	if _, _, err := postgres.Migrate(pgURL(pgOwner, templateDB), "up", 0); err != nil {
		fmt.Println("migrate template:", err)
		return 1
	}

	os.Setenv("AWS_ACCESS_KEY_ID", "wallet-tests")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "wallet-tests")
	sqsClient, err = messaging.NewClient(ctx, messaging.ClientConfig{Region: "us-east-1", Endpoint: sqsEndpoint})
	if err != nil {
		fmt.Println("sqs client:", err)
		return 1
	}

	dir, err := os.MkdirTemp("", "wallet-it")
	if err != nil {
		fmt.Println(err)
		return 1
	}
	defer os.RemoveAll(dir)
	binaryPath = filepath.Join(dir, "wallet")
	if runtime.GOOS == "windows" {
		binaryPath += ".exe"
	}
	args := []string{"build", "-o", binaryPath}
	if raceBuild {
		args = append(args, "-race")
	}
	build := exec.Command("go", append(args, "./cmd/wallet")...)
	build.Dir = "../.."
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Println("build binary:", err)
		return 1
	}
	return m.Run()
}

func newDatabase(t testing.TB) (string, string) {
	t.Helper()
	name := fmt.Sprintf("wallet_it_%s_%d", runID, dbCounter.Add(1))
	ctx := context.Background()
	var err error
	for i := 0; i < 50; i++ {
		_, err = adminPool.Exec(ctx, "CREATE DATABASE "+name+" TEMPLATE "+templateDB)
		if err == nil || !strings.Contains(err.Error(), "being accessed") {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		adminPool.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	return pgURL(pgApp, name), pgURL(pgOwner, name)
}

type queueSet struct {
	Names messaging.QueueNames
	URLs  messaging.Queues
}

func newQueues(t testing.TB, maxReceives int32, visibility int32) queueSet {
	t.Helper()
	prefix := fmt.Sprintf("it-%s-%d", runID, queueCounter.Add(1))
	names := messaging.QueueNames{Wager: prefix + "-wager.fifo", DLQ: prefix + "-wager-dlq.fifo", Events: prefix + "-events.fifo"}
	urls, err := messaging.Provision(context.Background(), sqsClient, messaging.ProvisionConfig{
		Names: names, Visibility: visibility, MaxReceives: maxReceives,
		Principals: &messaging.Principals{
			Producers: []string{"arn:aws:iam::000000000000:role/wager-gateway"}, Service: "arn:aws:iam::000000000000:role/wallet-service",
			EventReaders: []string{"arn:aws:iam::000000000000:role/reader"}, Operators: []string{"arn:aws:iam::000000000000:role/ops"},
		},
	})
	if err != nil {
		t.Fatalf("provision queues: %v", err)
	}
	t.Cleanup(func() {
		for _, u := range []string{urls.Wager, urls.DLQ, urls.Events} {
			sqsClient.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: aws.String(u)})
		}
	})
	return queueSet{Names: names, URLs: urls}
}

type testEnv struct {
	DBURL    string
	OwnerURL string
	Queues   queueSet
	Owner    *pgxpool.Pool
}

func newEnv(t testing.TB) *testEnv {
	t.Helper()
	appURL, ownerURL := newDatabase(t)
	owner, err := pgxpool.New(context.Background(), ownerURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(owner.Close)
	return &testEnv{DBURL: appURL, OwnerURL: ownerURL, Queues: newQueues(t, 3, 5), Owner: owner}
}

func freePort(t testing.TB) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func (e *testEnv) vars(port int) map[string]string {
	return map[string]string{
		"HTTP_ADDR":                 fmt.Sprintf("127.0.0.1:%d", port),
		"LOG_LEVEL":                 "warn",
		"DATABASE_URL":              e.DBURL,
		"OIDC_ISSUER":               oidcIssuer,
		"OIDC_JWKS_URL":             keycloakURL + "/realms/wallet/protocol/openid-connect/certs",
		"AWS_ENDPOINT_URL":          sqsEndpoint,
		"AWS_REGION":                "us-east-1",
		"AWS_ACCESS_KEY_ID":         "wallet-tests",
		"AWS_SECRET_ACCESS_KEY":     "wallet-tests",
		"SQS_WAGER_QUEUE":           e.Queues.Names.Wager,
		"SQS_WAGER_DLQ":             e.Queues.Names.DLQ,
		"SQS_EVENTS_QUEUE":          e.Queues.Names.Events,
		"SQS_WAIT_SECONDS":          "1",
		"SQS_VISIBILITY_TIMEOUT":    "5",
		"SQS_HANDLE_TIMEOUT":        "4s",
		"SQS_CONSUMER_WORKERS":      "2",
		"REQUEST_TIMEOUT":           "5s",
		"SHUTDOWN_TIMEOUT":          "10s",
		"OUTBOX_POLL_INTERVAL":      "100ms",
		"OUTBOX_LEASE":              "2s",
		"REFERENCE_POLL_INTERVAL":   "100ms",
		"REFERENCE_INITIAL_BACKOFF": "200ms",
		"REFERENCE_MAX_BACKOFF":     "1s",
		"REFERENCE_LEASE":           "2s",
		"REFERENCE_TTL":             "1m",
	}
}

func (e *testEnv) config(t testing.TB, port int, overrides map[string]string) config.Config {
	t.Helper()
	vars := e.vars(port)
	for k, v := range overrides {
		vars[k] = v
	}
	cfg, err := config.LoadFrom(func(k string) (string, bool) { v, ok := vars[k]; return v, ok })
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

type inProcess struct {
	URL     string
	Metrics *observability.Metrics
	app     *fx.App
	stopped bool
}

func (e *testEnv) start(t *testing.T, overrides map[string]string) *inProcess {
	t.Helper()
	port := freePort(t)
	cfg := e.config(t, port, overrides)
	var m *observability.Metrics
	a := fx.New(wiring.Options(cfg), fx.Populate(&m), fx.NopLogger)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := a.Start(ctx); err != nil {
		t.Fatalf("start app: %v", err)
	}
	p := &inProcess{URL: fmt.Sprintf("http://127.0.0.1:%d", port), Metrics: m, app: a}
	t.Cleanup(p.Stop)
	return p
}

func (p *inProcess) Stop() {
	if p.stopped {
		return
	}
	p.stopped = true
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p.app.Stop(ctx)
}

type process struct {
	URL  string
	cmd  *exec.Cmd
	logs *lockedBuffer
	done chan struct{}
	err  error
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func (e *testEnv) spawn(t *testing.T, overrides map[string]string) *process {
	t.Helper()
	p := e.spawnNoWait(t, overrides)
	if err := p.waitReady(60 * time.Second); err != nil {
		t.Fatalf("process not ready: %v\n%s", err, p.logs.String())
	}
	return p
}

func (e *testEnv) spawnNoWait(t *testing.T, overrides map[string]string) *process {
	t.Helper()
	port := freePort(t)
	vars := e.vars(port)
	vars["LOG_LEVEL"] = "info"
	for k, v := range overrides {
		vars[k] = v
	}
	cmd := exec.Command(binaryPath, "serve")
	cmd.Env = os.Environ()
	for k, v := range vars {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	p := &process{URL: fmt.Sprintf("http://127.0.0.1:%d", port), cmd: cmd, logs: &lockedBuffer{}, done: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = p.logs, p.logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.err = cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		p.Kill()
		if t.Failed() {
			t.Logf("logs of %s:\n%s", p.URL, tail(p.logs.String(), 60))
		}
	})
	return p
}

func tail(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

func (p *process) waitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-p.done:
			return fmt.Errorf("exited: %v", p.err)
		default:
		}
		resp, err := http.Get(p.URL + "/health/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout")
}

func (p *process) Kill() {
	select {
	case <-p.done:
		return
	default:
	}
	p.cmd.Process.Kill()
	<-p.done
}

func (p *process) ExitCode(timeout time.Duration) (int, bool) {
	select {
	case <-p.done:
		return p.cmd.ProcessState.ExitCode(), true
	case <-time.After(timeout):
		return 0, false
	}
}

var tokens sync.Map

type cachedToken struct {
	value   string
	expires time.Time
}

func token(t testing.TB, client string) string {
	t.Helper()
	if v, ok := tokens.Load(client); ok && time.Now().Before(v.(cachedToken).expires) {
		return v.(cachedToken).value
	}
	v, ttl := fetchToken(t, client)
	tokens.Store(client, cachedToken{v, time.Now().Add(ttl - 10*time.Second)})
	return v
}

func fetchToken(t testing.TB, client string) (string, time.Duration) {
	t.Helper()
	resp, err := http.PostForm(keycloakURL+"/realms/wallet/protocol/openid-connect/token", map[string][]string{
		"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {client + "-secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.AccessToken == "" {
		t.Fatalf("token for %s: %v %s", client, err, resp.Status)
	}
	return body.AccessToken, time.Duration(body.ExpiresIn) * time.Second
}

type response struct {
	Status int
	Body   map[string]any
	Raw    string
}

func (r response) str(path ...string) string {
	var cur any = r.Body
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = m[p]
	}
	s, _ := cur.(string)
	return s
}

var httpClient = &http.Client{Timeout: 30 * time.Second}

func call(t testing.TB, method, url, tok string, headers map[string]string, body any) response {
	t.Helper()
	r, err := tryCall(method, url, tok, headers, body)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return r
}

func tryCall(method, url, tok string, headers map[string]string, body any) (response, error) {
	var rd io.Reader
	if body != nil {
		switch b := body.(type) {
		case string:
			rd = strings.NewReader(b)
		default:
			raw, _ := json.Marshal(b)
			rd = bytes.NewReader(raw)
		}
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		return response{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return response{}, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := response{Status: resp.StatusCode, Raw: string(raw)}
	_ = json.Unmarshal(raw, &out.Body)
	return out, nil
}

func openWallet(t testing.TB, baseURL, amount string) (string, string) {
	t.Helper()
	player := uuid.NewString()
	r := call(t, "POST", baseURL+"/wallets", token(t, "wallet-internal"), nil, map[string]any{
		"playerId": player, "initialBalance": map[string]string{"amount": amount, "currency": "BRL"},
	})
	if r.Status != http.StatusCreated {
		t.Fatalf("open wallet: %d %s", r.Status, r.Raw)
	}
	return r.str("id"), player
}

type op struct {
	Provider string
	External string
	Key      string
	Player   string
	Wallet   string
	Round    string
	Kind     string
	Amount   string
	Currency string
	Ref      string
}

func bet(wallet, player, external, amount string) op {
	return op{Provider: "provider-a", External: external, Player: player, Wallet: wallet, Round: "round-1",
		Kind: "BET", Amount: amount}
}

func (o op) with(kind, external, amount, ref string) op {
	o.Kind, o.External, o.Amount, o.Ref, o.Key = kind, external, amount, ref, ""
	return o
}

func (o op) body() map[string]any {
	cur := o.Currency
	if cur == "" {
		cur = "BRL"
	}
	b := map[string]any{
		"providerId": o.Provider, "externalTransactionId": o.External, "playerId": o.Player, "walletId": o.Wallet,
		"roundId": o.Round, "gameId": "fortune-chimp", "kind": o.Kind,
		"money": map[string]string{"amount": o.Amount, "currency": cur},
	}
	if o.Ref != "" {
		b["referenceExternalTransactionId"] = o.Ref
	}
	return b
}

func (o op) key() string {
	if o.Key != "" {
		return o.Key
	}
	return o.Provider + ":" + o.External
}

func submit(t testing.TB, baseURL string, o op) response {
	t.Helper()
	return call(t, "POST", baseURL+"/wagering/transactions", token(t, o.Provider), map[string]string{"Idempotency-Key": o.key()}, o.body())
}

func (o op) message(messageID string) string {
	data := o.body()
	data["idempotencyKey"] = o.key()
	raw, _ := json.Marshal(map[string]any{
		"messageId": messageID, "type": "WagerTransactionRequested",
		"occurredAt": time.Now().UTC().Format(time.RFC3339Nano), "data": data,
	})
	return string(raw)
}

func sendMessage(t testing.TB, queueURL, body, group, dedup string) {
	t.Helper()
	_, err := sqsClient.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl: aws.String(queueURL), MessageBody: aws.String(body),
		MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(dedup),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func receiveAll(t testing.TB, queueURL string, wait time.Duration) []types.Message {
	t.Helper()
	var out []types.Message
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		r, err := sqsClient.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 1, VisibilityTimeout: 60,
			MessageAttributeNames:       []string{"All"},
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range r.Messages {
			sqsClient.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(queueURL), ReceiptHandle: m.ReceiptHandle})
		}
		out = append(out, r.Messages...)
		if len(r.Messages) == 0 && len(out) > 0 {
			break
		}
	}
	return out
}

func queueDepth(t testing.TB, queueURL string) int {
	t.Helper()
	out, err := sqsClient.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, v := range out.Attributes {
		var i int
		fmt.Sscan(v, &i)
		n += i
	}
	return n
}

func eventually(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func queryInt(t testing.TB, pool *pgxpool.Pool, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func (e *testEnv) ledgerEntries(t testing.TB, walletID string) int64 {
	return queryInt(t, e.Owner, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID)
}

func (e *testEnv) balance(t testing.TB, walletID string) int64 {
	return queryInt(t, e.Owner, `SELECT balance_minor FROM wallets WHERE id = $1`, walletID)
}

func (e *testEnv) assertConsistent(t testing.TB) {
	t.Helper()
	rows, err := e.Owner.Query(context.Background(), `
		SELECT w.id, w.balance_minor,
		       COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount_minor ELSE -l.amount_minor END), 0)::bigint,
		       w.version, COALESCE(MAX(l.wallet_version), 1)
		FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id = w.id
		GROUP BY w.id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	checked := 0
	for rows.Next() {
		var id uuid.UUID
		var stored, fromLedger, version, lastVersion int64
		if err := rows.Scan(&id, &stored, &fromLedger, &version, &lastVersion); err != nil {
			t.Fatal(err)
		}
		if stored != fromLedger || stored < 0 {
			t.Errorf("wallet %s: stored %d, ledger %d", id, stored, fromLedger)
		}
		if version != lastVersion {
			t.Errorf("wallet %s: version %d, last ledger version %d", id, version, lastVersion)
		}
		checked++
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	if checked == 0 {
		t.Error("no wallets checked")
	}
}

func txStatus(t testing.TB, pool *pgxpool.Pool, provider, external string) (status, failure string) {
	t.Helper()
	var f *string
	err := pool.QueryRow(context.Background(), `
		SELECT status, failure_code FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`,
		provider, external).Scan(&status, &f)
	if err == pgx.ErrNoRows {
		return "", ""
	}
	if err != nil {
		t.Fatal(err)
	}
	if f != nil {
		failure = *f
	}
	return status, failure
}

func parallel(n int, fn func(i int)) {
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			fn(i)
		}(i)
	}
	close(start)
	wg.Wait()
}
