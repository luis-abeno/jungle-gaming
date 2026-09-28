package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr        string
	LogLevel        string
	ShutdownTimeout time.Duration
	RequestTimeout  time.Duration

	DatabaseURL          string
	MigrationDatabaseURL string
	DBMaxConns           int32
	DBLockTimeout        time.Duration
	DBStatementTimeout   time.Duration

	OIDCIssuer   string
	OIDCJWKSURL  string
	OIDCAudience string

	AWSRegion   string
	AWSEndpoint string

	WagerQueue        string
	WagerDLQ          string
	EventsQueue       string
	ConsumerName      string
	ConsumerWorkers   int
	ReceiveWait       int32
	VisibilityTimeout int32
	MaxReceives       int32
	HandleTimeout     time.Duration
	AllowedSenders    map[string][]string

	ConsumerEnabled   bool
	OutboxEnabled     bool
	ReferencesEnabled bool

	OutboxPollInterval time.Duration
	OutboxBatch        int
	OutboxLease        time.Duration
	OutboxMaxBackoff   time.Duration

	ReferencePollInterval   time.Duration
	ReferenceBatch          int
	ReferenceLease          time.Duration
	ReferenceTTL            time.Duration
	ReferenceMaxAttempts    int
	ReferenceInitialBackoff time.Duration
	ReferenceMaxBackoff     time.Duration

	FaultPoints []string
}

func Load() (Config, error) { return LoadFrom(os.LookupEnv) }

func LoadFrom(lookup func(string) (string, bool)) (Config, error) {
	r := reader{lookup: lookup}
	c := Config{
		HTTPAddr:        r.str("HTTP_ADDR", ":8080"),
		LogLevel:        r.str("LOG_LEVEL", "info"),
		ShutdownTimeout: r.dur("SHUTDOWN_TIMEOUT", 25*time.Second),
		RequestTimeout:  r.dur("REQUEST_TIMEOUT", 10*time.Second),

		DatabaseURL:          r.required("DATABASE_URL"),
		MigrationDatabaseURL: r.str("MIGRATION_DATABASE_URL", ""),
		DBMaxConns:           int32(r.integer("DB_MAX_CONNS", 20)),
		DBLockTimeout:        r.dur("DB_LOCK_TIMEOUT", 5*time.Second),
		DBStatementTimeout:   r.dur("DB_STATEMENT_TIMEOUT", 15*time.Second),

		OIDCIssuer:   r.required("OIDC_ISSUER"),
		OIDCJWKSURL:  r.str("OIDC_JWKS_URL", ""),
		OIDCAudience: r.str("OIDC_AUDIENCE", "wallet-api"),

		AWSRegion:   r.str("AWS_REGION", "us-east-1"),
		AWSEndpoint: r.str("AWS_ENDPOINT_URL", ""),

		WagerQueue:        r.str("SQS_WAGER_QUEUE", "wager-transactions.fifo"),
		WagerDLQ:          r.str("SQS_WAGER_DLQ", "wager-transactions-dlq.fifo"),
		EventsQueue:       r.str("SQS_EVENTS_QUEUE", "wallet-events.fifo"),
		ConsumerName:      r.str("SQS_CONSUMER_NAME", "wallet-wager-consumer"),
		ConsumerWorkers:   r.integer("SQS_CONSUMER_WORKERS", 4),
		ReceiveWait:       int32(r.integer("SQS_WAIT_SECONDS", 10)),
		VisibilityTimeout: int32(r.integer("SQS_VISIBILITY_TIMEOUT", 30)),
		MaxReceives:       int32(r.integer("SQS_MAX_RECEIVES", 5)),
		HandleTimeout:     r.dur("SQS_HANDLE_TIMEOUT", 20*time.Second),
		AllowedSenders:    r.senders("SQS_ALLOWED_SENDERS", "000000000000=*"),

		ConsumerEnabled:   r.boolean("CONSUMER_ENABLED", true),
		OutboxEnabled:     r.boolean("OUTBOX_ENABLED", true),
		ReferencesEnabled: r.boolean("REFERENCES_ENABLED", true),

		OutboxPollInterval: r.dur("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
		OutboxBatch:        r.integer("OUTBOX_BATCH", 100),
		OutboxLease:        r.dur("OUTBOX_LEASE", 30*time.Second),
		OutboxMaxBackoff:   r.dur("OUTBOX_MAX_BACKOFF", time.Minute),

		ReferencePollInterval:   r.dur("REFERENCE_POLL_INTERVAL", 500*time.Millisecond),
		ReferenceBatch:          r.integer("REFERENCE_BATCH", 20),
		ReferenceLease:          r.dur("REFERENCE_LEASE", 30*time.Second),
		ReferenceTTL:            r.dur("REFERENCE_TTL", 5*time.Minute),
		ReferenceMaxAttempts:    r.integer("REFERENCE_MAX_ATTEMPTS", 20),
		ReferenceInitialBackoff: r.dur("REFERENCE_INITIAL_BACKOFF", time.Second),
		ReferenceMaxBackoff:     r.dur("REFERENCE_MAX_BACKOFF", 30*time.Second),

		FaultPoints: r.list("FAULT_POINTS"),
	}
	if c.OIDCJWKSURL == "" && c.OIDCIssuer != "" {
		c.OIDCJWKSURL = strings.TrimRight(c.OIDCIssuer, "/") + "/protocol/openid-connect/certs"
	}
	if c.MigrationDatabaseURL == "" {
		c.MigrationDatabaseURL = c.DatabaseURL
	}
	r.check(c.ConsumerWorkers > 0, "SQS_CONSUMER_WORKERS must be positive")
	r.check(c.ReceiveWait >= 0 && c.ReceiveWait <= 20, "SQS_WAIT_SECONDS must be between 0 and 20")
	r.check(c.VisibilityTimeout > 0, "SQS_VISIBILITY_TIMEOUT must be positive")
	r.check(c.HandleTimeout < time.Duration(c.VisibilityTimeout)*time.Second,
		"SQS_HANDLE_TIMEOUT must be shorter than SQS_VISIBILITY_TIMEOUT")
	r.check(c.MaxReceives > 0, "SQS_MAX_RECEIVES must be positive")
	r.check(c.OutboxBatch > 0 && c.ReferenceBatch > 0, "batch sizes must be positive")
	r.check(c.ReferenceMaxAttempts > 0, "REFERENCE_MAX_ATTEMPTS must be positive")
	r.check(c.ReferenceInitialBackoff > 0 && c.ReferenceMaxBackoff >= c.ReferenceInitialBackoff,
		"reference backoff must be positive and REFERENCE_MAX_BACKOFF >= REFERENCE_INITIAL_BACKOFF")
	r.check(c.ReferenceTTL > c.ReferenceInitialBackoff, "REFERENCE_TTL must exceed the first backoff")
	r.check(strings.HasSuffix(c.WagerQueue, ".fifo") && strings.HasSuffix(c.WagerDLQ, ".fifo") &&
		strings.HasSuffix(c.EventsQueue, ".fifo"), "queues must be FIFO (.fifo suffix)")
	r.check(c.DBMaxConns > 0, "DB_MAX_CONNS must be positive")
	return c, r.err()
}

type reader struct {
	lookup   func(string) (string, bool)
	problems []string
}

func (r *reader) get(key string) string {
	v, _ := r.lookup(key)
	return v
}

func (r *reader) check(ok bool, msg string) {
	if !ok {
		r.problems = append(r.problems, msg)
	}
}

func (r *reader) err() error {
	if len(r.problems) == 0 {
		return nil
	}
	return errors.New("invalid configuration: " + strings.Join(r.problems, "; "))
}

func (r *reader) str(key, def string) string {
	if v := r.get(key); v != "" {
		return v
	}
	return def
}

func (r *reader) required(key string) string {
	v := r.get(key)
	if v == "" {
		r.problems = append(r.problems, key+" is required")
	}
	return v
}

func (r *reader) integer(key string, def int) int {
	v := r.get(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		r.problems = append(r.problems, fmt.Sprintf("%s: %q is not an integer", key, v))
	}
	return n
}

func (r *reader) dur(key string, def time.Duration) time.Duration {
	v := r.get(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		r.problems = append(r.problems, fmt.Sprintf("%s: %q is not a positive duration", key, v))
	}
	return d
}

func (r *reader) boolean(key string, def bool) bool {
	v := r.get(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		r.problems = append(r.problems, fmt.Sprintf("%s: %q is not a boolean", key, v))
	}
	return b
}

func (r *reader) list(key string) []string {
	var out []string
	for _, p := range strings.Split(r.get(key), ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (r *reader) senders(key, def string) map[string][]string {
	out := map[string][]string{}
	for _, item := range strings.Split(r.str(key, def), ",") {
		sender, providers, ok := strings.Cut(strings.TrimSpace(item), "=")
		if !ok || sender == "" || providers == "" {
			r.problems = append(r.problems, fmt.Sprintf("%s: invalid entry %q", key, item))
			continue
		}
		out[sender] = strings.Split(providers, "|")
	}
	return out
}
