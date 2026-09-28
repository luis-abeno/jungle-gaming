package config

import (
	"strings"
	"testing"
	"time"
)

func from(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestDefaults(t *testing.T) {
	c, err := LoadFrom(from(map[string]string{
		"DATABASE_URL": "postgres://x", "OIDC_ISSUER": "http://idp/realms/wallet",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.OIDCJWKSURL != "http://idp/realms/wallet/protocol/openid-connect/certs" {
		t.Errorf("jwks url %s", c.OIDCJWKSURL)
	}
	if c.MigrationDatabaseURL != "postgres://x" || c.ReferenceTTL != 5*time.Minute || !c.ConsumerEnabled {
		t.Errorf("defaults: %+v", c)
	}
	if got := c.AllowedSenders["000000000000"]; len(got) != 1 || got[0] != "*" {
		t.Errorf("senders %v", c.AllowedSenders)
	}
}

func TestReportsAllProblems(t *testing.T) {
	_, err := LoadFrom(from(map[string]string{
		"SQS_WAGER_QUEUE":        "plain-queue",
		"OUTBOX_LEASE":           "soon",
		"SQS_CONSUMER_WORKERS":   "0",
		"SQS_VISIBILITY_TIMEOUT": "5",
		"SQS_HANDLE_TIMEOUT":     "10s",
		"SQS_ALLOWED_SENDERS":    "broken",
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"DATABASE_URL", "OIDC_ISSUER", "OUTBOX_LEASE", "FIFO", "SQS_CONSUMER_WORKERS", "SQS_HANDLE_TIMEOUT", "SQS_ALLOWED_SENDERS"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %s in %v", want, err)
		}
	}
}

func TestSenders(t *testing.T) {
	c, err := LoadFrom(from(map[string]string{
		"DATABASE_URL": "x", "OIDC_ISSUER": "y",
		"SQS_ALLOWED_SENDERS": "AIDA1=provider-a|provider-b, AIDA2=*",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.AllowedSenders["AIDA1"]) != 2 || c.AllowedSenders["AIDA2"][0] != "*" {
		t.Fatalf("%v", c.AllowedSenders)
	}
}
