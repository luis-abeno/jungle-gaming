//go:build integration

package integration

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func selfSigned(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	enc := func(v any) string {
		b, _ := json.Marshal(v)
		return base64.RawURLEncoding.EncodeToString(b)
	}
	header := enc(map[string]string{"alg": "RS256", "typ": "JWT", "kid": "forged"})
	payload := enc(map[string]any{
		"iss": oidcIssuer, "aud": "wallet-api", "azp": "provider-a", "typ": "Bearer",
		"provider_id": "provider-a", "roles": []string{"wallet-provider", "wallet-internal"},
		"exp": time.Now().Add(time.Hour).Unix(), "iat": time.Now().Unix(), "sub": uuid.NewString(),
	})
	sum := sha256.Sum256([]byte(header + "." + payload))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	return header + "." + payload + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func tamper(tok string) string {
	parts := strings.Split(tok, ".")
	raw, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var claims map[string]any
	json.Unmarshal(raw, &claims)
	claims["provider_id"] = "provider-b"
	b, _ := json.Marshal(claims)
	parts[1] = base64.RawURLEncoding.EncodeToString(b)
	return strings.Join(parts, ".")
}

func TestAuthenticationIsRequired(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, nil)
	walletID, playerID := openWallet(t, app.URL, "100.00")
	o := bet(walletID, playerID, "auth-bet", "10.00")

	shortLived, _ := fetchToken(t, "provider-a-short-lived")
	time.Sleep(3 * time.Second)

	cases := map[string]string{
		"missing":        "",
		"garbage":        "not-a-jwt",
		"forged key":     selfSigned(t),
		"tampered":       tamper(token(t, "provider-a")),
		"wrong audience": token(t, "other-api"),
		"expired":        shortLived,
	}
	for name, tok := range cases {
		r := call(t, "POST", app.URL+"/wagering/transactions", tok, map[string]string{"Idempotency-Key": o.key()}, o.body())
		if r.Status != http.StatusUnauthorized {
			t.Errorf("%s: got %d %s", name, r.Status, r.Raw)
		}
		r = call(t, "GET", app.URL+"/wallets/"+walletID, tok, nil, nil)
		if r.Status != http.StatusUnauthorized {
			t.Errorf("%s wallet read: got %d", name, r.Status)
		}
	}

	if n := queryInt(t, e.Owner, `SELECT count(*) FROM wager_transactions WHERE origin = 'EXTERNAL'`); n != 0 {
		t.Fatalf("unauthenticated calls created %d transactions", n)
	}
	if e.balance(t, walletID) != 10000 || e.ledgerEntries(t, walletID) != 1 {
		t.Fatal("unauthenticated calls changed the wallet")
	}

	if r := submit(t, app.URL, o); r.Status != http.StatusOK {
		t.Fatalf("valid token: %d %s", r.Status, r.Raw)
	}
}

func TestRoleRestrictions(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, nil)
	walletID, playerID := openWallet(t, app.URL, "100.00")

	provider := token(t, "provider-a")
	internal := token(t, "wallet-internal")
	noRole := token(t, "reporting")

	for _, c := range []struct {
		method, path, tok string
		body              any
	}{
		{"POST", "/wallets", provider, map[string]any{"playerId": uuid.NewString(), "initialBalance": map[string]string{"amount": "1.00", "currency": "BRL"}}},
		{"GET", "/wallets/" + walletID, provider, nil},
		{"GET", "/wallets/" + walletID + "/ledger", provider, nil},
		{"POST", "/wallets/" + walletID + "/reconciliation", provider, nil},
		{"POST", "/wagering/transactions", internal, bet(walletID, playerID, "role-bet", "1.00").body()},
		{"GET", "/wallets/" + walletID, noRole, nil},
		{"GET", "/wagering/transactions/" + uuid.NewString(), noRole, nil},
	} {
		r := call(t, c.method, app.URL+c.path, c.tok, map[string]string{"Idempotency-Key": "k-" + uuid.NewString()}, c.body)
		if r.Status != http.StatusForbidden {
			t.Errorf("%s %s: got %d %s", c.method, c.path, r.Status, r.Raw)
		}
	}
	if n := queryInt(t, e.Owner, `SELECT count(*) FROM wallets`); n != 1 {
		t.Fatalf("forbidden call opened a wallet: %d", n)
	}
	if n := queryInt(t, e.Owner, `SELECT count(*) FROM wager_transactions WHERE origin = 'EXTERNAL'`); n != 0 {
		t.Fatal("forbidden call created a transaction")
	}
}

func TestProviderIsolation(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, nil)
	walletID, playerID := openWallet(t, app.URL, "100.00")

	a := bet(walletID, playerID, "iso-bet", "10.00")
	first := submit(t, app.URL, a)
	if first.Status != http.StatusOK {
		t.Fatalf("provider-a bet: %d %s", first.Status, first.Raw)
	}
	txID := first.str("transactionId")
	b := token(t, "provider-b")

	r := call(t, "POST", app.URL+"/wagering/transactions", b, map[string]string{"Idempotency-Key": a.key()}, a.body())
	if r.Status != http.StatusForbidden || strings.Contains(r.Raw, txID) {
		t.Fatalf("replay as other provider: %d %s", r.Status, r.Raw)
	}
	if r := call(t, "GET", app.URL+"/wagering/transactions/"+txID, b, nil, nil); r.Status != http.StatusNotFound {
		t.Fatalf("read by id: %d %s", r.Status, r.Raw)
	}
	if r := call(t, "GET", app.URL+"/providers/provider-a/wagering/transactions/iso-bet", b, nil, nil); r.Status != http.StatusForbidden {
		t.Fatalf("read by external id: %d %s", r.Status, r.Raw)
	}
	own := a
	own.Provider = "provider-b"
	r = submit(t, app.URL, own)
	if r.Status != http.StatusOK || r.str("transactionId") == txID {
		t.Fatalf("provider-b own op: %d %s", r.Status, r.Raw)
	}
	if r := call(t, "GET", app.URL+"/providers/provider-b/wagering/transactions/iso-bet", b, nil, nil); r.str("providerId") != "provider-b" {
		t.Fatalf("provider-b read: %s", r.Raw)
	}

	if r := call(t, "GET", app.URL+"/wagering/transactions/"+txID, token(t, "provider-a"), nil, nil); r.Status != http.StatusOK {
		t.Fatalf("owner read: %d", r.Status)
	}
	if r := call(t, "GET", app.URL+"/providers/provider-a/wagering/transactions/iso-bet", token(t, "wallet-internal"), nil, nil); r.Status != http.StatusOK {
		t.Fatalf("internal read: %d", r.Status)
	}
	if e.balance(t, walletID) != 10000-1000-1000 {
		t.Fatalf("balance: %d", e.balance(t, walletID))
	}
	e.assertConsistent(t)
}
