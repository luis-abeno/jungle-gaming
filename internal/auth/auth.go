package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

const (
	RoleProvider = "wallet-provider"
	RoleInternal = "wallet-internal"
)

var ErrInvalidToken = errors.New("invalid token")

type Principal struct {
	Subject    string
	ClientID   string
	ProviderID string
	Roles      []string
}

func (p Principal) Has(role string) bool { return slices.Contains(p.Roles, role) }

type Verifier struct {
	verifier *oidc.IDTokenVerifier
	jwksURL  string
	client   *http.Client
}

func NewVerifier(issuer, jwksURL, audience string) *Verifier {
	client := &http.Client{Timeout: 5 * time.Second}
	ctx := oidc.ClientContext(context.Background(), client)
	keys := oidc.NewRemoteKeySet(ctx, jwksURL)
	return &Verifier{
		verifier: oidc.NewVerifier(issuer, keys, &oidc.Config{
			ClientID:             audience,
			SupportedSigningAlgs: []string{oidc.RS256},
		}),
		jwksURL: jwksURL,
		client:  client,
	}
}

type claims struct {
	Type        string   `json:"typ"`
	AZP         string   `json:"azp"`
	ProviderID  string   `json:"provider_id"`
	Roles       []string `json:"roles"`
	RealmAccess struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

func (v *Verifier) Verify(ctx context.Context, raw string) (Principal, error) {
	tok, err := v.verifier.Verify(ctx, raw)
	if err != nil {
		return Principal{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	var c claims
	if err := tok.Claims(&c); err != nil {
		return Principal{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if c.Type != "Bearer" {
		return Principal{}, fmt.Errorf("%w: not an access token", ErrInvalidToken)
	}
	roles := append(c.Roles, c.RealmAccess.Roles...)
	return Principal{Subject: tok.Subject, ClientID: c.AZP, ProviderID: c.ProviderID, Roles: roles}, nil
}

func (v *Verifier) Ready(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.jwksURL, nil)
	if err != nil {
		return err
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var body struct {
		Keys []json.RawMessage `json:"keys"`
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("jwks returned %s", resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return err
	}
	if len(body.Keys) == 0 {
		return errors.New("jwks has no keys")
	}
	return nil
}

type ctxKey struct{}

func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}
