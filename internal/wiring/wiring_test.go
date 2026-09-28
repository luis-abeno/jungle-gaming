package wiring

import (
	"testing"

	"go.uber.org/fx"

	"wallet/internal/config"
)

func TestGraph(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://u:p@localhost:1/db")
	t.Setenv("OIDC_ISSUER", "http://localhost:1/realms/wallet")
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if err := fx.ValidateApp(Options(cfg)); err != nil {
		t.Fatal(err)
	}
}
