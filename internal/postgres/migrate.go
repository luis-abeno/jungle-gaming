package postgres

import (
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"wallet/migrations"
)

func newMigrator(databaseURL string) (*migrate.Migrate, error) {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return nil, err
	}
	u := databaseURL
	for _, p := range []string{"postgres://", "postgresql://"} {
		if strings.HasPrefix(u, p) {
			u = "pgx5://" + strings.TrimPrefix(u, p)
		}
	}
	return migrate.NewWithSourceInstance("iofs", src, u)
}

func Migrate(databaseURL, cmd string, steps int) (uint, bool, error) {
	m, err := newMigrator(databaseURL)
	if err != nil {
		return 0, false, err
	}
	defer m.Close()

	switch cmd {
	case "up":
		err = m.Up()
	case "down":
		if steps < 0 {
			err = m.Down()
		} else {
			err = m.Steps(-max(steps, 1))
		}
	case "force":
		err = m.Force(steps)
	case "version":
	default:
		return 0, false, fmt.Errorf("unknown migrate command %q", cmd)
	}
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return 0, false, err
	}
	v, dirty, err := m.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		return 0, false, nil
	}
	return v, dirty, err
}
