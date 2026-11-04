package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

// The migrations are embedded, and that is a decision rather than a packaging detail.
//
// A schema applied by something other than the binary that depends on it can lag that binary, and the
// failure is quiet: a pod comes up against a database two migrations behind, every write still
// succeeds, and the missing facts simply never appear. `prompt_tokens_computed` is the exact case -
// a store without that column does not error, it just stops being able to say what llama.cpp's
// `prompt_n` disagrees with.
//
// So the binary carries the schema, applies it in name order on boot, and the schema a pod is writing
// against is by construction the schema its code was written for. The cost is that each migration must
// be safe to apply twice, which they are: 001 is all IF NOT EXISTS, and 002/003 are the additive
// upgrade path for a database that already exists. The contract lane proves re-runnability rather than
// assuming it.
//
//go:embed migrations/*.sql
var migrationFS embed.FS

// Migrations returns the embedded migration file names in apply order.
func Migrations() ([]string, error) {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	if len(names) == 0 {
		return nil, fmt.Errorf("no .sql migrations embedded")
	}
	return names, nil
}

// MigrateLatest applies every embedded migration, in name order, and returns the names it ran.
//
// It runs the whole set every time rather than tracking a schema_migrations table, because "which
// migrations have run" is state in the database that the deployer then has to reason about, and these
// migrations are idempotent by construction. A ledger would be one more thing that can disagree with
// the schema.
func (s *Store) MigrateLatest(ctx context.Context) ([]string, error) {
	names, err := Migrations()
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		sqlBytes, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return nil, fmt.Errorf("read embedded migration %s: %w", name, err)
		}
		if err := s.Migrate(ctx, sqlBytes); err != nil {
			return nil, fmt.Errorf("migration %s: %w", name, err)
		}
	}
	return names, nil
}
