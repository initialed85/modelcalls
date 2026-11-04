package store_test

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/initialed85/modelcalls/internal/store"
)

// The embedded set is what a deployed pod applies to a real database on boot, so the set itself and
// its re-runnability are worth asserting on rather than trusting. A pod that cannot come up twice in a
// row is worse than a pod that cannot come up at all: the first start succeeds, the rollout finishes,
// and the crash appears later, on the node, at the worst time.
func TestMigrationsAreEmbeddedInApplyOrder(t *testing.T) {
	names, err := store.Migrations()
	if err != nil {
		t.Fatalf("Migrations: %v", err)
	}
	t.Logf("embedded migrations: %s", strings.Join(names, ", "))

	if !slices.IsSorted(names) {
		t.Errorf("apply order is %v, which is not name order: the upgrade path depends on 002 running after 001", names)
	}
	for _, want := range []string{"001_initial.sql", "002_join_tiers.sql", "003_usage_semantics.sql",
		"004_first_content.sql"} {
		if !slices.Contains(names, want) {
			t.Errorf("%s is not embedded, so a deployed pod would come up without it", want)
		}
	}
}

func TestMigrateLatestIsSafeToRunEveryBoot(t *testing.T) {
	dsn := os.Getenv("MODELCALLS_TEST_DSN")
	if dsn == "" {
		t.Skip("MODELCALLS_TEST_DSN is not set; this needs a real Postgres 16")
	}
	ctx := context.Background()
	st, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	// Three passes, because that is what a Deployment does: initial start, a rollout, and a restart
	// after the node reboots.
	for pass := 1; pass <= 3; pass++ {
		applied, err := st.MigrateLatest(ctx)
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if len(applied) == 0 {
			t.Fatalf("pass %d applied nothing", pass)
		}
	}

	// The columns 002 and 003 exist to add, checked where a deployed pod would notice their absence:
	// in the schema the pod then writes through.
	for _, want := range []struct{ table, column string }{
		{"call_events", "prompt_tokens_computed"},
		{"call_events", "cache_rate_undefined"},
		{"call_quality", "calls_joined_by_content"},
		{"call_quality", "calls_joined_by_timing_only"},
		{"call_quality", "calls_with_undefined_cache_rate"},
		{"call_events", "first_content_ms"},
		{"call_events", "first_content_gap_ms"},
		{"call_quality", "calls_with_first_content_gap"},
		{"call_quality", "mean_first_content_gap_ms"},
	} {
		var found bool
		if err := st.DB().QueryRowContext(ctx, `SELECT count(*) FROM information_schema.columns
			WHERE table_name = $1 AND column_name = $2`, want.table, want.column).Scan(&found); err != nil {
			t.Fatalf("look up %s.%s: %v", want.table, want.column, err)
		}
		if !found {
			t.Errorf("%s.%s is missing after three migration passes", want.table, want.column)
		}
	}

	// join_method is a CHECK constraint, and a CHECK left over from an earlier schema is the failure
	// 002 exists to fix. Two things are checked here rather than one: what the live database says it
	// allows, and whether a row can actually be written under each tier. The first is the constraint
	// text, the second is the only proof that a deployed pod can record a join at all - and getting
	// this list wrong is itself the point of writing it down.
	var constraint string
	if err := st.DB().QueryRowContext(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint
		WHERE conrelid = 'call_events'::regclass AND conname = 'call_events_join_method_check'`).Scan(&constraint); err != nil {
		t.Fatalf("read join_method constraint: %v", err)
	}
	t.Logf("live join_method constraint: %s", constraint)

	methods := []string{"self", "upstream_id", "usage_content_window", "backend_key_window"}
	for _, method := range methods {
		if !strings.Contains(constraint, "'"+method+"'") {
			t.Errorf("the live CHECK does not allow join_method %q, so this tier cannot be recorded: %s", method, constraint)
		}
	}
	for _, method := range methods {
		if _, err := st.Write(ctx, store.Event{
			EventKey: "rooter|migration-probe-" + method, Source: "rooter",
			SourceIDs: []string{"rooter|migration-probe-" + method},
			TS:        time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC),
			Backend:   str("sushi"), Key: str("probe"),
		}); err != nil {
			t.Fatalf("seed probe for %s: %v", method, err)
		}
		if _, err := st.DB().ExecContext(ctx,
			"UPDATE call_events SET join_method = $1 WHERE event_key = $2", method, "rooter|migration-probe-"+method); err != nil {
			t.Errorf("join_method %q was refused: %v", method, err)
		}
		if _, err := st.DB().ExecContext(ctx,
			"DELETE FROM call_events WHERE event_key = $1", "rooter|migration-probe-"+method); err != nil {
			t.Fatalf("remove probe for %s: %v", method, err)
		}
	}
}

// TestCallsViewOnlyGainsColumnsAtTheEnd is the invariant that makes an upgrade possible at all, and it
// is cheap to check on any database, fresh or five months old.
//
// `calls` is created with CREATE OR REPLACE VIEW, and Postgres will add columns to an existing view only
// at the end, in order; it refuses to change the name or position of a column the view already has. So
// every column added since the first schema shipped has to sit after everything that shipped with it,
// and the view's column order is historical rather than logical. That is the right trade: the order is
// cosmetic, and a migration that only works on an empty database is not a migration - the harvester
// migrates on boot, so the failure lands at process start, in production, on the row that has been
// running for a week.
//
// Measured twice, not predicted: first_content_ms in the middle of the list broke a store that had been
// running for a day, and engine_cache_hit_rate has been in position 28 since it was written, which is
// why a database created by the first shipped schema cannot be upgraded at all. test/legacy_upgrade_path.sql
// proves the upgrade end to end; this is the version that fails on the shape rather than on the process.
func TestCallsViewOnlyGainsColumnsAtTheEnd(t *testing.T) {
	dsn := os.Getenv("MODELCALLS_TEST_DSN")
	if dsn == "" {
		t.Skip("MODELCALLS_TEST_DSN is not set; this needs a real Postgres 16")
	}
	ctx := context.Background()
	st, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	if _, err := st.MigrateLatest(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	rows, err := st.DB().QueryContext(ctx, `SELECT column_name FROM information_schema.columns
		WHERE table_name = 'calls' ORDER BY ordinal_position`)
	if err != nil {
		t.Fatalf("read calls columns: %v", err)
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatalf("scan column name: %v", err)
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read columns: %v", err)
	}

	// The columns that were added after the first schema shipped, in the order they were added. A new
	// column that is not listed here is a new column whose position has not been thought about.
	added := []string{"engine_cache_hit_rate", "expert_cache_hit_rate", "first_content_ms", "first_content_gap_ms"}
	if len(cols) < len(added) {
		t.Fatalf("the calls view has %d columns, fewer than the %d that were added since the first schema", len(cols), len(added))
	}
	tail := cols[len(cols)-len(added):]
	if !slices.Equal(tail, added) {
		t.Errorf("the last %d columns of the calls view are %v, want %v - a column added since the first schema sits before existing ones, and CREATE OR REPLACE VIEW will refuse it on a database that already has the view",
			len(added), tail, added)
	}
	for _, c := range added {
		if slices.Index(cols[:len(cols)-len(added)], c) >= 0 {
			t.Errorf("%s appears twice in the calls view, which means the column list has diverged from the view itself", c)
		}
	}
	t.Logf("calls view: %d columns, appended in the order they were added: %v", len(cols), added)
}
