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
	for _, want := range []string{"001_initial.sql", "002_join_tiers.sql", "003_usage_semantics.sql"} {
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
