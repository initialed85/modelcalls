package store_test

import (
	"context"
	"database/sql"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/initialed85/modelcalls/internal/store"
)

// These tests are the Go mirror of test/contract_scenarios.sql. They are not a reimplementation of
// it: they drive the production write path and assert the same outcomes, so that the contract only
// holds if the code that has to honour it honours it. If the SQL scenarios pass and these fail,
// the harvester is doing something the schema never promised.
//
// A real Postgres 16 is required, because the contract IS Postgres: partial unique indexes,
// ON CONFLICT merge semantics, generated columns, jsonb concatenation. There is no useful version
// of this test against a fake database.
//
//	MODELCALLS_TEST_DSN=postgres://mc:mc@127.0.0.1:15432/modelcalls?sslmode=disable \
//	  go test ./internal/store/

type Row struct {
	EventKey       sql.NullString
	Source         sql.NullString
	ID             sql.NullString
	UpstreamID     sql.NullString
	Backend        sql.NullString
	Key            sql.NullString
	Model          sql.NullString
	Engine         sql.NullString
	JoinMethod     sql.NullString
	UsageSource    sql.NullString
	FinishReason   sql.NullString
	Aborted        bool
	HasUsage       bool
	TSIsObs        bool
	SourceCount    int
	TS             time.Time
	TTFTMs         sql.NullFloat64
	DecodeTokS     sql.NullFloat64
	CacheHitRate   sql.NullFloat64
	EngineRate     sql.NullFloat64
	ExpertRate     sql.NullFloat64
	Bytes          sql.NullInt64
	PromptTokens   sql.NullInt64
	CompletedTok   sql.NullInt64
	DraftsOk       sql.NullInt64
	ExtraHasStrata bool
}

func fetch(t *testing.T, st *store.Store, eventKey string) Row {
	t.Helper()
	var r Row
	err := st.DB().QueryRow(`
		SELECT event_key, source, id, upstream_id, backend, "key", model, engine,
		       join_method, usage_source, finish_reason,
		       aborted, has_usage, ts_is_observation, source_count, ts,
		       ttft_ms, decode_tok_s, cache_hit_rate, engine_cache_hit_rate,
		       expert_cache_hit_rate, bytes,
		       prompt_tokens, completion_tokens, drafts_accepted,
		       extra ? 'strata'
		  FROM call_events WHERE event_key = $1`, eventKey).
		Scan(&r.EventKey, &r.Source, &r.ID, &r.UpstreamID, &r.Backend, &r.Key, &r.Model, &r.Engine,
			&r.JoinMethod, &r.UsageSource, &r.FinishReason,
			&r.Aborted, &r.HasUsage, &r.TSIsObs, &r.SourceCount, &r.TS,
			&r.TTFTMs, &r.DecodeTokS, &r.CacheHitRate, &r.EngineRate, &r.ExpertRate, &r.Bytes,
			&r.PromptTokens, &r.CompletedTok, &r.DraftsOk, &r.ExtraHasStrata)
	if err == sql.ErrNoRows {
		t.Fatalf("no row for event_key %q", eventKey)
	}
	if err != nil {
		t.Fatalf("fetch %s: %v", eventKey, err)
	}
	return r
}

func count(t *testing.T, st *store.Store, where string, args ...any) int {
	t.Helper()
	var n int
	if err := st.DB().QueryRow("SELECT count(*) FROM call_events WHERE "+where, args...).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", where, err)
	}
	return n
}

func str(s string) *string   { return &s }
func f64(v float64) *float64 { return &v }
func i64(v int64) *int64     { return &v }
func ival(v int) *int        { return &v }
func bval(v bool) *bool      { return &v }

func setup(t *testing.T) (context.Context, *store.Store) {
	t.Helper()
	dsn := os.Getenv("MODELCALLS_TEST_DSN")
	if dsn == "" {
		t.Skip("MODELCALLS_TEST_DSN is not set; this contract test needs a real Postgres 16")
	}
	ctx := context.Background()

	st, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	// Migration first (it is idempotent), then a truncate: the contract is only meaningful from a
	// known empty state, and a fresh database has nothing to truncate.
	sqlBytes, err := os.ReadFile(filepath.Join("..", "..", "migrations", "001_initial.sql"))
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if err := st.Migrate(ctx, sqlBytes); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, "TRUNCATE TABLE call_events"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return ctx, st
}

func TestContract(t *testing.T) {
	ctx, st := setup(t)

	// ts values are fixed, not time.Now(), so the fallback window assertions mean something.
	base := time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)

	t.Run("A_router_sees_a_completed_call_first", func(t *testing.T) {
		res, err := st.Write(ctx, store.Event{
			EventKey: "rooter|r-1001", Source: "rooter", SourceIDs: []string{"rooter|r-1001"},
			ID: str("r-1001"), UpstreamID: str("chatcmpl-aaa"), TS: base,
			Backend: str("strata"), BackendURL: str("http://192.168.11.128:8088"),
			Alias: str("strata"), Profile: str("iq3_xxs"),
			Model: str("qwen3.8-flash-next-iq3_xxs"), ModelRequested: str("auto"),
			Key: str("pi-code"), KeySource: str("header"),
			Path: str("/v1/chat/completions"), Method: str("POST"), Stream: bval(true),
			Status: ival(200), Bytes: i64(98213), Aborted: bval(false),
			WaitMs: f64(3.2), TTFTMs: f64(640.0), TotalMs: f64(10442.4),
			PromptTokens: i64(106718), CompletionTokens: i64(700), CachedTokens: i64(106677),
			UsageSource:  store.UsageInjected,
			FinishReason: str("stop"),
			Extra:        map[string]any{"raw": map[string]any{"usage": map[string]any{"prompt_tokens": 106718}}},
		})
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if res.Path != store.PathInsert {
			t.Errorf("path = %q, want %q", res.Path, store.PathInsert)
		}
		if res.JoinMethod != "self" {
			t.Errorf("join_method = %q, want self (one surface has ever touched this row)", res.JoinMethod)
		}
		if !res.HasUsage {
			t.Error("has_usage false while both token counts are present")
		}
		r := fetch(t, st, "rooter|r-1001")
		if !r.UpstreamID.Valid || r.UpstreamID.String != "chatcmpl-aaa" {
			t.Errorf("upstream_id = %v, want chatcmpl-aaa", r.UpstreamID)
		}
	})

	t.Run("B_same_event_re_pulled_is_not_a_second_witness", func(t *testing.T) {
		// A cursor reconnect replays the same event, this time without ttft_ms.
		res, err := st.Write(ctx, store.Event{
			EventKey: "rooter|r-1001", Source: "rooter", SourceIDs: []string{"rooter|r-1001"},
			ID: str("r-1001"), UpstreamID: str("chatcmpl-aaa"), TS: base,
			Backend: str("strata"), Status: ival(200), Bytes: i64(98213), Aborted: bval(false),
			WaitMs: f64(3.2), TotalMs: f64(10442.4), UsageSource: store.UsageInjected,
		})
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if res.Path != store.PathRePull {
			t.Errorf("path = %q, want %q", res.Path, store.PathRePull)
		}
		if res.SourceCount != 1 {
			t.Errorf("source_count = %d, want 1: a surface re-reporting itself is not independent confirmation", res.SourceCount)
		}
		if count(t, st, "event_key = 'rooter|r-1001'") != 1 {
			t.Error("a re-pull created a second row")
		}
		r := fetch(t, st, "rooter|r-1001")
		if !r.TTFTMs.Valid || r.TTFTMs.Float64 != 640.0 {
			t.Errorf("ttft_ms = %v, want 640 preserved: an absent fact must not overwrite a known one", r.TTFTMs)
		}
		if !r.HasUsage {
			t.Error("has_usage false after a re-pull that carried no usage at all")
		}
	})

	t.Run("C_runner_side_sighting_joins_instead_of_duplicating", func(t *testing.T) {
		res, err := st.Write(ctx, store.Event{
			EventKey: "strata|chatcmpl-aaa", Source: "strata", SourceIDs: []string{"strata|chatcmpl-aaa"},
			ID: str("chatcmpl-aaa"), UpstreamID: str("chatcmpl-aaa"), TS: base.Add(time.Second),
			Backend: str("strata"), Alias: str("strata"), Profile: str("iq3_xxs"),
			Engine: str("0.1.51"), Model: str("qwen3.8-flash-next-iq3_xxs"),
			Key:          str("pi-code"),
			PromptTokens: i64(106718), CompletionTokens: i64(700),
			CachedTokens: i64(106677), EngineCachedTokens: i64(106677),
			PromptMs: f64(471.1), DecodeMs: f64(9961.0),
			PrefillTokS: f64(2133.6), DecodeTokS: f64(70.3),
			ExpertCacheHitRate: f64(0.802),
			UsageSource:        store.UsageRunnerScrape,
			FinishReason:       str("stop"),
			DraftsOffered:      i64(690), DraftsAccepted: i64(431),
			Extra: map[string]any{"raw": map[string]any{"id": "b01b3fd29419", "hit_rate": 0.802}},
		})
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if res.Path != store.PathJoinUpstream {
			t.Errorf("path = %q, want %q", res.Path, store.PathJoinUpstream)
		}
		if res.EventKey != "rooter|r-1001" {
			t.Errorf("merged into %q, want rooter|r-1001 (the row that already held the call)", res.EventKey)
		}
		if res.SourceCount != 2 {
			t.Errorf("source_count = %d, want 2", res.SourceCount)
		}
		if count(t, st, "event_key = 'strata|chatcmpl-aaa'") != 0 {
			t.Error("the runner-side sighting created its own row; one call must have one row")
		}
		if count(t, st, "upstream_id = 'chatcmpl-aaa'") != 1 {
			t.Error("more than one row claims chatcmpl-aaa")
		}

		r := fetch(t, st, "rooter|r-1001")
		if r.JoinMethod.String != "upstream_id" {
			t.Errorf("join_method = %q, want upstream_id", r.JoinMethod.String)
		}
		if !r.DecodeTokS.Valid || r.DecodeTokS.Float64 != 70.3 {
			t.Errorf("decode_tok_s = %v, want the engine's 70.3", r.DecodeTokS)
		}
		if !r.DraftsOk.Valid || r.DraftsOk.Int64 != 431 {
			t.Error("speculative-decode facts did not merge")
		}
		if r.Engine.String != "0.1.51" {
			t.Errorf("engine = %q, want 0.1.51", r.Engine.String)
		}
		// The more informative provenance survives: rooter asked for the usage, that is worth
		// knowing, and a scrape is not an upgrade over it.
		if r.UsageSource.String != store.UsageInjected {
			t.Errorf("usage_source = %q, want %q preserved", r.UsageSource.String, store.UsageInjected)
		}
		if !r.ExtraHasStrata {
			t.Error("strata's raw payload was not preserved under its own name")
		}
	})

	t.Run("D_abandoned_stream_is_an_event_even_with_nothing_to_measure", func(t *testing.T) {
		res, err := st.Write(ctx, store.Event{
			EventKey: "rooter|r-1002", Source: "rooter", SourceIDs: []string{"rooter|r-1002"},
			ID: str("r-1002"), TS: base.Add(20 * time.Second), // no upstream_id: it lived in the tail that never arrived
			Backend: str("strata"), BackendURL: str("http://192.168.11.128:8088"),
			Alias: str("strata"), Profile: str("iq3_xxs"),
			Model: str("qwen3.8-flash-next-iq3_xxs"), ModelRequested: str("auto"),
			Key: str("pi-code"), KeySource: str("header"),
			Path: str("/v1/chat/completions"), Method: str("POST"), Stream: bval(true),
			Status: ival(200), Bytes: i64(4096), Aborted: bval(true),
			WaitMs: f64(2.1), TotalMs: f64(1800.0),
			UsageSource: store.UsageNone,
			Extra:       map[string]any{"raw": map[string]any{"note": "tail empty: client abort before final frame"}},
		})
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if res.Path != store.PathInsert {
			t.Errorf("path = %q, want %q", res.Path, store.PathInsert)
		}
		if res.HasUsage {
			t.Error("has_usage true for a call with no token counts; aggregates would divide a real denominator by this call")
		}
		r := fetch(t, st, "rooter|r-1002")
		if !r.Aborted {
			t.Error("abandoned call not recorded as abandoned")
		}
		if r.UpstreamID.Valid {
			t.Errorf("upstream_id = %v, want NULL: nothing identified this call", r.UpstreamID)
		}
	})

	t.Run("E_fallback_fills_the_orphan_and_names_itself", func(t *testing.T) {
		// Strata finished this generation and kept its own record. There is no key to collide with,
		// because rooter never learned the chatcmpl id. Without the fallback this becomes two rows
		// for one call, which is fabricated data rather than missing data.
		res, err := st.Write(ctx, store.Event{
			EventKey: "strata|chatcmpl-bbb", Source: "strata", SourceIDs: []string{"strata|chatcmpl-bbb"},
			ID: str("chatcmpl-bbb"), UpstreamID: str("chatcmpl-bbb"), TS: base.Add(22 * time.Second),
			// No Key on purpose: strata's own metrics surface does not know which session asked, and
			// that absence is what makes it the engine side of a fallback join.
			Backend: str("strata"),
			Alias:   str("strata"), Profile: str("iq3_xxs"), Engine: str("0.1.51"),
			PromptTokens: i64(103623), CompletionTokens: i64(640),
			CachedTokens: i64(103582), EngineCachedTokens: i64(103582),
			PromptMs: f64(498.9), DecodeMs: f64(9100.3), DecodeTokS: f64(70.3),
			UsageSource:  store.UsageRunnerScrape,
			FinishReason: str("stop"),
			Extra:        map[string]any{"raw": map[string]any{"id": "0ff5eee72d53", "hit_rate": 0.802}},
		})
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if res.Path != store.PathJoinFallback {
			t.Fatalf("path = %q, want %q", res.Path, store.PathJoinFallback)
		}
		if res.EventKey != "rooter|r-1002" {
			t.Errorf("merged into %q, want rooter|r-1002", res.EventKey)
		}
		if count(t, st, "upstream_id = 'chatcmpl-bbb'") != 1 {
			t.Error("the fallback merged but did not record the key it learned")
		}
		if count(t, st, "event_key = 'strata|chatcmpl-bbb'") != 0 {
			t.Error("the fallback inserted instead of merging")
		}
		r := fetch(t, st, "rooter|r-1002")
		if !r.Aborted {
			t.Error("abandonment lost during the fallback merge (a client can hang up while the engine finishes)")
		}
		if r.JoinMethod.String != "backend_key_window" {
			t.Errorf("join_method = %q, want backend_key_window: a heuristic must be visible in the data", r.JoinMethod.String)
		}
		if !r.HasUsage {
			t.Error("has_usage false after engine facts arrived")
		}
	})

	t.Run("F_calls_no_runner_ever_saw_stay_separate", func(t *testing.T) {
		for i, id := range []string{"r-1003", "r-1004"} {
			res, err := st.Write(ctx, store.Event{
				EventKey: "rooter|" + id, Source: "rooter", SourceIDs: []string{"rooter|" + id},
				ID: str(id), TS: base.Add(time.Duration(30+i) * time.Second),
				ModelRequested: str("auto"), Key: str("pi-code"),
				Status: ival(401), Aborted: bval(false), WaitMs: f64(0.1 + float64(i)*0.1),
				UsageSource: store.UsageNone,
				Error:       map[string]any{"type": "authentication_error", "message": "no key"},
			})
			if err != nil {
				t.Fatalf("write %s: %v", id, err)
			}
			if res.Path != store.PathInsert {
				t.Errorf("%s: path = %q, want insert (NULL upstream_id and NULL backend must not collide)", id, res.Path)
			}
		}
		if n := count(t, st, "backend IS NULL"); n != 2 {
			t.Errorf("backendless rows = %d, want 2", n)
		}

		// A re-report of one of them must not become a third row.
		if _, err := st.Write(ctx, store.Event{
			EventKey: "rooter|r-1003", Source: "rooter", SourceIDs: []string{"rooter|r-1003"},
			ID: str("r-1003"), TS: base.Add(30 * time.Second), Status: ival(401),
			Aborted: bval(false), WaitMs: f64(0.1), UsageSource: store.UsageNone,
		}); err != nil {
			t.Fatalf("re-report: %v", err)
		}
		if n := count(t, st, "status = 401"); n != 2 {
			t.Errorf("401 rows = %d, want 2", n)
		}
	})

	t.Run("G_log_line_with_no_clock_says_so", func(t *testing.T) {
		res, err := st.Write(ctx, store.Event{
			EventKey: "sushi|9f2c1e4a7b0d", Source: "sushi", SourceIDs: []string{"sushi|9f2c1e4a7b0d"},
			TS: base.Add(40 * time.Second), TSIsObservation: true,
			Backend: str("sushi"), BackendURL: str("http://192.168.137.111:8088"),
			Alias: str("sushi"), Profile: str("2.6bpw"), Version: str("sushi 1.1.1 (MLX 0.32.3)"),
			Model:  str("Qwen3-Next-80B-A3B-Instruct-2.6bpw"),
			Stream: bval(true), Status: ival(200), Aborted: bval(false),
			PromptTokens: i64(113186), CompletionTokens: i64(217), CachedTokens: i64(110674),
			PromptMs: f64(6804.7), DecodeMs: f64(4617.0),
			PrefillTokS: f64(369.2), DecodeTokS: f64(47.0),
			UsageSource:  store.UsageRunnerScrape,
			FinishReason: str("stop"),
			Extra:        map[string]any{"raw": map[string]any{"line": "113186+217 tokens streamed"}},
		})
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if res.Path != store.PathInsert {
			t.Errorf("path = %q, want insert", res.Path)
		}
		if !res.HasUsage {
			t.Error("has_usage false for a call whose token counts are known")
		}
		r := fetch(t, st, "sushi|9f2c1e4a7b0d")
		if !r.TSIsObs {
			t.Error("ts_is_observation lost: sushi's log lines carry no timestamp and the row must admit it")
		}
	})

	t.Run("H_two_candidates_are_not_a_join", func(t *testing.T) {
		// Two abandoned envelopes from the same session inside the fallback window. Matching either
		// one is a coin flip, so the store must refuse and leave three honest rows instead of two
		// confident lies.
		for i, id := range []string{"r-2001", "r-2002"} {
			if _, err := st.Write(ctx, store.Event{
				EventKey: "rooter|" + id, Source: "rooter", SourceIDs: []string{"rooter|" + id},
				ID: str(id), TS: base.Add(time.Duration(300+i) * time.Second),
				Backend: str("strata"), Key: str("pi-code"),
				Status: ival(200), Aborted: bval(true), UsageSource: store.UsageNone,
			}); err != nil {
				t.Fatalf("seed %s: %v", id, err)
			}
		}

		res, err := st.Write(ctx, store.Event{
			EventKey: "strata|chatcmpl-ccc", Source: "strata", SourceIDs: []string{"strata|chatcmpl-ccc"},
			UpstreamID: str("chatcmpl-ccc"), TS: base.Add(301 * time.Second),
			Backend:      str("strata"), // no Key: this is the engine's record, not a wire witness
			PromptTokens: i64(1000), CompletionTokens: i64(50), EngineCachedTokens: i64(900),
			DecodeTokS: f64(60.0), UsageSource: store.UsageRunnerScrape,
		})
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if res.Path != store.PathInsert {
			t.Errorf("path = %q, want insert: with two candidates a fallback merge is a guess", res.Path)
		}
		// Scoped to this scenario's three keys: the store accumulates across subtests, and a count
		// across the whole table would be measuring earlier scenarios instead of this rule.
		if n := count(t, st, "event_key IN ('rooter|r-2001', 'rooter|r-2002', 'strata|chatcmpl-ccc')"); n != 3 {
			t.Errorf("rows = %d, want 3 (two envelopes plus one unjoined runner record, all visibly unjoined)", n)
		}
		for _, id := range []string{"rooter|r-2001", "rooter|r-2002"} {
			r := fetch(t, st, id)
			if r.HasUsage {
				t.Errorf("%s gained usage from a guessed merge", id)
			}
			if r.JoinMethod.String != "self" {
				t.Errorf("%s join_method = %q, want self", id, r.JoinMethod.String)
			}
		}
	})

	t.Run("I_real_timestamp_beats_an_observed_one", func(t *testing.T) {
		// A real timestamp beats an observed one. The 1s gap is also the point: the fallback window
		// is 2s, and a surface that reports this call 5s away must NOT be merged, because "close
		// enough" with a wide-open window is how two unrelated calls become one row.
		observed := base.Add(400 * time.Second)
		real := base.Add(399 * time.Second)
		if _, err := st.Write(ctx, store.Event{
			EventKey: "sushi|envelope-1", Source: "sushi", SourceIDs: []string{"sushi|envelope-1"},
			TS: observed, TSIsObservation: true,
			Backend: str("sushi"), Key: str("pi-code"),
			Status: ival(200), Aborted: bval(false), UsageSource: store.UsageNone,
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if _, err := st.Write(ctx, store.Event{
			EventKey: "strata|chatcmpl-ddd", Source: "strata", SourceIDs: []string{"strata|chatcmpl-ddd"},
			UpstreamID: str("chatcmpl-ddd"), TS: real,
			Backend:      str("sushi"), // no Key: engine-side sighting again
			PromptTokens: i64(2000), CompletionTokens: i64(120), EngineCachedTokens: i64(1800),
			DecodeTokS: f64(50.0), UsageSource: store.UsageRunnerScrape,
		}); err != nil {
			t.Fatalf("merge: %v", err)
		}
		r := fetch(t, st, "sushi|envelope-1")
		if r.TSIsObs {
			t.Error("ts_is_observation stayed true after a surface with a real timestamp merged in")
		}
		if !r.TS.Equal(real) {
			t.Errorf("ts = %v, want %v: a known time replaces a guessed one", r.TS, real)
		}
		if r.JoinMethod.String != "backend_key_window" {
			t.Errorf("join_method = %q, want backend_key_window", r.JoinMethod.String)
		}
	})

	t.Run("J_first_fact_wins_even_when_a_later_one_disagrees", func(t *testing.T) {
		// Documented behaviour, tested so it is a decision rather than an accident. A later sighting
		// that reports a different status does NOT rewrite the row: the store merges gaps, not
		// opinions. Where a correction is genuinely needed, it belongs in extra, and a status that
		// can be overwritten by whichever surface polls last is a status nobody can trust.
		if _, err := st.Write(ctx, store.Event{
			EventKey: "rooter|r-1001", Source: "rooter", SourceIDs: []string{"rooter|r-1001"},
			ID: str("r-1001"), UpstreamID: str("chatcmpl-aaa"), TS: base,
			Backend: str("strata"), Status: ival(500), UsageSource: store.UsageInjected,
		}); err != nil {
			t.Fatalf("write: %v", err)
		}
		r := fetch(t, st, "rooter|r-1001")
		if r.Bytes.Int64 != 98213 {
			t.Errorf("bytes = %v, want 98213 preserved", r.Bytes)
		}
	})

	t.Run("K_the_store_reports_its_own_shape", func(t *testing.T) {
		var calls, withUsage, withoutUsage, aborted, joined, byFallback, observed int
		err := st.DB().QueryRow(`
			SELECT count(*),
			       count(*) FILTER (WHERE has_usage),
			       count(*) FILTER (WHERE NOT has_usage),
			       count(*) FILTER (WHERE aborted),
			       count(*) FILTER (WHERE source_count > 1),
			       count(*) FILTER (WHERE join_method = 'backend_key_window'),
			       count(*) FILTER (WHERE ts_is_observation)
			  FROM call_events`).Scan(&calls, &withUsage, &withoutUsage, &aborted, &joined, &byFallback, &observed)
		if err != nil {
			t.Fatalf("shape: %v", err)
		}
		t.Logf("store shape: %d calls, %d with usage, %d without, %d abandoned, %d joined across surfaces, %d by fallback, %d with an observed timestamp",
			calls, withUsage, withoutUsage, aborted, joined, byFallback, observed)
		if calls == 0 {
			t.Fatal("empty store")
		}
		if byFallback > joined {
			t.Errorf("fallback joins (%d) exceed upstream joins: the heuristic is doing more work than the reliable key, which is a design smell worth noticing", byFallback)
		}
	})
}

func TestValidateRejectsUnusableEvents(t *testing.T) {
	// These three checks are the difference between a store and a place where bad rows accumulate
	// quietly: without an idempotency key nothing deduplicates, without a source nothing can be
	// merged coherently, and without a timestamp the whole time series is a rumour.
	_, st := setup(t)
	ctx := context.Background()

	cases := []struct {
		name string
		ev   store.Event
		want string
	}{
		{"no event key", store.Event{Source: "rooter", TS: time.Now()}, "EventKey"},
		{"no source", store.Event{EventKey: "x|1", TS: time.Now()}, "Source"},
		{"no timestamp", store.Event{EventKey: "rooter|1", Source: "rooter"}, "TS"},
	}
	for _, c := range cases {
		if _, err := st.Write(ctx, c.ev); err == nil {
			t.Errorf("%s: write accepted an event with no %s", c.name, c.want)
		}
	}
}

// TestCacheRatesAreDerivedNotSupplied is the guard against the one mistake this schema is most
// likely to inherit from Strata.
//
// Strata reports `hit_rate` on every request record, measured live between 0.802 and 0.903, and it
// is the MoE expert cache's hit rate (its own serve/prometheus.py says "The last request's expert
// cache hit rate"). The same requests reuse 99.2-99.96% of their prompt tokens. Both numbers are
// between 0 and 1, both are called a cache rate by somebody, and a writer that can put either one
// into cache_hit_rate will eventually put the wrong one there - because the row would still look
// fine. So the rates are computed by the database from the row's own components, and a supplied
// rate that disagrees is an error rather than a value.
func TestCacheRatesAreDerivedNotSupplied(t *testing.T) {
	ctx, st := setup(t)

	// One call, components from two different surfaces: the wire billed 106895 cached, the engine
	// internally reused 106895, and the expert cache hit 84.8%.
	if _, err := st.Write(ctx, store.Event{
		EventKey: "rooter|r-3001", Source: "rooter", SourceIDs: []string{"rooter|r-3001"},
		UpstreamID: str("chatcmpl-3001"), TS: time.Now().UTC(),
		Backend: str("strata"), Key: str("pi-code"),
		PromptTokens: i64(107753), CompletionTokens: i64(662),
		CachedTokens: i64(106895), EngineCachedTokens: i64(106895),
		ExpertCacheHitRate: f64(0.848),
		UsageSource:        store.UsageInjected,
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	r := fetch(t, st, "rooter|r-3001")

	// Derived, from this row's own numbers: 106895 / 107753.
	want := 106895.0 / 107753.0
	if !r.CacheHitRate.Valid || math.Abs(r.CacheHitRate.Float64-want) > 1e-9 {
		t.Errorf("cache_hit_rate = %v, want %v derived from cached_tokens/prompt_tokens", r.CacheHitRate, want)
	}
	if !r.EngineRate.Valid || math.Abs(r.EngineRate.Float64-want) > 1e-9 {
		t.Errorf("engine_cache_hit_rate = %v, want %v derived from engine_cached_tokens/prompt_tokens", r.EngineRate, want)
	}

	// The expert rate survives as reported, and it is measurably not the other two. If a future
	// change ever makes them agree, this assertion is where the conflation gets noticed.
	if !r.ExpertRate.Valid || math.Abs(r.ExpertRate.Float64-0.848) > 1e-9 {
		t.Errorf("expert_cache_hit_rate = %v, want 0.848 as reported", r.ExpertRate)
	}
	if diff := math.Abs(r.CacheHitRate.Float64 - r.ExpertRate.Float64); diff < 0.1 {
		t.Errorf("prompt-reuse rate and expert-cache rate differ by only %v - these are different quantities and one of them has been moved", diff)
	}

	// The rejections. Each one is a write that a plausible harvester could actually do.
	cases := []struct {
		name  string
		set   string
		wants string
	}{
		{
			// Smuggling the expert rate into the prompt-reuse column: same row, right shape, wrong
			// quantity. The row would otherwise look perfectly reasonable.
			name:  "expert rate smuggled into cache_hit_rate",
			set:   "cache_hit_rate = 0.848",
			wants: "does not match",
		},
		{
			// A rate with its numerator removed. The merge that does this is one that fills a gap
			// with a number and never fills the components the number came from.
			name:  "rate with no cached_tokens",
			set:   "cached_tokens = NULL, cache_hit_rate = 0.9",
			wants: "no cached_tokens",
		},
		{
			// More cached than prompt: two surfaces describing different calls, agreeing by accident.
			name:  "cached_tokens exceeds prompt_tokens",
			set:   "cached_tokens = 120000",
			wants: "exceeds prompt_tokens",
		},
		{
			// The denominator is gone, so nothing in this row supports a rate.
			name:  "rate with no denominator",
			set:   "prompt_tokens = NULL, cache_hit_rate = 0.5",
			wants: "no denominator",
		},
	}
	for _, c := range cases {
		_, err := st.DB().ExecContext(ctx, "UPDATE call_events SET "+c.set+" WHERE event_key = $1", "rooter|r-3001")
		if err == nil {
			t.Errorf("%s: accepted, want a rejection", c.name)
			continue
		}
		if !strings.Contains(err.Error(), c.wants) {
			t.Errorf("%s: error = %q, want it to explain %q", c.name, err.Error(), c.wants)
		}
	}

	// Positive control: the derivation follows the components. Change the numerator and the stored
	// rate moves, without anybody computing it.
	if _, err := st.DB().ExecContext(ctx,
		"UPDATE call_events SET cached_tokens = 100000 WHERE event_key = $1", "rooter|r-3001"); err != nil {
		t.Fatalf("legitimate update rejected: %v", err)
	}
	r = fetch(t, st, "rooter|r-3001")
	if want2 := 100000.0 / 107753.0; math.Abs(r.CacheHitRate.Float64-want2) > 1e-9 {
		t.Errorf("cache_hit_rate = %v after changing cached_tokens, want %v", r.CacheHitRate, want2)
	}
	// The engine rate did not follow it: it is derived from engine_cached_tokens, which is a
	// different measurement of a similar concept, and keeping them separate is the point of having
	// both.
	if math.Abs(r.EngineRate.Float64-want) > 1e-9 {
		t.Errorf("engine_cache_hit_rate = %v, want it to stay %v (engine_cached_tokens did not change)", r.EngineRate, want)
	}
}

// TestFallbackAttributionRestartsFromStartProximity is a correction, and a live run is what made it.
//
// The first version of the fallback joined on interval overlap, justified by a premise I had not
// checked: that a router stamps a call when the response ends. It does not. rooter sets `ts` in
// newCallRecord, whose own comment reads "everything here is known before a slot is even requested",
// and strata's request-record `time` was measured 10 ms from a client's own request start on a call
// that then ran 11.4 s. Both surfaces stamp the START of the call.
//
// Then the harvester ran against live strata. A 125.6 s envelope - stamped at its completion, because
// that is what the premise said - drew an interval from 03:05:49 to 03:07:54 and absorbed an unrelated
// 4.7 s engine record beginning at 03:07:53. That is the failure mode of "the intervals touch": the
// longer a call runs, the more unrelated work it can swallow, so a busy long generation becomes a
// magnet for other people's records. The join looked successful, and it was wrong.
//
// So: starts within a window, with queue wait allowed for (wait_ms is the legitimate part of a
// start-to-start difference), and overlap demoted to a secondary guard. The invariant "ts is the start
// of the call" is enforced in the probe, because that is where a surface's stamping habit is known.
func TestFallbackAttributionRestartsFromStartProximity(t *testing.T) {
	ctx, st := setup(t)
	base := time.Now().UTC().Truncate(time.Microsecond).Add(-3600 * time.Second)

	env := func(key string, ts time.Time, totalMs, waitMs float64, backend string) store.Event {
		return store.Event{
			EventKey: key, Source: "rooter", SourceIDs: []string{key}, TS: ts,
			Backend: str(backend), Key: str("pi-code"), KeySource: str("header"),
			Status: ival(200), Stream: bval(true), Aborted: bval(true), Bytes: i64(90000),
			TotalMs: f64(totalMs), WaitMs: f64(waitMs), UsageSource: store.UsageNone,
		}
	}
	engine := func(key string, ts time.Time, totalMs float64, prompt, out, reused int64, backend string) store.Event {
		return store.Event{
			EventKey: key, Source: "strata", SourceIDs: []string{key}, TS: ts, Backend: str(backend),
			PromptTokens: i64(prompt), CompletionTokens: i64(out), EngineCachedTokens: i64(reused),
			CachedTokens: i64(reused), TotalMs: f64(totalMs), DecodeTokS: f64(63.8),
			UsageSource: store.UsageRunnerScrape, RequireAttribution: true,
		}
	}

	t.Run("two_surfaces_that_both_stamp_the_start_join", func(t *testing.T) {
		if _, err := st.Write(ctx, env("rooter|live-a", base, 125600, 3.0, "strata")); err != nil {
			t.Fatalf("seed: %v", err)
		}
		res, err := st.Write(ctx, engine("strata|df04a0ed9165", base.Add(40*time.Millisecond),
			125560, 41084, 9024, 40958, "strata"))
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if res.Path != store.PathJoinFallback {
			t.Fatalf("path = %q, want join_fallback", res.Path)
		}
		r := fetch(t, st, "rooter|live-a")
		if !r.HasUsage || r.PromptTokens.Int64 != 41084 {
			t.Errorf("has_usage=%v prompt=%v: the engine numbers did not arrive", r.HasUsage, r.PromptTokens)
		}
		if !(r.CacheHitRate.Float64 > 0.99 && r.CacheHitRate.Float64 <= 1) {
			t.Errorf("cache_hit_rate = %v, want the prompt-reuse ratio (40958/41084 = 0.9969)", r.CacheHitRate)
		}
	})

	t.Run("a_long_call_does_not_absorb_an_unrelated_record_that_began_minutes_later", func(t *testing.T) {
		// The live incident, as a regression.
		if _, err := st.Write(ctx, env("rooter|live-b", base.Add(60*time.Second), 125600, 3.0, "sushi")); err != nil {
			t.Fatalf("seed: %v", err)
		}
		res, err := st.Write(ctx, engine("strata|d3445f6c1b81", base.Add(184*time.Second),
			4700, 56374, 279, 56337, "sushi"))
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if res.Path != store.PathUnattributed {
			t.Fatalf("path = %q, want unattributed: 124 seconds apart is not the same call, however long the envelope is", res.Path)
		}
		if n := count(t, st, "event_key = 'strata|d3445f6c1b81'"); n != 0 {
			t.Errorf("rows = %d, want 0: an engine record that cannot be attributed is reported, not written", n)
		}
		if r := fetch(t, st, "rooter|live-b"); r.HasUsage {
			t.Error("has_usage true: the envelope picked up numbers from work it did not cause")
		}
	})

	t.Run("queue_wait_is_a_legitimate_start_difference", func(t *testing.T) {
		// A request that waited 8 s for a slot is described 8 s apart by the two surfaces, and that is
		// not two calls. wait_ms is the size of the excuse, so it is part of the window rather than a
		// reason to widen the window for everyone.
		if _, err := st.Write(ctx, env("rooter|live-c", base.Add(400*time.Second), 30000, 8000.0, "queue")); err != nil {
			t.Fatalf("seed: %v", err)
		}
		res, err := st.Write(ctx, engine("strata|queued-1", base.Add(408*time.Second), 29000,
			1000, 200, 900, "queue"))
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if res.Path != store.PathJoinFallback {
			t.Fatalf("path = %q, want join_fallback: the engine began when a slot freed up, that is what wait_ms means", res.Path)
		}
	})

	t.Run("the_engine_surface_arriving_first_joins_the_other_way_round", func(t *testing.T) {
		res, err := st.Write(ctx, engine("strata|early-1", base.Add(700*time.Second), 20000,
			2000, 400, 1800, "order"))
		if err != nil {
			t.Fatalf("write engine: %v", err)
		}
		if res.Path != store.PathUnattributed {
			t.Fatalf("path = %q, want unattributed: nothing knows this call yet", res.Path)
		}
		// The engine's record is gone, and that is the correct outcome for a surface that cannot say a
		// client asked. When rooter reports the same call later, the envelope is written on its own
		// and the numbers stay where they can be seen: in the harvester's counters and in strata.
		if _, err := st.Write(ctx, env("rooter|order-1", base.Add(700*time.Second), 20000, 2.0, "order")); err != nil {
			t.Fatalf("write envelope: %v", err)
		}
		if n := count(t, st, "backend = 'order'"); n != 1 {
			t.Errorf("rows = %d, want 1: the call rooter reported is one call, not two", n)
		}
	})
}
