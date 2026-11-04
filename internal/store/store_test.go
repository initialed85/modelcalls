package store_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
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
	EventKey      sql.NullString
	Source        sql.NullString
	ID            sql.NullString
	UpstreamID    sql.NullString
	Backend       sql.NullString
	Key           sql.NullString
	Model         sql.NullString
	Engine        sql.NullString
	JoinMethod    sql.NullString
	UsageSource   sql.NullString
	FinishReason  sql.NullString
	Aborted       bool
	HasUsage      bool
	TSIsObs       bool
	SourceCount   int
	TS            time.Time
	TTFTMs        sql.NullFloat64
	DecodeTokS    sql.NullFloat64
	CacheHitRate  sql.NullFloat64
	Bytes         sql.NullInt64
	PromptTokens  sql.NullInt64
	CompletedTok  sql.NullInt64
	DraftsOk      sql.NullInt64
	ExtraHasStrata bool
}

func fetch(t *testing.T, st *store.Store, eventKey string) Row {
	t.Helper()
	var r Row
	err := st.DB().QueryRow(`
		SELECT event_key, source, id, upstream_id, backend, "key", model, engine,
		       join_method, usage_source, finish_reason,
		       aborted, has_usage, ts_is_observation, source_count, ts,
		       ttft_ms, decode_tok_s, cache_hit_rate, bytes,
		       prompt_tokens, completion_tokens, drafts_accepted,
		       extra ? 'strata'
		  FROM call_events WHERE event_key = $1`, eventKey).
		Scan(&r.EventKey, &r.Source, &r.ID, &r.UpstreamID, &r.Backend, &r.Key, &r.Model, &r.Engine,
			&r.JoinMethod, &r.UsageSource, &r.FinishReason,
			&r.Aborted, &r.HasUsage, &r.TSIsObs, &r.SourceCount, &r.TS,
			&r.TTFTMs, &r.DecodeTokS, &r.CacheHitRate, &r.Bytes,
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

func str(s string) *string { return &s }
func f64(v float64) *float64 { return &v }
func i64(v int64) *int64 { return &v }
func ival(v int) *int { return &v }
func bval(v bool) *bool { return &v }

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
			CacheHitRate: f64(0.999616),
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
			Key: str("pi-code"),
			PromptTokens: i64(106718), CompletionTokens: i64(700),
			CachedTokens: i64(106677), EngineCachedTokens: i64(106677),
			PromptMs: f64(471.1), DecodeMs: f64(9961.0),
			PrefillTokS: f64(2133.6), DecodeTokS: f64(70.3), CacheHitRate: f64(0.999616),
			UsageSource:  store.UsageRunnerScrape,
			FinishReason: str("stop"),
			DraftsOffered: i64(690), DraftsAccepted: i64(431),
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
			Backend: str("strata"), Alias: str("strata"), Profile: str("iq3_xxs"), Engine: str("0.1.51"),
			Key: str("pi-code"),
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
			Model: str("Qwen3-Next-80B-A3B-Instruct-2.6bpw"),
			Stream: bval(true), Status: ival(200), Aborted: bval(false),
			PromptTokens: i64(113186), CompletionTokens: i64(217), CachedTokens: i64(110674),
			PromptMs: f64(6804.7), DecodeMs: f64(4617.0),
			PrefillTokS: f64(369.2), DecodeTokS: f64(47.0), CacheHitRate: f64(0.977806),
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
			Backend: str("strata"), Key: str("pi-code"),
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
			Backend: str("sushi"), Key: str("pi-code"),
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
