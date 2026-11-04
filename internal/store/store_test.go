package store_test

import (
	"context"
	"database/sql"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/lib/pq"
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

	// The binary's own migration path first, then a truncate: the contract is only meaningful from a
	// known empty state, and a fresh database has nothing to truncate. Going through MigrateLatest
	// rather than reading files off disk means the tests run against exactly the schema a deployed
	// pod brings its database up to, and a harness that applied only 001 would never execute the
	// upgrade path - which is where 002's reason for existing lives.
	applied, err := st.MigrateLatest(ctx)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if len(applied) == 0 {
		t.Fatalf("MigrateLatest applied nothing")
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
		var calls, withUsage, withoutUsage, aborted, joined, byContent, byTiming, observed int
		err := st.DB().QueryRow(`
			SELECT count(*),
			       count(*) FILTER (WHERE has_usage),
			       count(*) FILTER (WHERE NOT has_usage),
			       count(*) FILTER (WHERE aborted),
			       count(*) FILTER (WHERE source_count > 1),
			       count(*) FILTER (WHERE join_method = 'usage_content_window'),
			       count(*) FILTER (WHERE join_method = 'backend_key_window'),
			       count(*) FILTER (WHERE ts_is_observation)
			  FROM call_events`).Scan(&calls, &withUsage, &withoutUsage, &aborted, &joined, &byContent, &byTiming, &observed)
		if err != nil {
			t.Fatalf("shape: %v", err)
		}
		t.Logf("store shape: %d calls, %d with usage, %d without, %d abandoned, %d joined across surfaces, %d joined by content, %d joined by timing only, %d with an observed timestamp",
			calls, withUsage, withoutUsage, aborted, joined, byContent, byTiming, observed)
		if calls == 0 {
			t.Fatal("empty store")
		}
		if byContent+byTiming > joined {
			t.Errorf("fallback joins (%d+%d) exceed upstream joins: the heuristics are doing more work than the reliable key, which is a design smell worth noticing", byContent, byTiming)
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

// TestUndeterminableRatesAndComputedPromptCounts is the part of the contract that reverses an earlier
// decision, and it reverses it for a reason that cost data: this schema used to RAISE when
// cached_tokens exceeded prompt_tokens, on the theory that only a bad join could do that. llama.cpp's
// `prompt_n` is not the whole prompt - it is the tokens the runner processed on this pass - so the
// runner's own numbers can legitimately point that way, and rooter publishes prompt_tokens_computed
// beside the reported count instead of overwriting it. Rejecting the row threw away a call a router had
// fully witnessed, over a ratio that cannot be computed. The rate goes NULL, the row flags it, and the
// components survive. A CLAIMED rate is still refused, because components that support no rate mean any
// number in that column came from somewhere else.
func TestUndeterminableRatesAndComputedPromptCounts(t *testing.T) {
	ctx, st := setup(t)
	ts := time.Now().UTC().Add(-900 * time.Second)

	if _, err := st.Write(ctx, store.Event{
		EventKey: "rooter|r-4001", Source: "rooter", SourceIDs: []string{"rooter|r-4001"}, TS: ts,
		Backend: str("strata"), Key: str("pi-code"), UsageSource: store.UsageNone,
		PromptTokens: i64(106718), CompletionTokens: i64(641), CachedTokens: i64(106677),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	t.Run("a_rate_the_components_cannot_produce_is_null_and_flagged", func(t *testing.T) {
		if _, err := st.DB().ExecContext(ctx,
			"UPDATE call_events SET cached_tokens = 120000 WHERE event_key = $1", "rooter|r-4001"); err != nil {
			t.Fatalf("accepted a write that should have succeeded: %v", err)
		}
		var rate sql.NullFloat64
		var undefined bool
		var cached, prompt int64
		if err := st.DB().QueryRowContext(ctx, `SELECT cache_hit_rate, cache_rate_undefined, cached_tokens, prompt_tokens
			FROM call_events WHERE event_key = $1`, "rooter|r-4001").
			Scan(&rate, &undefined, &cached, &prompt); err != nil {
			t.Fatalf("read back: %v", err)
		}
		if rate.Valid {
			t.Errorf("cache_hit_rate = %v, want NULL: 120000 cached over %d prompt tokens is not a rate", rate.Float64, prompt)
		}
		if !undefined {
			t.Error("cache_rate_undefined false: the row is hiding that its rate is unusable")
		}
		if cached != 120000 {
			t.Errorf("cached_tokens = %d, want 120000: the components are the data, the rate is a derived read of them", cached)
		}
		if prompt != 106718 {
			t.Errorf("prompt_tokens = %d, want the reported count preserved", prompt)
		}

		// A claimed rate on these components is still refused in both directions.
		if _, err := st.DB().ExecContext(ctx,
			"UPDATE call_events SET cache_hit_rate = 0.848 WHERE event_key = $1", "rooter|r-4001"); err == nil {
			t.Error("accepted a claimed rate on components that support no rate: that number came from somewhere else")
		} else if !strings.Contains(err.Error(), "support no rate") {
			t.Errorf("error = %q, want it to say the components support no rate", err)
		}

		// Consistent components again: the rate comes back and the flag goes away on its own.
		if _, err := st.DB().ExecContext(ctx,
			"UPDATE call_events SET cached_tokens = 106677 WHERE event_key = $1", "rooter|r-4001"); err != nil {
			t.Fatalf("restore: %v", err)
		}
		if err := st.DB().QueryRowContext(ctx, `SELECT cache_hit_rate, cache_rate_undefined FROM call_events WHERE event_key = $1`,
			"rooter|r-4001").Scan(&rate, &undefined); err != nil {
			t.Fatalf("read back after restore: %v", err)
		}
		if !rate.Valid || rate.Float64 < 0.999 || rate.Float64 > 1.0 {
			t.Errorf("cache_hit_rate = %v, want the derived ratio back: %v/%v", rate, 106677, 106718)
		}
		if undefined {
			t.Error("cache_rate_undefined still true after the components became consistent")
		}
	})

	t.Run("the_engine_rate_behaves_the_same_way", func(t *testing.T) {
		if _, err := st.DB().ExecContext(ctx,
			"UPDATE call_events SET engine_cached_tokens = 130000 WHERE event_key = $1", "rooter|r-4001"); err != nil {
			t.Fatalf("accepted a write that should have succeeded: %v", err)
		}
		var engineRate sql.NullFloat64
		var wireRate sql.NullFloat64
		var undefined bool
		if err := st.DB().QueryRowContext(ctx, `SELECT engine_cache_hit_rate, cache_hit_rate, cache_rate_undefined
			FROM call_events WHERE event_key = $1`, "rooter|r-4001").
			Scan(&engineRate, &wireRate, &undefined); err != nil {
			t.Fatalf("read back: %v", err)
		}
		if engineRate.Valid {
			t.Errorf("engine_cache_hit_rate = %v, want NULL", engineRate.Float64)
		}
		if !wireRate.Valid {
			t.Errorf("cache_hit_rate = %v, want the wire rate untouched: an unusable ENGINE rate is not this row's problem", wireRate)
		}
		if !undefined {
			t.Error("cache_rate_undefined false: an engine component out of range is the same state")
		}
		if _, err := st.DB().ExecContext(ctx,
			"UPDATE call_events SET engine_cached_tokens = 106677 WHERE event_key = $1", "rooter|r-4001"); err != nil {
			t.Fatalf("restore: %v", err)
		}
	})

	t.Run("a_computed_prompt_count_is_its_own_number", func(t *testing.T) {
		// rooter reports prompt_n and its own arithmetic answer. Both are stored. Neither replaces the
		// other, in either direction, and a computed count alone does not make a row usable in a token
		// aggregate - has_usage means the runner reported the counts it billed. Live rooter payload:
		// prompt_tokens 98060, prompt_tokens_computed 98060, extra.prompt_n 98060.
		if _, err := st.Write(ctx, store.Event{
			EventKey: "rooter|r-4002", Source: "rooter", SourceIDs: []string{"rooter|r-4002"}, TS: ts.Add(60 * time.Second),
			Backend: str("sushi"), Key: str("pi-code"), UsageSource: store.UsageNone,
			PromptTokens: i64(98060), PromptTokensComputed: i64(98389), CompletionTokens: i64(289),
		}); err != nil {
			t.Fatalf("write: %v", err)
		}
		var reported, computed sql.NullInt64
		var hasUsage bool
		if err := st.DB().QueryRowContext(ctx, `SELECT prompt_tokens, prompt_tokens_computed, has_usage
			FROM call_events WHERE event_key = $1`, "rooter|r-4002").
			Scan(&reported, &computed, &hasUsage); err != nil {
			t.Fatalf("read back: %v", err)
		}
		if reported.Int64 != 98060 || computed.Int64 != 98389 {
			t.Errorf("reported = %d computed = %d, want 98060 and 98389 kept apart: overwriting either one erases the evidence of how often they differ",
				reported.Int64, computed.Int64)
		}
		if !hasUsage {
			t.Error("has_usage false: the reported counts are present, so this row is aggregateable")
		}

		// A surface that only ever publishes the computed answer cannot smuggle it into the reported
		// column: prompt_tokens stays NULL and the row stays out of token aggregates.
		if _, err := st.Write(ctx, store.Event{
			EventKey: "rooter|r-4003", Source: "rooter", SourceIDs: []string{"rooter|r-4003"}, TS: ts.Add(120 * time.Second),
			Backend: str("sushi"), Key: str("pi-code"), UsageSource: store.UsageNone,
			PromptTokensComputed: i64(4242), CompletionTokens: i64(12),
		}); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := st.DB().QueryRowContext(ctx, `SELECT prompt_tokens, prompt_tokens_computed, has_usage
			FROM call_events WHERE event_key = $1`, "rooter|r-4003").
			Scan(&reported, &computed, &hasUsage); err != nil {
			t.Fatalf("read back: %v", err)
		}
		if reported.Valid {
			t.Errorf("prompt_tokens = %d, want NULL: a computed guess did not become a reported count", reported.Int64)
		}
		if computed.Int64 != 4242 {
			t.Errorf("prompt_tokens_computed = %d, want 4242 stored as what it is", computed.Int64)
		}
		if hasUsage {
			t.Error("has_usage true: an arithmetic estimate is not a count the runner billed")
		}
	})
}

// TestContentAgreementIsWhatClosesAJoin is this file's second correction, and it came from a live
// capture rather than from a thought experiment. Rooter's ring records turned out to carry the
// runner's chatcmpl id, so the rows reaching the fallback are mostly rows that ALREADY name their
// call - and a fallback that refuses every identified candidate, as this one did, joins nothing while
// continuing to look correct. Meanwhile 35 rooter events against 12 strata records produced pairs that
// matched to the token (prompt 66933/66933, cached 66509/66509, completion 549/549) while the routers'
// own total_ms differed by 450 ms, and pairs that matched on nothing but a clock (-0.09 s and -0.10 s)
// while reporting 49903 against 66933 prompt tokens and 183 against 1395 completion tokens.
//
// So: content decides when both sides measured, a clock is allowed only where nothing was measured and
// nobody has identified the row yet, and disagreement between two measurements is a veto rather than a
// weak match.
func TestContentAgreementIsWhatClosesAJoin(t *testing.T) {
	ctx, st := setup(t)
	base := time.Now().UTC().Truncate(time.Microsecond).Add(-3600 * time.Second)

	witness := func(key, upstreamID string, ts time.Time, prompt, completion, cached *int64) store.Event {
		ev := store.Event{
			EventKey: key, Source: "rooter", SourceIDs: []string{key}, TS: ts,
			Backend: str("strata"), Key: str("pi-code"), KeySource: str("header"),
			Status: ival(200), Stream: bval(true), TotalMs: f64(10250.4),
			WaitMs: f64(0.004), UsageSource: store.UsageNone,
		}
		if upstreamID != "" {
			ev.UpstreamID = str(upstreamID)
		}
		ev.PromptTokens, ev.CompletionTokens, ev.CachedTokens = prompt, completion, cached
		return ev
	}
	engine := func(key string, ts time.Time, prompt, completion, cached int64) store.Event {
		return store.Event{
			EventKey: key, Source: "strata", SourceIDs: []string{key}, TS: ts, Backend: str("strata"),
			PromptTokens: i64(prompt), CompletionTokens: i64(completion),
			CachedTokens: i64(cached), EngineCachedTokens: i64(cached),
			DecodeTokS: f64(56.4), TotalMs: f64(9800.0),
			UsageSource: store.UsageRunnerScrape, RequireAttribution: true,
		}
	}

	t.Run("numbers_that_agree_join_a_row_that_already_names_its_call", func(t *testing.T) {
		// rooter /31 against strata, as captured live. The envelope is identified - it has the runner's
		// chatcmpl id - and the old rule would have refused it out of hand.
		if _, err := st.Write(ctx, witness("rooter|31", "chatcmpl-b434b754", base,
			i64(66933), i64(549), i64(66509))); err != nil {
			t.Fatalf("seed: %v", err)
		}
		res, err := st.Write(ctx, engine("strata|5628878f244b", base.Add(380*time.Millisecond),
			66933, 549, 66509))
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if res.Path != store.PathJoinFallback {
			t.Fatalf("path = %q, want join_fallback", res.Path)
		}
		r := fetch(t, st, "rooter|31")
		if r.JoinMethod.String != "usage_content_window" {
			t.Errorf("join_method = %q, want usage_content_window: a content join must not wear the same name as a clock join", r.JoinMethod.String)
		}
		if r.UpstreamID.String != "chatcmpl-b434b754" {
			t.Errorf("upstream_id = %q, want the envelope's own id preserved", r.UpstreamID.String)
		}
		if r.DecodeTokS.Float64 != 56.4 {
			t.Errorf("decode_tok_s = %v, want the engine's number (56.4): this is the axis the envelope cannot produce", r.DecodeTokS)
		}
		if r.SourceCount != 2 {
			t.Errorf("source_count = %d, want 2", r.SourceCount)
		}
	})

	t.Run("two_measurements_that_disagree_are_not_one_call_however_close_the_clocks", func(t *testing.T) {
		// The live false positives: rooter /32 sat 90 ms from a strata record and reported 49903 prompt
		// tokens against that record's 66933; rooter /40 sat 100 ms from another with 183 completion
		// tokens against 1395. Timing alone called both of them the same call.
		if _, err := st.Write(ctx, witness("rooter|32", "chatcmpl-49903", base,
			i64(49903), i64(67), i64(49126))); err != nil {
			t.Fatalf("seed 32: %v", err)
		}
		if _, err := st.Write(ctx, witness("rooter|40", "", base.Add(60*time.Second),
			i64(52922), i64(183), i64(52887))); err != nil {
			t.Fatalf("seed 40: %v", err)
		}
		for _, c := range []struct {
			key     string
			ts      time.Time
			prompt  int64
			compl   int64
			cached  int64
			witness string
			why     string
		}{
			{"strata|against-32", base.Add(-90 * time.Millisecond), 66933, 549, 66509, "rooter|32",
				"identified candidate, and the tokens disagree"},
			{"strata|against-40", base.Add(60*time.Second - 100*time.Millisecond), 44333, 1395, 0, "rooter|40",
				"unidentified candidate, but both sides measured and the tokens disagree"},
		} {
			res, err := st.Write(ctx, engine(c.key, c.ts, c.prompt, c.compl, c.cached))
			if err != nil {
				t.Fatalf("%s: %v", c.key, err)
			}
			if res.Path != store.PathUnattributed {
				t.Errorf("%s path = %q, want unattributed: %s", c.key, res.Path, c.why)
			}
			r := fetch(t, st, c.witness)
			if r.SourceCount != 1 {
				t.Errorf("%s source_count = %d, want 1: the envelope kept its own numbers", c.witness, r.SourceCount)
			}
			if r.DecodeTokS.Valid {
				t.Errorf("%s decode_tok_s = %v, want NULL: someone else's decode rate must not land here", c.witness, r.DecodeTokS)
			}
			if r.PromptTokens.Int64 != promptOf(c.witness) {
				t.Errorf("%s prompt_tokens = %d, want the envelope's own %d", c.witness, r.PromptTokens.Int64, promptOf(c.witness))
			}
		}
	})

	t.Run("an_identified_row_with_no_usage_is_not_filled_by_a_clock", func(t *testing.T) {
		// The strict half of the rule, and it costs something: an abandoned call that rooter identified
		// but got no token counts for stays without engine numbers, because the only evidence linking it
		// to an engine record is that they happened at nearly the same time. Those rows keep
		// has_usage = false and show up in call_quality as such. Refusing here is a real loss, and it is
		// smaller than inventing a number inside a row a reader already trusts.
		if _, err := st.Write(ctx, witness("rooter|abandoned", "chatcmpl-abandoned", base.Add(300*time.Second),
			nil, nil, nil)); err != nil {
			t.Fatalf("seed: %v", err)
		}
		res, err := st.Write(ctx, engine("strata|near-abandoned", base.Add(300*time.Second+40*time.Millisecond),
			1000, 200, 900))
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if res.Path != store.PathUnattributed {
			t.Fatalf("path = %q, want unattributed", res.Path)
		}
		r := fetch(t, st, "rooter|abandoned")
		if r.HasUsage {
			t.Error("has_usage true: numbers arrived on a row whose only link to them is a timestamp")
		}
		if n := count(t, st, "event_key = 'strata|near-abandoned'"); n != 0 {
			t.Errorf("rows = %d, want 0: the engine's record is reported, not inserted as a call", n)
		}
	})

	t.Run("a_row_nobody_has_identified_is_still_rescuable_by_timing", func(t *testing.T) {
		// The case the fallback was built for, kept working: a client gave up before the frame carrying
		// the id arrived, so the envelope has no id and no usage, and the engine's record is the only
		// account of what the work cost.
		if _, err := st.Write(ctx, witness("rooter|orphan", "", base.Add(600*time.Second), nil, nil, nil)); err != nil {
			t.Fatalf("seed: %v", err)
		}
		res, err := st.Write(ctx, engine("strata|orphan-work", base.Add(600*time.Second+50*time.Millisecond),
			4000, 120, 3900))
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if res.Path != store.PathJoinFallback {
			t.Fatalf("path = %q, want join_fallback", res.Path)
		}
		if r := fetch(t, st, "rooter|orphan"); r.JoinMethod.String != "backend_key_window" {
			t.Errorf("join_method = %q, want backend_key_window: this join rests on a clock and has to say so", r.JoinMethod.String)
		}
	})
}

func promptOf(eventKey string) int64 {
	switch eventKey {
	case "rooter|32":
		return 49903
	case "rooter|40":
		return 52922
	default:
		return 0
	}
}

// TestThinkingGapIsComputedNotSupplied is the Go mirror of contract scenario step J, and it exists
// because the two halves of this fact arrive at different times in production: rooter's ring reports a
// call it saw end (first byte, no content token), and the content measurement arrives on a later pull.
// If the gap were a column a writer set, the second write would have to remember to recompute it - and
// the failure would be silent, a column that is NULL on exactly the rows that were merged.
func TestThinkingGapIsComputedNotSupplied(t *testing.T) {
	ctx, st := setup(t)

	// A stream that got a first byte and never produced content.
	if _, err := st.Write(ctx, store.Event{
		EventKey: "rooter|gap-1", Source: "rooter", SourceIDs: []string{"rooter|gap-1"},
		ID: str("gap-1"), Backend: str("sushi"), Key: str("probe"),
		TS: time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC), Aborted: bval(true),
		TTFTMs: f64(2350.2),
	}); err != nil {
		t.Fatalf("write first half: %v", err)
	}

	var gap *float64
	if err := st.DB().QueryRowContext(ctx,
		"SELECT first_content_gap_ms FROM call_events WHERE event_key = $1", "rooter|gap-1").Scan(&gap); err != nil {
		t.Fatalf("read gap: %v", err)
	}
	if gap != nil {
		t.Errorf("a call with a first byte and no content token has a gap of %v: an unmeasured gap is NULL, and 0 is a number a panel averages", *gap)
	}

	// The missing half arrives on a later write of the same event. Nobody recomputed anything.
	if _, err := st.Write(ctx, store.Event{
		EventKey: "rooter|gap-1", Source: "rooter", SourceIDs: []string{"rooter|gap-1"},
		ID: str("gap-1"), Backend: str("sushi"), Key: str("probe"),
		TS:             time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC),
		FirstContentMs: f64(32666.1),
	}); err != nil {
		t.Fatalf("write second half: %v", err)
	}
	var ttft, first, computed float64
	if err := st.DB().QueryRowContext(ctx,
		`SELECT ttft_ms, first_content_ms, first_content_gap_ms FROM call_events WHERE event_key = $1`,
		"rooter|gap-1").Scan(&ttft, &first, &computed); err != nil {
		t.Fatalf("read merged row: %v", err)
	}
	if d := computed - (first - ttft); d > 1e-9 || d < -1e-9 {
		t.Errorf("gap after the merge is %v, want %v (first_content_ms %v - ttft_ms %v)", computed, first-ttft, first, ttft)
	}
	if computed < 30000 {
		t.Errorf("gap %v: the merged row lost the measurement, since ttft %v to content %v is a %v second gap",
			computed, ttft, first, computed/1000)
	}
	t.Logf("gap computed by Postgres from the two components: ttft %.1f -> first content %.1f, gap %.1f ms", ttft, first, computed)

	// A re-pull that carries neither timing must not erase either half. This is the shape the fact gets
	// lost in: the surface that reports a call again is usually not the one that measured it.
	if _, err := st.Write(ctx, store.Event{
		EventKey: "rooter|gap-1", Source: "rooter", SourceIDs: []string{"rooter|gap-1"},
		ID: str("gap-1"), Backend: str("sushi"), Key: str("probe"),
		TS: time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("re-pull: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx,
		"SELECT first_content_ms FROM call_events WHERE event_key = $1", "rooter|gap-1").Scan(&first); err != nil {
		t.Fatalf("read after re-pull: %v", err)
	}
	if first != 32666.1 {
		t.Errorf("first_content_ms is %v after a re-pull that carried no measurement, want 32666.1 preserved", first)
	}

	// And a writer cannot supply the gap. Same reason the cache rates are derived: two definitions of
	// one quantity in one column get averaged together on a dashboard with nothing in the row saying
	// which number came from where.
	_, err := st.DB().ExecContext(ctx, `INSERT INTO call_events
		(event_key, source, source_ids, id, ts, backend, ttft_ms, first_content_ms, first_content_gap_ms)
		VALUES ($1,'rooter',$2,$3,$4,'sushi',100,200,12)`,
		"rooter|gap-2", pq.StringArray{"rooter|gap-2"}, "gap-2", time.Now().UTC())
	if err == nil {
		t.Errorf("a writer was allowed to supply first_content_gap_ms, so the column can disagree with the two measurements it derives from")
	} else if !strings.Contains(err.Error(), "first_content_gap_ms") {
		t.Errorf("the insert failed for a different reason, which is not this contract: %v", err)
	}
}

// TestPromptProvenanceSurvivesTheMerge is the store-level half of migration 005.
//
// The reason this is a test and not a comment is that the two engines' shapes are genuinely
// different, and the field that says which is which is the one the harvester was dropping:
//
//	sushi   prompt 72292  computed 742   cached 71550  prompt_n 72292  includes_cache true
//	strata  prompt 60174  computed 140   cached 60034  prompt_n 140    includes_cache <absent>
//
// Both satisfy cached + computed == prompt. On sushi that is the router's own subtraction, i.e. a
// tautology; on strata it is two independent fields agreeing. The store's cache rates use prompt as
// the denominator, so the difference decides whether a rate is a measurement or arithmetic, and the
// only thing that says which is the flag.
//
// The merge path is what matters here, not the insert: the surface that knows the semantics is
// usually not the surface that reports the call again, and a re-pull that carries no flag must not
// erase the flag that made the row readable.
func TestPromptProvenanceSurvivesTheMerge(t *testing.T) {
	ctx, st := setup(t)

	// A sushi-shaped row: prompt_n is the full length, so the computed column is the router's
	// subtraction, and the engine says so.
	if _, err := st.Write(ctx, store.Event{
		EventKey: "rooter|prov-1", Source: "rooter", SourceIDs: []string{"rooter|prov-1"},
		ID: str("prov-1"), Backend: str("sushi"), Key: str("pi-code"),
		TS:           time.Date(2026, 11, 4, 11, 0, 0, 0, time.UTC),
		PromptTokens: i64(72292), PromptTokensComputed: i64(742), CachedTokens: i64(71550),
		RunnerPromptNIncludesCache: bval(true),
	}); err != nil {
		t.Fatalf("write with flag: %v", err)
	}

	// A row from a surface that said nothing. NULL, not false - "the engine did not tell us" and
	// "the engine told us no" are different facts, and reading the first as the second would let a
	// panel conclude that every prompt_n in the store is a work count.
	if _, err := st.Write(ctx, store.Event{
		EventKey: "rooter|prov-2", Source: "rooter", SourceIDs: []string{"rooter|prov-2"},
		ID: str("prov-2"), Backend: str("strata"), Key: str("pi-code"),
		TS:           time.Date(2026, 11, 4, 11, 0, 1, 0, time.UTC),
		PromptTokens: i64(60174), PromptTokensComputed: i64(140), CachedTokens: i64(60034),
	}); err != nil {
		t.Fatalf("write without flag: %v", err)
	}

	var said, silent *bool
	if err := st.DB().QueryRowContext(ctx,
		`SELECT runner_prompt_n_includes_cache, prompt_tokens_derived FROM calls WHERE call_id = $1`,
		"rooter|prov-2").Scan(&said, &silent); err != nil {
		t.Fatalf("read prov-2 through the view: %v", err)
	}
	if said != nil {
		t.Errorf("a row whose surface said nothing has runner_prompt_n_includes_cache = %v; an unknown must stay NULL, because false is a claim", *said)
	}
	if silent != nil {
		t.Errorf("a row whose surface said nothing has prompt_tokens_derived = %v", *silent)
	}

	// The merge: a re-pull that carries the numbers and not the flag. This is the COALESCE path, and
	// it is the one that would silently lose the provenance in production.
	if _, err := st.Write(ctx, store.Event{
		EventKey: "rooter|prov-1", Source: "rooter", SourceIDs: []string{"rooter|prov-1"},
		ID: str("prov-1"), Backend: str("sushi"), Key: str("pi-code"),
		TS:           time.Date(2026, 11, 4, 11, 0, 0, 0, time.UTC),
		PromptTokens: i64(72292), TotalMs: f64(4000),
	}); err != nil {
		t.Fatalf("re-pull without flag: %v", err)
	}
	var kept *bool
	var computed *int64
	if err := st.DB().QueryRowContext(ctx,
		`SELECT runner_prompt_n_includes_cache, prompt_tokens_computed FROM calls WHERE call_id = $1`,
		"rooter|prov-1").Scan(&kept, &computed); err != nil {
		t.Fatalf("read prov-1 after the merge: %v", err)
	}
	if kept == nil || !*kept {
		t.Errorf("the flag was %v after a merge that did not mention it, want true preserved - the flag is the only thing that makes the number beside it interpretable", kept)
	}
	if computed == nil || *computed != 742 {
		t.Errorf("prompt_tokens_computed is %v after the merge, want 742 preserved", computed)
	}

	// And the coverage the store publishes has to separate the three states, because the whole point
	// of the column is that a reader can tell which rows are arithmetic.
	var withFlag, inclCache, equal int64
	if err := st.DB().QueryRowContext(ctx,
		`SELECT calls_with_prompt_n_flag, calls_with_prompt_n_including_cache,
			calls_where_prompt_equals_cached_plus_computed
		FROM call_quality WHERE backend = 'sushi'`).Scan(&withFlag, &inclCache, &equal); err != nil {
		t.Fatalf("read call_quality: %v", err)
	}
	if withFlag != 1 || inclCache != 1 {
		t.Errorf("sushi reports %v rows with the flag and %v including the cache, want 1/1 - the flag is set on exactly one row", withFlag, inclCache)
	}
	if equal < 1 {
		t.Errorf("the equality count is %v, want at least 1: the sushi row satisfies cached + computed == prompt by construction and the store should show that as a tautology, not hide it", equal)
	}
	t.Logf("provenance: %d/%d sushi rows carry the flag, %d satisfy the equality (the equality is not evidence)", withFlag, inclCache, equal)
}
