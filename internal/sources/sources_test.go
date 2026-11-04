package sources_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/initialed85/modelcalls/internal/sources"
)

// These tests pin the translation, not the transport. Each fixture is a shape copied from a live
// surface, because every one of these mappings has been wrong at least once under real data: the
// epoch conversion, which count is the prompt, and where a rate is allowed to go.

func TestStrataPull(t *testing.T) {
	doc := `{
	  "engine": {"model": "qwen3.8-flash-next-iq3_xxs", "engine": "0.1.51", "context": 131072},
	  "requests": [
	    {"id": "5628878f244b", "time": 1793761423.6647432, "duration_s": 125.6,
	     "prompt_ms": 900.0, "decode_ms": 124600.0, "prompt_tokens": 78049, "prompt_total": 78049,
	     "prompt_read": 800, "reused": 77279, "output_tokens": 1900, "decode_tok_s": 56.4,
	     "hit_rate": 0.888, "finish": "stop", "first_engine_token_s": 0.9,
	     "drafts_offered": 400, "drafts_accepted": 260, "engine_generated": 1900, "projection": false},
	    {"id": "aaaaaaaaaaaa", "time": 1793761500.5, "duration_s": 4.7, "prompt_ms": 40.0,
	     "decode_ms": 4600.0, "prompt_tokens": 100, "prompt_total": 100, "prompt_read": 100,
	     "reused": 0, "output_tokens": 200, "decode_tok_s": 43.0, "hit_rate": 0.0,
	     "finish": "length", "first_engine_token_s": 0.04}
	  ]
	}`
	var got int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got++
		if auth := r.Header.Get("Authorization"); auth != "Bearer tok" {
			t.Errorf("Authorization = %q, want the bearer token sent", auth)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(doc))
	}))
	defer srv.Close()

	s := &sources.Strata{URL: srv.URL, Token: "tok", Backend: "strata", Alias: "strata", Profile: "iq3_xxs"}
	events, err := s.Pull(context.Background())
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
	ev := events[0]

	// The conversion that failed against live data: seconds x 1e6 passed as the SECONDS argument
	// produced a timestamp in the year 56844014, and Postgres rejected it.
	//
	// Compared with a tolerance rather than exactly, because a float64 epoch second cannot hold a
	// nanosecond: 1793761423.6647432 decodes to .664743185, 15 ns away. Postgres stores microseconds,
	// so the difference is below the resolution of the column that receives it - and a test that
	// demanded the exact nanosecond would be asserting a precision the pipeline does not have.
	want := time.Unix(1793761423, 664743200).UTC()
	if d := ev.TS.Sub(want); d < -time.Microsecond || d > time.Microsecond {
		t.Errorf("ts = %v, want %v within a microsecond (strata reports epoch seconds as a float)", ev.TS, want)
	}
	if !ev.RequireAttribution {
		t.Error("RequireAttribution false: a strata record cannot say a client asked, and inserting one invents a call")
	}
	if ev.Key != nil {
		t.Errorf("Key = %v, want nil: strata does not know who asked, and a made-up key would make this row look like a wire witness", ev.Key)
	}
	if ev.UpstreamID != nil {
		t.Errorf("UpstreamID = %v, want nil: this id is strata's own, not the chatcmpl id in the response", ev.UpstreamID)
	}
	if ev.PromptTokens == nil || *ev.PromptTokens != 78049 {
		t.Errorf("prompt_tokens = %v, want prompt_total (78049)", ev.PromptTokens)
	}
	if ev.CachedTokens == nil || *ev.CachedTokens != 77279 || ev.EngineCachedTokens == nil || *ev.EngineCachedTokens != 77279 {
		t.Errorf("cached = %v engine_cached = %v, want both to take `reused` (77279)", ev.CachedTokens, ev.EngineCachedTokens)
	}
	if ev.ExpertCacheHitRate == nil || *ev.ExpertCacheHitRate != 0.888 {
		t.Errorf("expert_cache_hit_rate = %v, want hit_rate (0.888), and only there", ev.ExpertCacheHitRate)
	}
	if ev.TotalMs == nil || *ev.TotalMs != 125600 {
		t.Errorf("total_ms = %v, want duration_s x 1000", ev.TotalMs)
	}
	if ev.PrefillTokS == nil || *ev.PrefillTokS < 86700 || *ev.PrefillTokS > 86900 {
		t.Errorf("prefill_tok_s = %v, want 78049 / 0.9 s", ev.PrefillTokS)
	}
	raw, _ := json.Marshal(ev.Extra["raw"])
	if !json.Valid(raw) || len(raw) == 0 {
		t.Error("extra.raw missing: the record the runner wrote about itself has to survive")
	}

	// A re-pull of the same document is not new information. The store would merge it harmlessly as a
	// re-pull, but a harvester that re-submits 12 records every cycle is a harvester that hides the
	// records that matter in noise.
	again, err := s.Pull(context.Background())
	if err != nil {
		t.Fatalf("second pull: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("second pull = %d events, want 0 (seen ids); server hits = %d", len(again), got)
	}
}

func TestRooterPull(t *testing.T) {
	var lastQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastQuery = r.URL.RawQuery
		if r.URL.Path != "/rooter/events" {
			t.Errorf("path = %q, want /rooter/events", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{
		  "cursor": 77, "stale": false, "dropped_total": 0, "available": 2, "boot_id": "b1",
		  "events": [
		    {"id": "r-1", "upstream_id": "chatcmpl-xyz", "upstream_created": 1793761423,
		     "ts": "2026-11-04T03:03:43.664Z", "path": "/v1/chat/completions", "method": "POST",
		     "kind": "gen", "key": "pi-code", "key_source": "header", "backend": "strata",
		     "backend_url": "http://192.168.11.128:8088", "alias": "strata", "profile": "iq3_xxs",
		     "model": "qwen3.8-flash-next-iq3_xxs", "stream": true, "status": 200, "bytes": 4096,
		     "aborted": true, "wait_ms": 2.4, "ttft_ms": 900.1, "total_ms": 125600.0,
		     "cached_tokens": 77279, "prefill_ms": 900.0, "cache_hit_rate": 0.9901,
		     "finish_reason": "stop", "extra": {"raw": {"note": "tail empty"}}},
		    {"id": "r-2", "ts": "not-a-time", "path": "/v1/models", "method": "GET", "kind": "read",
		     "key": "pi-code", "key_source": "header", "backend": "", "status": 401, "bytes": 91}
		  ]}`))
	}))
	defer srv.Close()

	ro := &sources.Rooter{URL: srv.URL, Token: "k", Limit: 200}
	events, err := ro.Pull(context.Background())
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
	if ro.Cursor != 77 {
		t.Errorf("cursor = %d, want the envelope's 77", ro.Cursor)
	}

	ev := events[0]
	if ev.RequireAttribution {
		t.Error("RequireAttribution set on a rooter record: this is the surface that KNOWS a client asked")
	}
	if ev.UpstreamID == nil || *ev.UpstreamID != "chatcmpl-xyz" {
		t.Errorf("upstream_id = %v, want chatcmpl-xyz: it is the only join key that means the same thing to both sides", ev.UpstreamID)
	}
	if ev.Key == nil || *ev.Key != "pi-code" {
		t.Errorf("key = %v, want pi-code", ev.Key)
	}
	if !ev.TS.Equal(time.Date(2026, 11, 4, 3, 3, 43, 664000000, time.UTC)) {
		t.Errorf("ts = %v, want the record's own stamp", ev.TS)
	}
	if ev.TSIsObservation {
		t.Error("ts_is_observation set on a record that carries a real timestamp")
	}
	// rooter's prefill_ms and strata's prompt_ms are one quantity, and the schema holds one column.
	if ev.PromptMs == nil || *ev.PromptMs != 900.0 {
		t.Errorf("prompt_ms = %v, want rooter's prefill_ms (900) written into the single column", ev.PromptMs)
	}
	if ev.UsageSource != "" {
		t.Errorf("usage_source = %q, want empty: rooter's record does not say which mechanism produced the counts", ev.UsageSource)
	}
	if _, ok := ev.Extra["upstream_created"]; !ok {
		t.Error("upstream_created missing from extra: it is the join aid for a runner record indexed by its own clock")
	}
	if v, ok := ev.Extra["rooter_cache_hit_rate"]; !ok || v != 0.9901 {
		t.Errorf("rooter_cache_hit_rate = %v, want the rate kept as evidence rather than sent as a column the database derives", ev.Extra["rooter_cache_hit_rate"])
	}
	// The second record is the honest shape of a rejected call: no backend, because the request never
	// reached one. An empty string there would read as a backend named "".
	rej := events[1]
	if rej.Backend != nil {
		t.Errorf("backend = %v, want nil for a call the router answered itself", rej.Backend)
	}
	if !rej.TSIsObservation {
		t.Error("an unparseable ts must be kept and flagged as an observation, not dropped")
	}
	if rej.Status == nil || *rej.Status != 401 {
		t.Errorf("status = %v, want 401", rej.Status)
	}

	// The cursor must be sent back, or the next poll re-reads the ring forever.
	if _, err := ro.Pull(context.Background()); err != nil {
		t.Fatalf("second pull: %v", err)
	}
	if lastQuery != "cursor=77&limit=200" {
		t.Errorf("second request query = %q, want cursor=77&limit=200", lastQuery)
	}
}

func TestRooterPullOnABuildWithoutTheRing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	ro := &sources.Rooter{URL: srv.URL, Token: "k"}
	_, err := ro.Pull(context.Background())
	if err == nil {
		t.Fatal("want an error: a router with no ring cannot be harvested")
	}
	// This is the failure that actually happened first: home-dev was running an image predating
	// /rooter/events, and a bare 404 reads like a typo in the path rather than a version gap.
	if want := "predates"; wantNotContains(t, err.Error(), want) {
		t.Errorf("error = %q, want it to name the real cause (the deployed image %s /rooter/events)", err, want)
	}
}

func wantNotContains(t *testing.T, hay, needle string) bool {
	t.Helper()
	for i := 0; i+len(needle) <= len(hay); i++ {
		if hay[i:i+len(needle)] == needle {
			return false
		}
	}
	return true
}
