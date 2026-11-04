// Package sources turns a surface's own reporting into store.Event values.
//
// Each source here is a translation layer and nothing else: it does not decide what a call is, it
// does not join, and it does not retry. Those are the store's jobs, because the join rules are the
// contract and a contract enforced in three places drifts.
package sources

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/initialed85/modelcalls/internal/store"
)

// Strata reads strata's /metrics, which is a JSON document (not Prometheus text: the wire format
// for this runner is its own, and the Prometheus view of it is served by strata-exporter).
//
// What matters about this source is what it CANNOT say. A strata request record has no session key
// and no client identity - the runner does not know who asked - and the id it carries is its own
// internal request id, not the chatcmpl id it put in the response. So every event produced here is
// marked RequireAttribution: it may only be written when it can be joined to a row that does know a
// client. Without that rule, every engine record becomes its own row, and the store starts counting
// one generation twice - once with a client and once without. That is not a missing-data problem,
// it is a fabricated-data problem, and it is the reason this file refuses to insert.
//
// The join that does work is the good one: strata's id has no chatcmpl equivalent, so attribution
// here rests on the fallback predicate. The permanent fix is for strata to expose the chatcmpl id it
// minted per request; at that point this source sets UpstreamID and the join stops being a guess.
type Strata struct {
	URL   string
	Token string
	// Backend, Alias, Profile name the runner from the OUTSIDE. strata's own document says which
	// model and engine it is running, but not which alias of rooter's it is mounted under, and
	// inventing one here would be a label nobody else uses.
	Backend, Alias, Profile string
	HTTP                    *http.Client

	// seen holds the record ids already turned into events this process, so a 2-second poll of a
	// metrics document that keeps the last 12 requests does not re-submit the same 12 forever.
	// It is a cache, not correctness: the store's event_key merge is what makes a re-pull harmless.
	seen map[string]bool
}

type strataDoc struct {
	Engine struct {
		Model   string  `json:"model"`
		Engine  string  `json:"engine"`
		Version string  `json:"version"`
		Context float64 `json:"context"`
	} `json:"engine"`
	Requests []strataRequest `json:"requests"`
}

// strataRequest is one entry of the "requests" array. Field names are strata's, measured against a
// live document rather than guessed: this list is the reason the rate columns stay honest, so the
// mapping from reused -> engine_cached_tokens is load-bearing and the mapping from hit_rate ->
// anywhere is not.
type strataRequest struct {
	ID              string  `json:"id"`
	Time            float64 `json:"time"`
	DurationS       float64 `json:"duration_s"`
	PromptMs        float64 `json:"prompt_ms"`
	DecodeMs        float64 `json:"decode_ms"`
	PromptTokens    float64 `json:"prompt_tokens"`
	PromptTotal     float64 `json:"prompt_total"`
	PromptRead      float64 `json:"prompt_read"`
	Reused          float64 `json:"reused"`
	OutputTokens    float64 `json:"output_tokens"`
	DecodeTokS      float64 `json:"decode_tok_s"`
	HitRate         float64 `json:"hit_rate"`
	Finish          string  `json:"finish"`
	FirstEngineTokS float64 `json:"first_engine_token_s"`
	DraftsOffered   float64 `json:"drafts_offered"`
	DraftsAccepted  float64 `json:"drafts_accepted"`
	EngineGenerated float64 `json:"engine_generated"`
	Projection      bool    `json:"projection"`
}

// Pull fetches the document and returns the events for records this process has not handed over
// yet, oldest first.
func (s *Strata) Pull(ctx context.Context) ([]store.Event, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL, nil)
	if err != nil {
		return nil, err
	}
	if s.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.Token)
	}
	client := s.HTTP
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("strata %s: %w", s.URL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("strata %s: HTTP %d", s.URL, resp.StatusCode)
	}

	var doc strataDoc
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, fmt.Errorf("strata %s: not the JSON document this source expects: %w", s.URL, err)
	}

	out := make([]store.Event, 0, len(doc.Requests))
	for _, r := range doc.Requests {
		if r.ID == "" || s.seen[r.ID] {
			continue
		}
		if s.seen == nil {
			s.seen = map[string]bool{}
		}
		s.seen[r.ID] = true
		out = append(out, s.event(doc, r))
	}
	return out, nil
}

func (s *Strata) event(doc strataDoc, r strataRequest) store.Event {
	backend, alias, profile := s.Backend, s.Alias, s.Profile
	if backend == "" {
		backend = "strata"
	}
	if alias == "" {
		alias = backend
	}
	ev := store.Event{
		// The key is namespaced by source: this id is strata's own and means nothing to any other
		// surface, so it must not be able to collide with one.
		EventKey:  "strata|" + r.ID,
		Source:    "strata",
		SourceIDs: []string{"strata|" + r.ID},
		// Measured: this is the START of the call, not its end (10 ms off the client's own start on
		// a request that then took 11.4 s). The fallback join depends on that being true.
		//
		// strata reports epoch seconds as a float. Splitting it is not a detail: the first version of
		// this line passed seconds x 1e6 as the SECONDS argument of time.Unix and Postgres rejected
		// "56844014-10-05", which is the sound a timestamp bug makes when the column has a range.
		TS:                 epochSeconds(r.Time),
		Backend:            &backend,
		Alias:              &alias,
		RequireAttribution: true,
		UsageSource:        store.UsageRunnerScrape,
	}
	if profile != "" {
		ev.Profile = &profile
	}
	if doc.Engine.Model != "" {
		m := doc.Engine.Model
		ev.Model = &m
	}
	if doc.Engine.Engine != "" {
		e := doc.Engine.Engine
		ev.Engine = &e
	}
	if r.Finish != "" {
		f := r.Finish
		ev.FinishReason = &f
	}

	// The runner's own prompt count is prompt_total; prompt_tokens is the same quantity for the
	// calls measured here, and where the two differ the larger one is the one that charged the
	// context. CachedTokens and EngineCachedTokens both take `reused`: this IS the engine's count of
	// prompt tokens it did not re-read, and the derived cache_hit_rate is then exactly the ratio the
	// column means.
	setInt(&ev.PromptTokens, r.PromptTotal)
	setInt(&ev.CompletionTokens, r.OutputTokens)
	setInt(&ev.CachedTokens, r.Reused)
	setInt(&ev.EngineCachedTokens, r.Reused)
	setInt(&ev.DraftsOffered, r.DraftsOffered)
	setInt(&ev.DraftsAccepted, r.DraftsAccepted)
	setFloat(&ev.PromptMs, r.PromptMs)
	setFloat(&ev.DecodeMs, r.DecodeMs)
	setFloat(&ev.DecodeTokS, r.DecodeTokS)
	setFloat(&ev.TTFTMs, r.FirstEngineTokS*1000)
	setFloat(&ev.TotalMs, r.DurationS*1000)
	if r.PromptMs > 0 && r.PromptTotal > 0 {
		v := r.PromptTotal / (r.PromptMs / 1000)
		ev.PrefillTokS = &v
	}

	// hit_rate goes to extra and nowhere else. Measured on the same calls it reports: 0.802 to 0.903,
	// while reused/prompt_tokens on those same calls was 0.992 to 0.9996. It is the MoE expert-cache
	// rate. The one place it is allowed as a column is expert_cache_hit_rate, which is exactly what
	// it is and which the schema keeps out of every prompt-reuse average.
	ev.ExpertCacheHitRate = &r.HitRate
	ev.Extra = map[string]any{
		"raw": map[string]any{
			"id":                 r.ID,
			"time":               r.Time,
			"hit_rate":           r.HitRate,
			"reused":             r.Reused,
			"prompt_tokens":      r.PromptTokens,
			"prompt_total":       r.PromptTotal,
			"prompt_read":        r.PromptRead,
			"engine_generated":   r.EngineGenerated,
			"projection":         r.Projection,
			"duration_s":         r.DurationS,
			"first_engine_token": r.FirstEngineTokS,
		},
	}
	return ev
}

func epochSeconds(v float64) time.Time {
	sec := int64(v)
	nsec := int64((v - float64(sec)) * 1e9)
	return time.Unix(sec, nsec).UTC()
}

func setInt(dst **int64, v float64) {
	if v > 0 {
		x := int64(v)
		*dst = &x
	}
}

func setFloat(dst **float64, v float64) {
	if v > 0 {
		x := v
		*dst = &x
	}
}
