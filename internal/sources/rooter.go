package sources

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/initialed85/modelcalls/internal/store"
)

// Rooter reads rooter's /rooter/events ring: one record per proxied exchange, with a cursor so a
// consumer can pick up where it stopped.
//
// This is the surface that knows a CLIENT asked. It carries the session key, the wire status, the
// byte count, and - since 94fa4ae - the runner's own chatcmpl id plus the epoch second the runner
// stamped on it. So events from here are written unconditionally: a routed call with no engine
// numbers attached is still a call that happened, and dropping it would undercount exactly the
// failures worth looking at.
//
// The mirror of that rule is why Strata sets RequireAttribution: two surfaces, and only one of them
// is allowed to invent a call.
type Rooter struct {
	URL   string
	Token string
	HTTP  *http.Client

	// Cursor is the ring position this consumer has read up to. It is a position, not a dedup key:
	// the record id is that, and it travels in event_key. A restart from 0 re-reads the ring and the
	// store merges every one of those reads as a re-pull, which costs nothing and loses nothing.
	Cursor int64
	// Limit bounds one pull. The ring is 2048 records; pulling all of it every 2 seconds would be
	// wasteful but not wrong, so this is throughput, not correctness.
	Limit int
}

type rooterEnvelope struct {
	Cursor    int64          `json:"cursor"`
	Stale     bool           `json:"stale"`
	Dropped   int64          `json:"dropped_total"`
	Available int            `json:"available"`
	BootID    string         `json:"boot_id"`
	Events    []rooterRecord `json:"events"`
}

// rooterRecord mirrors rooter's callRecord. The names are rooter's and the omitempty asymmetry is
// theirs too: a field that is absent in JSON was either zero or never measured, and this schema
// refuses to read that as a measurement of zero.
type rooterRecord struct {
	ID              string  `json:"id"`
	UpstreamID      string  `json:"upstream_id"`
	UpstreamCreated int64   `json:"upstream_created"`
	TS              string  `json:"ts"`
	Path            string  `json:"path"`
	Method          string  `json:"method"`
	Kind            string  `json:"kind"`
	Key             string  `json:"key"`
	KeySource       string  `json:"key_source"`
	Backend         string  `json:"backend"`
	BackendURL      string  `json:"backend_url"`
	Alias           string  `json:"alias"`
	Profile         string  `json:"profile"`
	Version         string  `json:"version"`
	Model           string  `json:"model"`
	ModelRequested  string  `json:"model_requested"`
	Stream          bool    `json:"stream"`
	Status          int     `json:"status"`
	Bytes           int64   `json:"bytes"`
	Aborted         bool    `json:"aborted"`
	Error           string  `json:"error"`
	WaitMs          float64 `json:"wait_ms"`
	TTFTMs          float64 `json:"ttft_ms"`
	// FirstContentMs is rooter's own name for the first content token, and this store keeps the name.
	// The two are measured from one anchor on one observer's clock, which is what makes the difference
	// between them meaningful - and it is also why the difference is computed in the database rather
	// than here, so no arithmetic of mine can end up on a dashboard next to someone else's.
	//
	// A pointer because "absent" is a real state and 0 is not it. Measured across 393 live ring records
	// today: every one carries a non-zero first_content_ms (they are all streamed), so a nil here is a
	// surface that did not measure content at all rather than a call that produced a token instantly.
	FirstContentMs *float64 `json:"first_content_ms"`
	TotalMs        float64  `json:"total_ms"`
	PromptTokens   *int64   `json:"prompt_tokens"`
	// Rooter publishes its own arithmetic answer for the prompt length next to llama.cpp's reported
	// count, because prompt_n is not the whole prompt. Live: prompt_tokens 98060,
	// prompt_tokens_computed 98060, extra.prompt_n 98060. It is stored as its own column and never
	// substituted for the reported number.
	PromptTokensComputed *int64          `json:"prompt_tokens_computed"`
	CompletionTokens     *int64          `json:"completion_tokens"`
	CachedTokens         *int64          `json:"cached_tokens"`
	PrefillMs            *float64        `json:"prefill_ms"`
	PrefillTokS          *float64        `json:"prefill_tok_s"`
	DecodeMs             *float64        `json:"decode_ms"`
	DecodeTokS           *float64        `json:"decode_tok_s"`
	CacheHitRate         *float64        `json:"cache_hit_rate"`
	FinishReason         string          `json:"finish_reason"`
	Extra                json.RawMessage `json:"extra"`
}

// Pull reads everything after the cursor and advances it. An error leaves the cursor where it was,
// so a failed poll is a repeat, not a hole.
func (r *Rooter) Pull(ctx context.Context) ([]store.Event, error) {
	q := url.Values{}
	q.Set("cursor", strconv.FormatInt(r.Cursor, 10))
	if r.Limit > 0 {
		q.Set("limit", strconv.Itoa(r.Limit))
	}
	endpoint := r.URL + "/rooter/events?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	if r.Token != "" {
		req.Header.Set("Authorization", "Bearer "+r.Token)
	}
	client := r.HTTP
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("rooter %s: %w", endpoint, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		// Specific rather than generic, because this exact 404 was the first live failure of this
		// source: the ring exists in rooter's master, and the image running in home-dev predated it.
		// A 404 here means "you are talking to a router too old to have a ring", which is a different
		// fix from "your path is wrong".
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 400))
		return nil, fmt.Errorf("rooter %s: HTTP 404 (%s): this rooter build has no /rooter/events ring; the deployed image predates it", endpoint, body)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("rooter %s: HTTP %d", endpoint, resp.StatusCode)
	}

	var env rooterEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return nil, fmt.Errorf("rooter %s: not the events envelope this source expects: %w", endpoint, err)
	}

	out := make([]store.Event, 0, len(env.Events))
	for _, rec := range env.Events {
		out = append(out, r.event(rec))
	}
	if env.Cursor > r.Cursor {
		r.Cursor = env.Cursor
	}
	return out, nil
}

func (r *Rooter) event(rec rooterRecord) store.Event {
	ev := store.Event{
		EventKey:       "rooter|" + rec.ID,
		Source:         "rooter",
		SourceIDs:      []string{"rooter|" + rec.ID},
		Stream:         &rec.Stream,
		Aborted:        &rec.Aborted,
		Status:         &rec.Status,
		Bytes:          &rec.Bytes,
		WaitMs:         &rec.WaitMs,
		TTFTMs:         &rec.TTFTMs,
		FirstContentMs: rec.FirstContentMs,
		TotalMs:        &rec.TotalMs,
	}
	if s, err := time.Parse(time.RFC3339Nano, rec.TS); err == nil {
		ev.TS = s.UTC()
	} else {
		// A record whose timestamp will not parse still gets kept, stamped with the time this probe
		// read it and flagged as such. Dropping it would make a router bug look like a missing call.
		ev.TS = time.Now().UTC()
		ev.TSIsObservation = true
	}
	for _, f := range []struct {
		dst **string
		val string
	}{
		{&ev.ID, rec.ID}, {&ev.UpstreamID, rec.UpstreamID}, {&ev.Backend, rec.Backend},
		{&ev.BackendURL, rec.BackendURL}, {&ev.Alias, rec.Alias}, {&ev.Profile, rec.Profile},
		{&ev.Version, rec.Version}, {&ev.Model, rec.Model}, {&ev.ModelRequested, rec.ModelRequested},
		{&ev.Kind, rec.Kind}, {&ev.Key, rec.Key}, {&ev.KeySource, rec.KeySource},
		{&ev.Path, rec.Path}, {&ev.Method, rec.Method}, {&ev.FinishReason, rec.FinishReason},
	} {
		// An empty string from rooter is "this call had no such thing", not a fact to store: an empty
		// backend on a 401 the router answered itself is the honest shape of that event.
		if v := strip(f.val); v != "" {
			*f.dst = &v
		}
	}
	if rec.UpstreamCreated > 0 {
		// Kept in extra, not promoted: it is the runner's clock at one-second granularity, and a
		// column that looked like a timestamp would be read as one. It is the join aid, so it has to
		// survive the merge.
		if ev.Extra == nil {
			ev.Extra = map[string]any{}
		}
		ev.Extra["upstream_created"] = rec.UpstreamCreated
	}
	ev.PromptTokens = rec.PromptTokens
	ev.PromptTokensComputed = rec.PromptTokensComputed
	ev.CompletionTokens = rec.CompletionTokens
	ev.CachedTokens = rec.CachedTokens
	// rooter calls it prefill_ms, strata calls it prompt_ms, and the schema has one column because it
	// is one quantity: the time the engine spent reading the prompt. Two columns of the same number
	// under two names is how a report ends up averaging one half of the calls.
	ev.PromptMs, ev.PrefillTokS = rec.PrefillMs, rec.PrefillTokS
	ev.DecodeMs, ev.DecodeTokS = rec.DecodeMs, rec.DecodeTokS
	if rec.CacheHitRate != nil {
		// Rooter reports a rate it got from somewhere else. The store derives its own rates from
		// components and rejects a supplied rate that disagrees, so this is NOT sent as a rate; it is
		// kept as evidence of what rooter believed.
		if ev.Extra == nil {
			ev.Extra = map[string]any{}
		}
		ev.Extra["rooter_cache_hit_rate"] = *rec.CacheHitRate
	}
	if rec.Error != "" {
		ev.Error = map[string]any{"message": rec.Error}
	}
	if len(rec.Extra) > 0 {
		var raw any
		if err := json.Unmarshal(rec.Extra, &raw); err == nil {
			if ev.Extra == nil {
				ev.Extra = map[string]any{}
			}
			ev.Extra["raw"] = raw
		}
	}
	// No UsageSource. rooter's live ring records carry no provenance field at all (checked against the
	// deployed payload: no usage_source key in any event), and rooter's own contract notes say the
	// vocabulary it would report is 'none' or 'client' - never an injected value, because injecting
	// include_usage turned out to be a no-op on llama.cpp: the usage tail arrives whether or not the
	// request asked for it. Guessing here would put a false provenance on the one field this schema
	// exists to keep honest, so the column default 'none' reports what is actually known: nobody said.
	return ev
}

func strip(s string) string {
	if len(s) > 512 {
		return s[:512]
	}
	return s
}
