// Package store writes model-call events into the modelcalls store.
//
// One row per call, not one row per sighting. A call can be seen by the router that routed it and
// by the engine that ran it, and those two surfaces describe the same event with different facts
// at different times. A row per sighting would double-count every call that was lucky enough to be
// well observed, which is the exact opposite of what a measurement should do.
//
// So the whole package is one question, asked in a fixed order: have I already seen this call?
//
//	1. same event_key                  -> this source is re-reporting. Merge, and do NOT count it
//	                                      as a second witness.
//	2. same upstream_id, other source  -> the join. Merge, and record the key that made it a join.
//	3. narrow heuristic, one candidate -> merge, and name the method so it is auditable.
//	4. none of the above               -> insert.
//
// All four paths run through ONE SQL statement, because they all end up merging a fact-set into a
// row that either exists or does not. The difference is only which event_key is named and whether
// join_method is overridden.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// UsageSource names the mechanism that produced the token counts, not the component that used it.
// The vocabulary is shared with the schema's CHECK constraint, and it is shared exactly: when the
// two drift, inserts fail with a check violation, which is the loudest possible way of saying that
// a Go constant and a database contract have stopped meaning the same thing.
const (
	UsageNone         = "none"          // nobody knows, and nobody will
	UsageClient       = "client"        // the client's own request asked for usage
	UsageInjected     = "injected"      // the observer added include_usage=true to make it appear
	UsageRunnerScrape = "runner_scrape" // the runner's own metrics or log said it
)

// FallbackWindow bounds the heuristic in path 3. It is not a tuning knob; widening it increases
// the rate at which two unrelated calls from the same session get silently merged into one row,
// which is a failure that no query will ever reveal.
const FallbackWindow = 2 * time.Second

// Event is one surface's description of one call. Every field is optional except the identity
// fields, because the surfaces genuinely differ in what they can know: an abandoned stream has a
// status and a byte count and no token count, and a log line with no clock has a model and a rate
// and no timestamp.
type Event struct {
	// EventKey is this source's idempotency key, namespaced by source: "rooter|r-1001",
	// "strata|chatcmpl-aaa", "sushi|<sha256 of the canonical fact set>".
	EventKey string
	// Source is who reported it: rooter, strata, sushi.
	Source string
	// SourceIDs are the ids that contributed to this row. On a merge it is the union, and it is
	// provenance: one row whose facts came from two surfaces must be able to say so.
	SourceIDs []string

	// ID is the identifier THIS source saw. UpstreamID is the runner's own id (chatcmpl-...),
	// which is the only cross-surface join key that means the same thing to both sides.
	ID         *string
	UpstreamID *string

	TS time.Time
	// TSIsObservation is true when ts is "when the probe read this", not "when the call ran".
	// Sushi's log lines have no timestamp; pretending otherwise would put invented precision on a
	// graph that people make decisions from.
	TSIsObservation bool

	// Backend is the runner family that answered. nil means the call never reached a backend - a
	// 401 the router answered itself is an event with no backend.
	Backend     *string
	BackendURL  *string
	Alias       *string
	Profile     *string
	Version     *string
	Engine      *string
	Model       *string
	ModelRequested *string

	Kind     *string // gen | spill | rebind; nil is the normal case for a call
	Key      *string // caller identity: token label or client name, never the token itself
	KeySource *string
	Path     *string
	Method   *string
	Stream   *bool
	Status   *int
	Bytes    *int64
	Aborted  *bool

	Error map[string]any

	WaitMs *float64
	TTFTMs *float64
	TotalMs *float64
	PromptMs *float64
	DecodeMs *float64

	PromptTokens       *int64
	CompletionTokens   *int64
	CachedTokens       *int64
	EngineCachedTokens *int64

	PrefillTokS  *float64
	DecodeTokS   *float64
	CacheHitRate *float64

	UsageSource  string
	FinishReason *string

	DraftsOffered  *int64
	DraftsAccepted *int64

	// Extra is what this source said, at its own top level: {"hit_rate": 0.802}, not
	// {"strata": {"hit_rate": 0.802}}. The store namespaces it under Source on the way in, which
	// is what keeps two surfaces describing one call from overwriting each other's evidence.
	// Strata's hit_rate ends up at extra.strata.hit_rate, where it can be read without anyone
	// having to guess which surface a number came from - and where it stays unexplained, because
	// nobody has established what it counts and an unexplained number promoted to a column becomes
	// a dashboard legend that lies.
	Extra map[string]any

	// JoinMethod is set by the store, not the caller: self | upstream_id | backend_key_window.
	JoinMethod string
}

// Result reports which path the write took. Returning it is not a nicety: a harvester that cannot
// tell a join from an insert cannot report how much of its own data came from a heuristic.
type Result struct {
	EventKey    string
	Path        string
	JoinMethod  string
	SourceCount int
	HasUsage    bool
}

const (
	PathRePull        = "re_pull"        // same event_key, merged
	PathJoinUpstream  = "join_upstream"  // collided on the runner's id, merged
	PathJoinFallback  = "join_fallback"  // merged by the narrow heuristic
	PathInsert        = "insert"         // genuinely new
)

type Store struct {
	db *sql.DB
}

func Open(ctx context.Context, dsn string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open modelcalls: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(10 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping modelcalls: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// DB exposes the pool for reads. It is not a convenience: a harvester that hides its database
// makes every query a new exercise in guessing where the truth lives, and the dashboards here are
// meant to be able to say how much of what they show came from a heuristic.
func (s *Store) DB() *sql.DB { return s.db }

// Migrate applies a migration file. It is deliberately a single blob executed in one round trip:
// the migration is a contract, and applying half of it is worse than applying none of it.
func (s *Store) Migrate(ctx context.Context, sqlBytes []byte) error {
	_, err := s.db.ExecContext(ctx, string(sqlBytes))
	return err
}

// Write merges one event into the store and reports how it landed.
func (s *Store) Write(ctx context.Context, ev Event) (Result, error) {
	if err := validate(ev); err != nil {
		return Result{}, err
	}

	// A concurrent harvester can land a row between our lookup and our write. The unique indexes
	// catch it; retrying re-derives the path instead of guessing. Bounded, because a store that
	// keeps colliding with itself is telling us something we should hear.
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		res, err := s.writeOnce(ctx, ev)
		if err == nil {
			return res, nil
		}
		// Either a unique violation (another writer landed the row mid-flight) or racedMerge (our
		// lookup and our write disagreed about what existed). Both are retried because the answer is
		// the same: re-derive the path from the current state instead of acting on a stale read.
		if !isUniqueViolation(err) && !errors.Is(err, errRaced) {
			return Result{}, err
		}
		lastErr = err
	}
	return Result{}, fmt.Errorf("write %s: colliding with another writer on every attempt: %w", ev.EventKey, lastErr)
}

func (s *Store) writeOnce(ctx context.Context, ev Event) (Result, error) {
	key := ev.EventKey
	var joinOverride *string

	// 1. This exact source, this exact id. A re-pull after a cursor reconnect. Not a new witness,
	//    and treating it as one would inflate the one number that says how well observed a call was.
	exists, err := s.existsEventKey(ctx, key)
	if err != nil {
		return Result{}, err
	}

	if exists {
		return s.upsert(ctx, ev, key, nil, PathRePull)
	}

	// 2. A different source that knows the runner's own id for this call. In Postgres this is the
	//    partial UNIQUE index on upstream_id throwing; here it is a lookup that finds the row the
	//    index would have collided with. Same rule, checked instead of crashed into.
	if id := deref(ev.UpstreamID); id != "" {
		found, err := s.lookupUpstream(ctx, id)
		if err != nil {
			return Result{}, err
		}
		if found != "" {
			joinOverride = ptr("upstream_id")
			return s.upsert(ctx, ev, found, joinOverride, PathJoinUpstream)
		}
	}

	// 3. The heuristic, and only where the heuristic is honest. See lookupFallback: it returns an
	//    empty key unless exactly one candidate qualifies.
	candidate, err := s.lookupFallback(ctx, ev)
	if err != nil {
		return Result{}, err
	}
	if candidate != "" {
		joinOverride = ptr("backend_key_window")
		return s.upsert(ctx, ev, candidate, joinOverride, PathJoinFallback)
	}

	// 4. New.
	return s.upsert(ctx, ev, key, nil, PathInsert)
}

// upsert is the whole write. One statement for every path: merge a fact-set into the row named by
// key, or create that row if nothing has that key.
//
// The merge rule, stated once, applied everywhere: a fact that is now known fills a gap; a fact
// that is not known must not overwrite a fact that was. A NULL from a surface that never had the
// information is absence, not evidence, and COALESCE is what makes the difference show up in the
// data instead of in a bug report.
const upsertSQL = `
INSERT INTO call_events (
	event_key, source, source_ids, id, upstream_id, ts, ts_is_observation,
	backend, backend_url, alias, profile, version, engine, model, model_requested,
	kind, "key", key_source, path, method, stream, status, bytes, aborted, error,
	wait_ms, ttft_ms, total_ms, prompt_ms, decode_ms,
	prompt_tokens, completion_tokens, cached_tokens, engine_cached_tokens,
	prefill_tok_s, decode_tok_s, cache_hit_rate,
	usage_source, finish_reason, drafts_offered, drafts_accepted, extra, join_method
) VALUES (
	$1, $2, $3, $4, $5, $6, $7,
	$8, $9, $10, $11, $12, $13, $14, $15,
	$16, $17, $18, $19, $20, $21, $22, $23, $24, $25,
	$26, $27, $28, $29, $30,
	$31, $32, $33, $34,
	$35, $36, $37,
	$38, $39, $40, $41, jsonb_build_object($2::text, $42::jsonb), COALESCE($43::text, 'self')
)
ON CONFLICT (event_key) DO UPDATE SET
	id                   = COALESCE(call_events.id, EXCLUDED.id),
	upstream_id          = COALESCE(call_events.upstream_id, EXCLUDED.upstream_id),
	backend              = COALESCE(call_events.backend, EXCLUDED.backend),
	backend_url          = COALESCE(call_events.backend_url, EXCLUDED.backend_url),
	alias                = COALESCE(call_events.alias, EXCLUDED.alias),
	profile              = COALESCE(call_events.profile, EXCLUDED.profile),
	version              = COALESCE(call_events.version, EXCLUDED.version),
	engine               = COALESCE(call_events.engine, EXCLUDED.engine),
	model                = COALESCE(call_events.model, EXCLUDED.model),
	model_requested      = COALESCE(call_events.model_requested, EXCLUDED.model_requested),
	kind                 = COALESCE(call_events.kind, EXCLUDED.kind),
	"key"                = COALESCE(call_events."key", EXCLUDED."key"),
	key_source           = COALESCE(call_events.key_source, EXCLUDED.key_source),
	path                 = COALESCE(call_events.path, EXCLUDED.path),
	method               = COALESCE(call_events.method, EXCLUDED.method),
	stream               = COALESCE(call_events.stream, EXCLUDED.stream),
	status               = COALESCE(call_events.status, EXCLUDED.status),
	bytes                = COALESCE(call_events.bytes, EXCLUDED.bytes),
	-- Abandonment is sticky. One surface that saw the client hang up is enough; a surface that did
	-- not notice must not undo it. And a call can be abandoned by the client while the engine still
	-- finished it, which is why aborted and usage coexist.
	aborted              = call_events.aborted OR EXCLUDED.aborted,
	error                = COALESCE(call_events.error, EXCLUDED.error),
	wait_ms              = COALESCE(call_events.wait_ms, EXCLUDED.wait_ms),
	ttft_ms              = COALESCE(call_events.ttft_ms, EXCLUDED.ttft_ms),
	total_ms             = COALESCE(call_events.total_ms, EXCLUDED.total_ms),
	prompt_ms            = COALESCE(call_events.prompt_ms, EXCLUDED.prompt_ms),
	decode_ms            = COALESCE(call_events.decode_ms, EXCLUDED.decode_ms),
	prompt_tokens        = COALESCE(call_events.prompt_tokens, EXCLUDED.prompt_tokens),
	completion_tokens    = COALESCE(call_events.completion_tokens, EXCLUDED.completion_tokens),
	cached_tokens        = COALESCE(call_events.cached_tokens, EXCLUDED.cached_tokens),
	engine_cached_tokens = COALESCE(call_events.engine_cached_tokens, EXCLUDED.engine_cached_tokens),
	prefill_tok_s        = COALESCE(call_events.prefill_tok_s, EXCLUDED.prefill_tok_s),
	decode_tok_s         = COALESCE(call_events.decode_tok_s, EXCLUDED.decode_tok_s),
	cache_hit_rate       = COALESCE(call_events.cache_hit_rate, EXCLUDED.cache_hit_rate),
	finish_reason        = COALESCE(call_events.finish_reason, EXCLUDED.finish_reason),
	drafts_offered       = COALESCE(call_events.drafts_offered, EXCLUDED.drafts_offered),
	drafts_accepted      = COALESCE(call_events.drafts_accepted, EXCLUDED.drafts_accepted),
	-- A real timestamp beats an observed one, in both directions: an envelope stamped when it was
	-- read is upgraded when a surface that knows the actual time turns up, and a call that already
	-- has a true ts is never downgraded by a clock-less surface describing it.
	ts                   = CASE
		WHEN call_events.ts_is_observation AND NOT EXCLUDED.ts_is_observation THEN EXCLUDED.ts
		ELSE call_events.ts
		END,
	ts_is_observation    = call_events.ts_is_observation AND EXCLUDED.ts_is_observation,
	-- The more informative provenance wins: 'none' is replaced by anything, and a value that says
	-- how the numbers were obtained is never replaced by one that says less.
	usage_source         = CASE
		WHEN call_events.usage_source = 'none' THEN EXCLUDED.usage_source
		ELSE call_events.usage_source
		END,
	-- Union only when the merge joins a DIFFERENT source (the override is what marks that). A
	-- source re-reporting its own id must not grow the list that answers "how well observed was
	-- this call", or the store starts flattering itself.
	source_ids           = CASE
		WHEN $43::text IS NULL THEN call_events.source_ids
		ELSE ARRAY(SELECT DISTINCT unnest(call_events.source_ids || EXCLUDED.source_ids))
		END,
	join_method          = COALESCE($43::text, call_events.join_method),
	-- Append, not replace, and namespace by source. A caller's Extra is what THIS source said, and
	-- the store wraps it under the source name. Without that wrapping two surfaces that both call
	-- their payload "raw" overwrite each other on the merge, and the evidence of who said what
	-- silently becomes the evidence of whoever wrote last. Enforcing it here is the point; a
	-- convention every writer has to remember is not a convention.
	extra                = call_events.extra || EXCLUDED.extra,
	last_seen_at         = now(),
	updated_at           = now()
RETURNING event_key, (xmax = 0) AS inserted, join_method, source_count, has_usage
`

func (s *Store) upsert(ctx context.Context, ev Event, key string, joinOverride *string, path string) (Result, error) {
	extra, err := jsonOrEmpty(ev.Extra)
	if err != nil {
		return Result{}, fmt.Errorf("encode extra: %w", err)
	}
	errJSON, err := jsonOrNull(ev.Error)
	if err != nil {
		return Result{}, fmt.Errorf("encode error: %w", err)
	}

	sourceIDs := ev.SourceIDs
	if len(sourceIDs) == 0 {
		sourceIDs = []string{ev.EventKey}
	}

	var res Result
	var inserted bool
	row := s.db.QueryRowContext(ctx, upsertSQL,
		key, ev.Source, pq.StringArray(sourceIDs),
		nullableString(ev.ID), nullableString(ev.UpstreamID), ev.TS, ev.TSIsObservation,
		nullableString(ev.Backend), nullableString(ev.BackendURL), nullableString(ev.Alias), nullableString(ev.Profile),
		nullableString(ev.Version), nullableString(ev.Engine), nullableString(ev.Model), nullableString(ev.ModelRequested),
		nullableString(ev.Kind), nullableString(ev.Key), nullableString(ev.KeySource), nullableString(ev.Path), nullableString(ev.Method),
		derefOpt(ev.Stream), derefOpt(ev.Status), derefOpt(ev.Bytes), boolOrDefault(ev.Aborted, false), errJSON,
		derefOpt(ev.WaitMs), derefOpt(ev.TTFTMs), derefOpt(ev.TotalMs), derefOpt(ev.PromptMs), derefOpt(ev.DecodeMs),
		derefOpt(ev.PromptTokens), derefOpt(ev.CompletionTokens), derefOpt(ev.CachedTokens), derefOpt(ev.EngineCachedTokens),
		derefOpt(ev.PrefillTokS), derefOpt(ev.DecodeTokS), derefOpt(ev.CacheHitRate),
		usageSourceOrDefault(ev.UsageSource), nullableString(ev.FinishReason), derefOpt(ev.DraftsOffered), derefOpt(ev.DraftsAccepted),
		extra, nullableString(joinOverride),
	)
	if err := row.Scan(&res.EventKey, &inserted, &res.JoinMethod, &res.SourceCount, &res.HasUsage); err != nil {
		return Result{}, fmt.Errorf("write %s: %w", key, err)
	}
	res.Path = path

	// The store checking itself against the database's answer. (xmax = 0) says whether Postgres
	// inserted or merged; the path says what our lookups concluded. If those disagree, either a
	// concurrent writer moved underneath us or a lookup is wrong, and in both cases the honest move
	// is to say so and re-derive - not to report a join that did not happen.
	switch {
	case path == PathInsert && !inserted:
		return res, fmt.Errorf("%w: path=%s but Postgres merged into an existing row (concurrent insert between lookup and write)", errRaced, path)
	case path != PathInsert && inserted:
		return res, fmt.Errorf("%w: path=%s but Postgres inserted a new row for key %s (the row our lookup found is gone, or the lookup is wrong)", errRaced, path, key)
	}
	return res, nil
}

// errRaced marks "the state changed under us". Retrying is usually enough; reporting it is not
// optional, because silently merging into a row that appeared a microsecond ago is how a store
// starts attributing facts to the wrong call.
var errRaced = errors.New("raced with another write")

// existsEventKey answers "is this the same surface reporting the same id again".
func (s *Store) existsEventKey(ctx context.Context, key string) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM call_events WHERE event_key = $1 LIMIT 1`, key).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lookup event_key %s: %w", key, err)
	}
	return true, nil
}

// lookupUpstream finds the row that the partial UNIQUE index on upstream_id would have collided
// with. Checking instead of crashing is the same rule made visible: it lets the write report that a
// join happened, which is the difference between a store and a pile of inserts.
func (s *Store) lookupUpstream(ctx context.Context, upstreamID string) (string, error) {
	var key string
	err := s.db.QueryRowContext(ctx, `
		SELECT event_key FROM call_events
		 WHERE upstream_id = $1
		 ORDER BY ts DESC
		 LIMIT 1`, upstreamID).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("lookup upstream_id %s: %w", upstreamID, err)
	}
	return key, nil
}

// lookupFallback is the only path here that guesses, so it is deliberately narrow and it names
// itself in join_method.
//
// It exists for one specific hole: a call the router knows about but cannot identify, because the
// runner's chatcmpl id lived in the part of the response that never arrived. The engine that
// finished the work has its own record, and without a fallback the two surfaces produce two rows
// for one call - which is not missing data, it is fabricated data.
//
// The predicate requires all of: same backend, same session key, a timestamp within
// FallbackWindow, a row that has no upstream_id (so nothing better can be done), and a row that
// has no usage yet (so the merge is filling a hole and not overwriting a measurement). And exactly
// one candidate: two candidates is a coin flip, and a coin flip is not a join. Ambiguity is left
// as two unjoined rows that call_quality then reports as unjoined, which is a visible weakness
// rather than an invisible one.
func (s *Store) lookupFallback(ctx context.Context, ev Event) (string, error) {
	if deref(ev.Backend) == "" || deref(ev.Key) == "" {
		return "", nil
	}
	// Only worth doing when this event brings something the envelope cannot produce.
	if derefOpt(ev.PromptTokens) == nil && derefOpt(ev.CompletionTokens) == nil &&
		derefOpt(ev.EngineCachedTokens) == nil && derefOpt(ev.DecodeTokS) == nil {
		return "", nil
	}

	const q = `
		SELECT event_key FROM call_events
		 WHERE backend = $1
		   AND "key" = $2
		   AND upstream_id IS NULL
		   AND NOT has_usage
		   AND abs(extract(epoch from ts) - extract(epoch from $3::timestamptz)) <= $4
		 ORDER BY ts`
	rows, err := s.db.QueryContext(ctx, q, *ev.Backend, *ev.Key, ev.TS, FallbackWindow.Seconds())
	if err != nil {
		return "", fmt.Errorf("fallback lookup: %w", err)
	}
	defer rows.Close()

	var (
		candidates []string
		key        string
	)
	for rows.Next() {
		if err := rows.Scan(&key); err != nil {
			return "", fmt.Errorf("fallback lookup scan: %w", err)
		}
		candidates = append(candidates, key)
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("fallback lookup rows: %w", err)
	}
	if len(candidates) != 1 {
		return "", nil
	}
	return candidates[0], nil
}

func validate(ev Event) error {
	if ev.EventKey == "" {
		return errors.New("event: EventKey is required (it is the idempotency key)")
	}
	if ev.Source == "" {
		return errors.New("event: Source is required (rooter | strata | sushi); without it a merge cannot say who said what")
	}
	if ev.TS.IsZero() {
		return errors.New("event: TS is required - set TSIsObservation if it is really an observation time")
	}
	return nil
}

func isUniqueViolation(err error) bool {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return pqErr.Code == "23505"
	}
	return false
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// nullableString sends NULL when a surface said nothing, which is not the same as sending an empty
// string. It matters structurally, not cosmetically: path 3 looks for rows with upstream_id IS
// NULL, and an empty string written where absence was meant would make every router-side row look
// already-joined, quietly disabling the fallback for exactly the abandoned calls it exists for.
func nullableString(s *string) any {
	if s == nil || *s == "" {
		return nil
	}
	return *s
}

func ptr(s string) *string { return &s }

func derefOpt[T any](v *T) any {
	if v == nil {
		return nil
	}
	return *v
}

func boolOrDefault(v *bool, def bool) bool {
	if v == nil {
		return def
	}
	return *v
}

func usageSourceOrDefault(s string) string {
	if s == "" {
		return UsageNone
	}
	return s
}

func jsonOrNull(m map[string]any) (any, error) {
	if len(m) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

func jsonOrEmpty(m map[string]any) (any, error) {
	if len(m) == 0 {
		return "{}", nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}


