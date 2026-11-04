-- legacy_schema_snapshot.sql — a FROZEN copy of what an installed modelcalls database looked like at
-- the first commit that shipped a schema (431c17e, "modelcalls: the store, and the contract that keeps
-- it honest"), kept verbatim in git and never edited.
--
-- It exists because migrations have two paths and CI only ever exercises the easy one. A fresh database
-- runs 001 once, top to bottom, and everything in it is CREATE. A database that has been running for a
-- week is a different animal: 001 runs AGAIN on top of tables that already have rows, views that already
-- have columns, and CHECK constraints that already have names - and Postgres is strict there in ways it
-- is not on a fresh install. `CREATE OR REPLACE VIEW` will add columns only at the END of an existing
-- view, and a CHECK constraint cannot be swapped while rows still violate it.
--
-- Measured, not predicted: putting the thinking-gap columns in the middle of the `calls` view select list
-- failed on a database that had been in use since the day before, with "cannot change name of view column
-- \"prompt_ms\" to \"first_content_ms\"", and the failure landed at process start, because the harvester
-- migrates on boot.
--
-- This file is applied by test/legacy_upgrade_path.sql, so the upgrade path is a lane step rather than
-- something somebody remembers to do by hand before a deploy. Do not "fix" anything in here: its value
-- is that it is the old shape, warts included.
-- 001_initial.sql — the call_events contract.
--
-- What this table is: one row per *call* that any of our surfaces can prove happened. Events,
-- not samples. Nothing here is an average, a histogram bucket, or a rollup; those are queries.
-- A row is written the first time any source reports the call and filled in afterwards by
-- whichever other source learns more about it.
--
-- Field names are shared with rooter (rooter's logLine()/Outcome/requestInfo vocabulary) on
-- purpose. Two repos inventing two vocabularies for the same facts is how joins rot.
--
-- The two rules everything else follows from:
--
--   1. NULL means "never learned", never "zero". Every timing, token count and rate is nullable
--      for a real reason: rooter forwards a stream without buffering it, so a client that hangs
--      up mid-stream leaves no usage and no timings in existence, ever. Those calls are still
--      events - the envelope (who, which backend, which model, what status, how many bytes) is
--      known and useful. Required columns would have made them unrecordable, which is how a
--      store ends up silently measuring only the successful calls.
--
--   2. `has_usage` says whether an aggregate may touch this row. Any average of tok/s or ms that
--      does not filter on it is dividing by a denominator that includes rows where the number
--      never existed. That is the same class of error as a counter that never decrements: the
--      number stays plausible and is wrong.
--
-- Dedup and the join are the same mechanism. See call_events_upstream_key below.

BEGIN;

CREATE TABLE IF NOT EXISTS call_events (
	-- ─── identity and idempotency ──────────────────────────────────────────────────────
	--
	-- event_key is the idempotency key of the source that first reported the call, formatted
	-- '<source>|<native id>': 'rooter|<rooter event id>', 'strata|<requests[].id>',
	-- 'sushi|<sha256 of the canonical log fact set>'. Every poll re-reports recent calls, so a
	-- key that changes per poll would duplicate everything.
	--
	-- rooter's event ids, not timestamps, carry this. Its event ring is in memory and resets on
	-- pod restart, so cursor continuity is an optimisation; the UNIQUE on this key is the actual
	-- correctness guarantee.
	event_key text PRIMARY KEY,

	-- Every '<source>|<native id>' that confirmed this row, merged rather than replaced, so
	-- provenance survives the join and nothing has to be trusted to a single surface.
	source_ids text[] NOT NULL DEFAULT '{}',

	-- The two native ids of the two surfaces that can describe one call, kept in their own columns
	-- rather than only inside source_ids, because they are join keys and a join key has to be
	-- indexable on its own.
	--
	--   id          - rooter's event id. NULL for a call that never went through a router.
	--   upstream_id - the runner's chatcmpl id, present in the response body and echoed in
	--                 strata's requests[]. NULL when the call was rejected before the runner ever
	--                 answered (401, 429, no backend) - which is most of what a router knows that
	--                 no runner ever will.
	--
	-- Both are text and both are nullable: they are foreign identifiers, and inventing a placeholder
	-- for "unknown" is how a join silently matches on the placeholder.
	id text,
	upstream_id text,

	-- How many surfaces agree this call happened. 1 is normal (a direct-to-runner call seen by
	-- one source); 2 means it was routed and both the router and the runner reported it.
	source_count int GENERATED ALWAYS AS (coalesce(array_length(source_ids, 1), 0)) STORED,

	-- How this row came to represent the call it represents:
	--   self                 - the only source that saw it.
	--   upstream_id          - joined to an existing row by the runner's chatcmpl id.
	--   backend_key_window   - fallback join: same backend, same session key, nearest ts.
	-- Recorded rather than inferred at query time, because the fallback join is the part of this
	-- design that rests on an assumption, and a dashboard must be able to show how much of what
	-- it displays depends on it.
	join_method text NOT NULL DEFAULT 'self'
		check (join_method in ('self', 'upstream_id', 'backend_key_window')),

	-- ─── envelope: what a router knows even when the model never finished ───────────────
	--
	-- 'rooter' | 'strata' | 'sushi' | 'probe'. Which surface wrote the row first.
	source text NOT NULL,

	ts timestamptz NOT NULL,

	-- The honest version of "when did this happen" for surfaces with no clock. Sushi's log lines
	-- carry no timestamp, so ts there is the instant the probe read the line. Set true for those
	-- rows and they can be excluded from latency work instead of quietly bending it.
	ts_is_observation boolean NOT NULL DEFAULT false,

	path text,
	method text,

	-- rooter's routing enum: read | gen | spill | rebind. NULL for a call that never reached a
	-- router (a direct hit on a runner), which is why it is not constrained to NOT NULL.
	kind text check (kind is null or kind in ('read', 'gen', 'spill', 'rebind')),

	-- Session axis: rooter's short-form key and where it came from (header, path, default).
	-- Deliberately never a histogram label anywhere upstream: per-session cardinality would blow
	-- up every series. It belongs in a store, not in an index-friendly label set.
	"key" text,
	key_source text,

	-- The runner family that answered: 'strata', 'sushi'. NULL means the call never reached a
	-- backend at all - a 401 or 429 that rooter answered itself is an event with no backend, and
	-- inventing a sentinel string for it would make "which backend is unhealthy?" answerable by
	-- a made-up value.
	backend text,
	backend_url text,

	-- model is what actually answered and, for anything routed, only rooter knows it - runners
	-- echo model:"auto" back verbatim. model_requested is what the client asked for. Keeping both
	-- is the only way to see what "auto" chose.
	model text,
	model_requested text,

	-- Runner-side axes, never supplied by the router:
	--   alias  - runner family, e.g. 'strata', 'sushi'.
	--   profile- config variant, e.g. 'iq3_xxs', '2.6bpw', 'STQ-2.75bpw'.
	--   engine - the runner's own build string (strata engine 0.1.51).
	--   version - runner version, authoritative only where a runner states it (sushi's log banner
	--             "sushi 1.1.1 (MLX 0.32.3)"). No /version endpoint exists on either runner and
	--             rooter does not invent one.
	alias text,
	profile text,
	engine text,
	version text,

	stream boolean,
	status integer,
	bytes integer,

	-- Abandonment used to be invisible everywhere: rooter's inflight counter never decremented on
	-- an abandoned stream and byStatus never counted it. This column is the record that a call
	-- exists which no counter accounted for.
	aborted boolean NOT NULL DEFAULT false,

	error text,

	-- ─── timings: NULL when the fact never existed ─────────────────────────────────────
	--
	-- wait_ms is queue wait, known to the router, invisible to the client. prompt_ms and decode_ms
	-- are the runner's split of prefill and generation; ttft_ms and total_ms are what was seen on
	-- the wire. They are kept separate on purpose - router-side TTFT includes queue wait and
	-- proxy hops, runner-side TTFT does not, and collapsing them hides exactly the difference you
	-- go looking for when a dashboard says "the model got slower".
	wait_ms double precision check (wait_ms is null or wait_ms >= 0),
	ttft_ms double precision check (ttft_ms is null or ttft_ms >= 0),
	prompt_ms double precision check (prompt_ms is null or prompt_ms >= 0),
	decode_ms double precision check (decode_ms is null or decode_ms >= 0),
	total_ms double precision check (total_ms is null or total_ms >= 0),

	-- ─── tokens: NULL when not reported ────────────────────────────────────────────────
	--
	-- A streaming response with no stream_options.include_usage carries no usage at all. rooter
	-- injects include_usage into streaming requests it forwards so the tail is parseable, which
	-- is a request-body mutation and the one place rooter stops being transparent - so the row
	-- records whose usage this is rather than assuming.
	prompt_tokens integer check (prompt_tokens is null or prompt_tokens >= 0),
	completion_tokens integer check (completion_tokens is null or completion_tokens >= 0),

	-- cached_tokens is the wire/usage view (what the runner billed as cached). engine_cached_tokens
	-- is the runner's own internal view of the same thing (strata's "reused"). They are different
	-- measurements of one concept and both are kept, because the gap between them is a fact about
	-- the runner and not noise to be averaged away.
	cached_tokens integer check (cached_tokens is null or cached_tokens >= 0),
	engine_cached_tokens integer check (engine_cached_tokens is null or engine_cached_tokens >= 0),

	drafts_offered integer check (drafts_offered is null or drafts_offered >= 0),
	drafts_accepted integer check (drafts_accepted is null or drafts_accepted >= 0),
	reasoning_recoveries integer check (reasoning_recoveries is null or reasoning_recoveries >= 0),

	-- ─── rates: derived, and derived from the stored components ─────────────────────────
	--
	-- prefill_tok_s / decode_tok_s / cache_hit_rate are stored rather than computed on read only
	-- because the surface that computed them may have been the only one that ever knew the
	-- components.
	--
	-- cache_hit_rate has ONE definition here, and it is measured rather than assumed:
	--     cache_hit_rate = cached_tokens / prompt_tokens
	-- It is a token ratio and nothing else. Strata's own `hit_rate` is NOT this ratio - measured
	-- live across its 12 retained requests, reused/prompt_tokens sits at 0.992-0.9996 while
	-- hit_rate sits at 0.802-0.903, and not one of the twelve agrees within 0.02. Strata runs an
	-- MoE expert cache (expert_slots, expert_cache_mib), which is the obvious candidate for what
	-- its hit_rate actually counts, but it is not established, so it does not get a column: an
	-- unknown metric promoted to a first-class column becomes a dashboard legend that lies. It is
	-- preserved verbatim in `extra` under the runner's own name until the meaning is known.
	--
	-- The components stay in the row so a definition change is a recompute and not a migration.
	prefill_tok_s double precision check (prefill_tok_s is null or prefill_tok_s >= 0),
	decode_tok_s double precision check (decode_tok_s is null or decode_tok_s >= 0),
	cache_hit_rate double precision check (cache_hit_rate is null or (cache_hit_rate >= 0 and cache_hit_rate <= 1)),

	-- usage_source names the MECHANISM that produced the numbers, not the component that happened
	-- to use it. A vocabulary of components rots the moment a second component learns the same
	-- trick, and "how much of my usage data was volunteered versus manufactured?" is a question this
	-- table has to keep answering.
	--   'none'         - nobody knows, and nobody will: the stream was abandoned, or the runner
	--                    never reports usage for this shape of request.
	--   'client'       - the client's own request asked for usage, so it arrived in the response.
	--   'injected'     - the observer mutated the request (include_usage=true) to make a usage
	--                    frame appear. Honest, but it is measurement affecting the measurement.
	--   'runner_scrape' - the runner's own metrics or log surface said it, independent of this
	--                    response entirely.
	-- There is deliberately no 'wire': every usage frame arrives over the wire, so the value would
	-- be unfalsifiable, and an unfalsifiable provenance value is worse than no provenance value.
	usage_source text NOT NULL DEFAULT 'none' check (usage_source in
		('client', 'injected', 'none', 'runner_scrape')),

	-- Whether this row may be included in an aggregate over tokens or rates. Generated, so no
	-- writer can disagree with it.
	has_usage boolean GENERATED ALWAYS AS (prompt_tokens is not null and completion_tokens is not null) STORED,

	finish_reason text,

	-- ─── engine internals: stays with the runner ────────────────────────────────────────
	--
	-- rooter is authoritative for routing and anything a client cannot see. It is not the source
	-- for engine internals (reuse, speculative-decode acceptance, reasoning recoveries) or for a
	-- runner's version banner. Those arrive from the runner surfaces and land here.
	--
	-- Redaction rule, enforced by the writer and not by hope: `extra` holds the source's raw
	-- object so a wrong derivation can be re-derived later, but content-bearing fields are
	-- stripped before insert - choices[].message.content, choices[].delta.content, top-level
	-- message/content arrays, and any string field longer than a page. A metrics store that
	-- quietly keeps a copy of everything the models said is a different kind of system, and this
	-- one is not it.
	extra jsonb NOT NULL DEFAULT '{}'::jsonb,

	-- ─── bookkeeping ────────────────────────────────────────────────────────────────────
	first_seen_at timestamptz NOT NULL DEFAULT now(),
	last_seen_at timestamptz NOT NULL DEFAULT now(),
	updated_at timestamptz NOT NULL DEFAULT now()
);

-- ─── the join is an index ──────────────────────────────────────────────────────────────
--
-- One row per chatcmpl id, enforced here rather than promised in code. When the harvester has
-- already recorded a routed call from rooter (event_key 'rooter|abc', upstream_id 'chatcmpl-xyz')
-- and then the Strata scrape reports 'chatcmpl-xyz' as its own request, the insert into
-- event_key 'strata|chatcmpl-xyz' collides with THIS index, not with the primary key. That
-- collision is the join: the writer catches it, merges the engine facts into the existing row,
-- appends its own id to source_ids, and records join_method = 'upstream_id'. A runner-side event
-- physically cannot create a second row for a call that is already in the store.
--
-- The open assumption is that strata's requests[].id is the same chatcmpl id it returns in the
-- response body. If it is not, the join silently stops happening and routed calls appear as two
-- single-source rows with source_count 1 each - which is measurable (see call_quality), so this
-- assumption fails visibly rather than quietly double counting.
CREATE UNIQUE INDEX IF NOT EXISTS call_events_upstream_key
	ON call_events (upstream_id)
	WHERE upstream_id is not null and upstream_id <> '';

-- Rooter's own event id, when the row came from a router. Unique where present, so a re-pull
-- after a ring reset lands on the same row through the same shape of collision handling.
CREATE UNIQUE INDEX IF NOT EXISTS call_events_rooter_id
	ON call_events (id)
	WHERE id is not null and id <> '';

CREATE INDEX IF NOT EXISTS call_events_backend_ts ON call_events (backend, ts desc);
CREATE INDEX IF NOT EXISTS call_events_key_ts ON call_events ("key", ts desc);
CREATE INDEX IF NOT EXISTS call_events_model_ts ON call_events (model, ts desc);
CREATE INDEX IF NOT EXISTS call_events_profile_ts ON call_events (profile, ts desc);
CREATE INDEX IF NOT EXISTS call_events_ts ON call_events (ts desc);
CREATE INDEX IF NOT EXISTS call_events_observed_ts ON call_events (last_seen_at desc);
CREATE INDEX IF NOT EXISTS call_events_source_ids ON call_events USING gin (source_ids);

-- ─── calls: the view dashboards should read ────────────────────────────────────────────
--
-- A thin, boring surface over the table with a stable call id. Reading the table directly is
-- fine for forensics; reading this is what keeps a panel honest when the join rules change.
CREATE OR REPLACE VIEW calls AS
SELECT
	coalesce(nullif(upstream_id, ''), event_key) AS call_id,
	backend,
	alias,
	profile,
	version,
	engine,
	model,
	model_requested,
	kind,
	"key",
	"key_source",
	backend_url,
	stream,
	status,
	aborted,
	finish_reason,
	error,
	ts,
	ts_is_observation,
	source_count,
	usage_source,
	has_usage,
	prompt_tokens,
	completion_tokens,
	cached_tokens,
	engine_cached_tokens,
	cache_hit_rate,
	prefill_tok_s,
	decode_tok_s,
	wait_ms,
	ttft_ms,
	prompt_ms,
	decode_ms,
	total_ms,
	join_method
FROM call_events;

-- ─── call_quality: the store's own trustworthiness, as data ────────────────────────────
--
-- Every surface in this system can produce a plausible number that is wrong: an inflight counter
-- that never decrements, a badge that returns 200 HTML, an average over rows that have no usage.
-- This view is how those show up as something to look at instead of something to discover late.
CREATE OR REPLACE VIEW call_quality AS
SELECT
	backend,
	count(*) AS calls,
	count(*) FILTER (WHERE has_usage) AS calls_with_usage,
	count(*) FILTER (WHERE NOT has_usage) AS calls_without_usage,
	count(*) FILTER (WHERE aborted) AS calls_aborted,
	count(*) FILTER (WHERE usage_source = 'injected') AS calls_with_usage_injected,
	count(*) FILTER (WHERE ts_is_observation) AS calls_with_observed_ts,
	count(*) FILTER (WHERE source_count > 1) AS calls_joined_across_surfaces,
	count(*) FILTER (WHERE upstream_id is not null) AS calls_with_upstream_id,
	count(*) FILTER (WHERE join_method = 'backend_key_window') AS calls_joined_by_fallback,
	-- The two measurements of the same concept. If these drift apart, one of them is not what you
	-- think it is.
	avg(cache_hit_rate) FILTER (WHERE cache_hit_rate is not null) AS mean_wire_cache_rate,
	avg(engine_cached_tokens::numeric / NULLIF(prompt_tokens, 0))
		FILTER (WHERE engine_cached_tokens is not null and prompt_tokens is not null and prompt_tokens > 0)
		AS mean_engine_cache_rate
FROM call_events
GROUP BY backend;

COMMIT;
