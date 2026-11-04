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
	--   usage_content_window - a fallback join resting on measured agreement.
	--   backend_key_window   - a fallback join resting on timing plus a wire witness.
	-- Recorded rather than inferred at query time, because the fallback joins are the part of this
	-- design that rests on assumptions, and a dashboard must be able to show how much of what it
	-- displays depends on each of them. The two fallback tiers are recorded SEPARATELY on purpose:
	--   usage_content_window  - both sides measured prompt and completion tokens and those numbers
	--                           agree exactly, on top of start proximity, the wire witness and
	--                           source_count = 1. Content agreement, and the only way to reach a row
	--                           that already carries an upstream_id - which rooter's ring records now
	--                           supply for most calls.
	--   backend_key_window    - same backend, exactly one side holding a session key, source_count = 1,
	--                           starts within the window (plus each side's wait_ms) with the intervals
	--                           overlapping as a secondary guard, and nobody has identified the
	--                           candidate. Used where one side measured no usage at all: a clock
	--                           agreement, which is what an abandoned stream leaves behind.
	-- Ranking them is the difference between "I know this is one call" and "I believe it is". When both
	-- sides measured and the numbers disagree, neither tier fires - that is a different call, and it is
	-- 90 ms and 100 ms from being mistaken for one.
	join_method text NOT NULL DEFAULT 'self'
		check (join_method in ('self', 'upstream_id', 'usage_content_window', 'backend_key_window')),

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
	-- first_content_ms is the first token of CONTENT, from the same anchor as ttft_ms, which is the
	-- first byte of the response. On a reasoning model these are far apart - measured on rooter's live
	-- ring: ttft 24ms with content 488ms later on one call, ttft 2350ms with content at 32666ms on
	-- another - and the distance between them is the only thing that says a box was busy while nothing
	-- had failed. Storing ttft alone is how a dashboard reports a healthy fast box over a call that
	-- spent half a minute thinking.
	first_content_ms double precision check (first_content_ms is null or first_content_ms >= 0),
	-- Derived from the two above, stored, and unsettable: the components can arrive in different
	-- writes (an aborted stream has a first byte and no content token), and a generated column
	-- recalculates when the merge fills the missing half. A plain column here could be written by a
	-- surface with its own idea of the gap, which is a number a panel would then average.
	first_content_gap_ms double precision GENERATED ALWAYS AS (first_content_ms - ttft_ms) STORED,
	prompt_ms double precision check (prompt_ms is null or prompt_ms >= 0),
	decode_ms double precision check (decode_ms is null or decode_ms >= 0),
	total_ms double precision check (total_ms is null or total_ms >= 0),

	-- ─── tokens: NULL when not reported ────────────────────────────────────────────────
	--
	-- A streaming response with no stream_options.include_usage carries no usage at all. rooter used to
	-- inject include_usage into streaming requests it forwarded so the tail was parseable; it now
	-- reports that the injection is a no-op on llama.cpp (the tail arrives either way), and its API
	-- only ever names 'none' or 'client'. Live confirms it: 56 rooter rows in this store, none of them
	-- 'injected'. So the mechanism is recorded when a surface can state it and 'none' when it cannot -
	-- which for rooter's ring today means 'none', because its records carry no provenance field.
	prompt_tokens integer check (prompt_tokens is null or prompt_tokens >= 0),
	-- prompt_tokens_computed exists because the number a runner calls "prompt" is not always the same
	-- number as the length of the prompt. rooter publishes its own arithmetic answer beside the
	-- reported count rather than replacing it, and so does this table: the two are separate columns,
	-- and a reader can see which calls were under-reported. Collapsing them would destroy the only
	-- evidence of how often that happens.
	--
	-- Which of the two is the arithmetic VARIES BY ENGINE, and that is the reason for the two
	-- provenance columns below. Live from rooter's ring, 2026-11-04, all 71 rows with a prompt count:
	--   strata (14):  prompt_tokens_computed == extra.prompt_n verbatim. Strata's prompt_n is the
	--                 tokens it evaluated this pass, so the gap between the two columns is the
	--                 runner's own statement about its cache.
	--   sushi  (57):  extra.prompt_n == prompt_tokens (the FULL length - the engine says so, via
	--                 runner_prompt_n_includes_cache) and prompt_tokens_computed == prompt_n -
	--                 cached_n. Here the computed column is the router's subtraction.
	-- cached_tokens + prompt_tokens_computed == prompt_tokens holds in all 71, which reads like two
	-- independent measurements agreeing and in 57 of them is arithmetic. Read the flag, not the
	-- equality.
	--
	-- And the ring is not the history. The 628 rows the harvester has KEPT show this same column meaning
	-- two different things on the same backend, split by time:
	--   174 sushi rows, 04:58-06:20   prompt_tokens_computed == prompt_tokens, cached_tokens large
	--   140 sushi rows, 06:34-07:21   prompt_tokens_computed == prompt_tokens - cached_tokens
	-- So rooter's subtraction moved into this column during the day, and the 71-row sample above is
	-- entirely post-cutover because the ring holds 60 minutes and had already rolled past the rest.
	-- `version` is NULL on every one of those rows, so the change cannot be attributed to a build. The
	-- 174 are permanently ambiguous - the flag did not exist when they were written. This is what the
	-- store's insistence on provenance-over-inference is for.
	prompt_tokens_computed integer check (prompt_tokens_computed is null or prompt_tokens_computed >= 0),
	-- The engine's own statement about what its prompt_n means, and the router's about whether
	-- prompt_tokens is its own addition. Both NULL when the surface did not say - never FALSE, which
	-- would launder an unknown into a fact. See 005.
	runner_prompt_n_includes_cache boolean,
	prompt_tokens_derived boolean,
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
	-- MoE expert cache (expert_slots, expert_cache_mib), which was the obvious candidate for what
	-- its hit_rate counts - and the runner now says so in its own words (serve/prometheus.py: "The
	-- last request's expert cache hit rate"), so it is a named column rather than a mystery living
	-- in `extra`. An unexplained number promoted to a first-class column becomes a dashboard legend
	-- that lies; an explained one gets a name that stops the next reader conflating it.
	--
	-- Three rates, three different questions, no shared column:
	--   cache_hit_rate         cached_tokens / prompt_tokens  - what the runner BILLED as cached
	--   engine_cache_hit_rate  engine_cached_tokens / prompt_tokens - what it internally REUSED
	--   expert_cache_hit_rate  reported, not derived - how much MoE expert weight stayed resident
	-- A 107k-token prompt can be ~99% reused and ~85% expert-resident at the same time, both true,
	-- and a single "cache hit rate" column would make one of those sentences false.
	--
	-- The components stay in the row so a definition change is a recompute and not a migration.
	prefill_tok_s double precision check (prefill_tok_s is null or prefill_tok_s >= 0),
	decode_tok_s double precision check (decode_tok_s is null or decode_tok_s >= 0),
	cache_hit_rate double precision check (cache_hit_rate is null or (cache_hit_rate >= 0 and cache_hit_rate <= 1)),
	engine_cache_hit_rate double precision check (engine_cache_hit_rate is null or (engine_cache_hit_rate >= 0 and engine_cache_hit_rate <= 1)),
	-- Recorded exactly as the runner reports it. Not a token ratio, never to be read as one, and
	-- kept separate from the two ratios above for that reason.
	expert_cache_hit_rate double precision check (expert_cache_hit_rate is null or (expert_cache_hit_rate >= 0 and expert_cache_hit_rate <= 1)),

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
	-- A rate that cannot exist. cached_tokens > prompt_tokens is not automatically a bug: llama.cpp's
	-- prompt_n can be smaller than the cached span it reuses, so the runner's own numbers can legitimately
	-- exceed each other. It is also exactly what a bad join produces - one surface's cached count sitting
	-- on another surface's prompt count. The two cases look identical in the data, so this column names
	-- the state instead of guessing which caused it, and call_quality counts it.
	cache_rate_undefined boolean GENERATED ALWAYS AS (
		(prompt_tokens is not null and prompt_tokens > 0 and (
			(cached_tokens is not null and cached_tokens > prompt_tokens)
			or (engine_cached_tokens is not null and engine_cached_tokens > prompt_tokens)
		))
	) STORED,

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
-- The assumption this index rests on is now KNOWN to be false, and stating that here is the point:
-- strata's requests[].id is NOT the chatcmpl id it returns in the response body. Measured twice -
-- a response body carrying chatcmpl-9d2f3f13f80c48a8a1b99817 against the same runner's metrics
-- record id cbace0db24184790b1111c5b9f984904, and a live harvest where the engine records were
-- 12-hex ids like 5628878f244b while rooter's events carried chatcmpl-b434b754898a409faacc8936.
-- So this index joins the surfaces that echo the response id (a router, a client-side recorder) and
-- joins NOTHING to strata's own records. Routed calls therefore reach one row through the fallback,
-- not through here - which is why the fallback is a first-class path with two named tiers rather than
-- a curiosity, and why call_quality counts them apart.
--
-- What would still break quietly: if two surfaces DID share an id for different calls, this collides
-- them into one row. The id space is a runner's, so the risk is a runner reusing an id, not us.
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

-- ─── the cache rates are derived by the database, not by the writer ───────────────────
--
-- Two of the three rate columns are ratios of columns in the same row, so the row is the only
-- place they can be defined consistently. If every probe computes its own, then "cache hit rate"
-- means whatever each probe decided it meant - and the one mistake this table is most likely to
-- inherit from Strata is feeding its expert-cache `hit_rate` (0.80-0.90) into a prompt-reuse column
-- (0.99+) and never noticing, because both are between 0 and 1 and both are called a cache rate.
--
-- So the database derives them, refuses a rate it cannot source, and refuses a rate that disagrees
-- with its own components. A BEFORE trigger is the only place this holds for every writer: the Go
-- store, a psql session, a future importer, and `ON CONFLICT DO UPDATE`, where the merged components
-- come from two different sightings and the rate has to be recomputed from the merge rather than
-- carried over from whichever event last set it.
--
-- The tolerance is 1e-6 because the columns are double precision and a 6-decimal literal is not
-- exactly representable. It is a statement about binary fractions, not a license to be approximate:
-- the disagreement it would let through is 0.000001, and the disagreement it exists to catch is 0.15.
CREATE OR REPLACE FUNCTION call_events_derive_rates() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
	wire_ratio numeric;
	engine_ratio numeric;
	-- A rate the writer did not touch is not a claim by the writer. On an UPDATE, NEW carries the
	-- value the row already held, which was derived from the components the row USED to have - so
	-- changing cached_tokens would otherwise be rejected for disagreeing with the stale rate it is
	-- changing. Only a rate the statement actually altered can be checked against a claim.
	claimed_wire boolean;
	claimed_engine boolean;
BEGIN
	claimed_wire := (TG_OP = 'INSERT' AND NEW.cache_hit_rate IS NOT NULL)
		OR (TG_OP = 'UPDATE' AND NEW.cache_hit_rate IS DISTINCT FROM OLD.cache_hit_rate);
	claimed_engine := (TG_OP = 'INSERT' AND NEW.engine_cache_hit_rate IS NOT NULL)
		OR (TG_OP = 'UPDATE' AND NEW.engine_cache_hit_rate IS DISTINCT FROM OLD.engine_cache_hit_rate);

	IF NEW.prompt_tokens IS NOT NULL AND NEW.prompt_tokens > 0 THEN
		IF NEW.cached_tokens IS NOT NULL THEN
			wire_ratio := NEW.cached_tokens::numeric / NEW.prompt_tokens::numeric;
			IF wire_ratio > 1 THEN
				-- Not an exception, and that reversal is deliberate. Two ways to get here: a bad join
				-- putting one surface's cached count on another's prompt count, or llama.cpp's prompt_n
				-- genuinely smaller than the cached span it reused (which is why rooter publishes
				-- prompt_tokens_computed at all). Raising here rejected the whole write - the store lost a
				-- call a router had fully witnessed because a DERIVED ratio was undefined, and the counter
				-- it reported was a write error rather than "one rate is unusable". The rate goes NULL,
				-- cache_rate_undefined says so in the row, and the components stay for a recompute.
				-- A CLAIMED rate is still refused: nothing is derivable from these components, so anyone
				-- putting a number here brought it from somewhere else.
				IF claimed_wire THEN
					RAISE EXCEPTION 'call_events: cache_hit_rate % claimed on event_key % while cached_tokens % exceeds prompt_tokens % - these components support no rate, so a claim here is an expert rate in a prompt-reuse column or a join that mixed two calls',
						NEW.cache_hit_rate, NEW.event_key, NEW.cached_tokens, NEW.prompt_tokens;
				END IF;
				NEW.cache_hit_rate := NULL;
			ELSE
				IF claimed_wire AND abs(NEW.cache_hit_rate::numeric - wire_ratio) > 0.000001 THEN
					RAISE EXCEPTION 'call_events: cache_hit_rate % does not match cached_tokens % / prompt_tokens % = % on event_key % (this is where Strata''s expert-cache hit_rate would land, and it is not a prompt-reuse rate)',
						NEW.cache_hit_rate, NEW.cached_tokens, NEW.prompt_tokens, wire_ratio, NEW.event_key;
				END IF;
				NEW.cache_hit_rate := wire_ratio;
			END IF;
		ELSIF NEW.cache_hit_rate IS NOT NULL THEN
			IF claimed_wire THEN
				RAISE EXCEPTION 'call_events: cache_hit_rate % on event_key % has no cached_tokens to derive it from - a rate with no numerator is a guess wearing a column',
					NEW.cache_hit_rate, NEW.event_key;
			END IF;
			-- The numerator went away, so the rate goes with it rather than lingering as a number
			-- that no longer describes anything in this row.
			NEW.cache_hit_rate := NULL;
		END IF;

		IF NEW.engine_cached_tokens IS NOT NULL THEN
			engine_ratio := NEW.engine_cached_tokens::numeric / NEW.prompt_tokens::numeric;
			IF engine_ratio > 1 THEN
				-- Same reasoning as the wire ratio above: the rate is undefined, the row is not invalid.
				IF claimed_engine THEN
					RAISE EXCEPTION 'call_events: engine_cache_hit_rate % claimed on event_key % while engine_cached_tokens % exceeds prompt_tokens % - no rate is derivable here either',
						NEW.engine_cache_hit_rate, NEW.event_key, NEW.engine_cached_tokens, NEW.prompt_tokens;
				END IF;
				NEW.engine_cache_hit_rate := NULL;
			ELSE
				IF claimed_engine AND abs(NEW.engine_cache_hit_rate::numeric - engine_ratio) > 0.000001 THEN
					RAISE EXCEPTION 'call_events: engine_cache_hit_rate % does not match engine_cached_tokens % / prompt_tokens % = % on event_key %',
						NEW.engine_cache_hit_rate, NEW.engine_cached_tokens, NEW.prompt_tokens, engine_ratio, NEW.event_key;
				END IF;
				NEW.engine_cache_hit_rate := engine_ratio;
			END IF;
		ELSIF NEW.engine_cache_hit_rate IS NOT NULL THEN
			IF claimed_engine THEN
				RAISE EXCEPTION 'call_events: engine_cache_hit_rate % on event_key % has no engine_cached_tokens to derive it from',
					NEW.engine_cache_hit_rate, NEW.event_key;
			END IF;
			NEW.engine_cache_hit_rate := NULL;
		END IF;
	ELSE
		-- No denominator. A rate claimed here is unsourced; a rate left over from before is stale
		-- and is dropped rather than kept as decoration.
		IF claimed_wire OR claimed_engine THEN
			RAISE EXCEPTION 'call_events: a cache rate on event_key % with prompt_tokens % has no denominator',
				NEW.event_key, NEW.prompt_tokens;
		END IF;
		NEW.cache_hit_rate := NULL;
		NEW.engine_cache_hit_rate := NULL;
	END IF;

	-- expert_cache_hit_rate is deliberately NOT touched: it is a reported quantity from the
	-- runner''s own accounting, it is not a ratio of anything in this row, and deriving it would
	-- invent a measurement the runner never made.
	RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS call_events_rates ON call_events;
CREATE TRIGGER call_events_rates BEFORE INSERT OR UPDATE ON call_events
	FOR EACH ROW EXECUTE FUNCTION call_events_derive_rates();

-- ─── the columns the views below reference ─────────────────────────────────────────────
--
-- Idempotent ALTERs, in the file that every boot runs first, because the views underneath reference
-- columns that 002/003/004 exist to add to databases that predate them. Without this block the upgrade
-- path is broken in a way that only shows up on a database that has been in use: 001 runs, its
-- CREATE TABLE IF NOT EXISTS adds nothing to an existing table, and then CREATE OR REPLACE VIEW
-- references a column that nothing has added yet - "column does not exist" at boot, on the pod that has
-- been running fine for a month. A fresh database never sees this; an installed one hits it on the
-- first restart after a migration is added, which is the worst possible time.
--
-- The definitions here are byte-for-byte the ones in 003, 004 and 005. That duplication is deliberate
-- and load-bearing: if they drifted, this file would create a column the later file then refuses to
-- accept, or the reverse. 003, 004 and 005 each verify what actually landed in the database they just
-- ran against.
DO $$
BEGIN
	ALTER TABLE call_events ADD COLUMN IF NOT EXISTS
		prompt_tokens_computed integer check (prompt_tokens_computed is null or prompt_tokens_computed >= 0);
	ALTER TABLE call_events ADD COLUMN IF NOT EXISTS cache_rate_undefined boolean GENERATED ALWAYS AS (
		(prompt_tokens is not null and prompt_tokens > 0 and (
			(cached_tokens is not null and cached_tokens > prompt_tokens)
			or (engine_cached_tokens is not null and engine_cached_tokens > prompt_tokens)
		))
	) STORED;
	-- The two reported rates. Not generated - the engine and the runner report them, and the trigger
	-- above only fills them in when the components allow a rate to be derived - so they are declared
	-- exactly as the CREATE TABLE declares them, check included. A column whose upgraded shape differs
	-- from its fresh shape is a column whose constraints depend on which path a database happened to
	-- take, and nobody reasons about that at 3am.
	ALTER TABLE call_events ADD COLUMN IF NOT EXISTS engine_cache_hit_rate double precision
		check (engine_cache_hit_rate is null or (engine_cache_hit_rate >= 0 and engine_cache_hit_rate <= 1));
	ALTER TABLE call_events ADD COLUMN IF NOT EXISTS expert_cache_hit_rate double precision
		check (expert_cache_hit_rate is null or (expert_cache_hit_rate >= 0 and expert_cache_hit_rate <= 1));

	ALTER TABLE call_events ADD COLUMN IF NOT EXISTS
		first_content_ms double precision check (first_content_ms is null or first_content_ms >= 0);
	ALTER TABLE call_events ADD COLUMN IF NOT EXISTS first_content_gap_ms double precision
		GENERATED ALWAYS AS (first_content_ms - ttft_ms) STORED;
	-- 005's pair, same reason: the view and call_quality below both reference them.
	ALTER TABLE call_events ADD COLUMN IF NOT EXISTS runner_prompt_n_includes_cache boolean;
	ALTER TABLE call_events ADD COLUMN IF NOT EXISTS prompt_tokens_derived boolean;
END $$;

-- ─── calls: the view dashboards should read ────────────────────────────────────────────
--
-- A thin, boring surface over the table with a stable call id. Reading the table directly is
-- fine for forensics; reading this is what keeps a panel honest when the join rules change.
--
-- DROP and CREATE, not CREATE OR REPLACE, and that is load-bearing rather than taste. A view is a
-- projection, so dropping it loses nothing; but Postgres will not let CREATE OR REPLACE VIEW change
-- the name or position of a column the view already has, and every column this file has ever added
-- lands somewhere in that list. So `CREATE OR REPLACE` makes this migration succeed on a database
-- created today and fail on a database created last month, at process start, because the harvester
-- migrates before it serves.
--
-- Measured on the live dev store, not simulated: 001 ran against a database whose view had
-- engine_cache_hit_rate in position 28 - where it had been since it was written - and the boot died
-- with "cannot change name of view column \"engine_cache_hit_rate\" to \"prefill_tok_s\"". Nothing about
-- the data was wrong; nothing about the fresh-install path was wrong. The only thing that would have
-- caught it is a test that migrates an old database, which is what test/legacy_upgrade_path.sql is.
--
-- The table keeps its columns appended at the end because ALTER TABLE ADD COLUMN has no other option,
-- so the view lists them in the table's order and the two stay comparable. The view is free to reorder;
-- it does not, because a panel that reads by position is a panel that would silently change meaning.
DROP VIEW IF EXISTS calls;
CREATE VIEW calls AS
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
	join_method,
	-- Everything added since the first schema shipped is appended HERE and nowhere else, and that is a
	-- requirement rather than tidiness. Postgres will add columns to an existing view only at the end,
	-- in order; it refuses to change the name or position of a column the view already has. So these
	-- seven are what a database created before them can accept, and the same seven slotted into the
	-- middle of the list is a migration that fails at process start - which is exactly where 001 runs,
	-- because the harvester migrates before it serves.
	--
	-- Measured, not predicted, twice over. first_content_ms in the middle killed a store that had been
	-- running for a day ("cannot change name of view column prompt_ms to first_content_ms"). And
	-- engine_cache_hit_rate sat here in position 28 for weeks, which is why the very first installed
	-- schema cannot be upgraded at all: the view replacement fails before the ALTER that would have
	-- added the column can matter. Both are caught by test/legacy_upgrade_path.sql, which builds the old
	-- shape from a frozen snapshot and migrates it forward, so the next column added in the wrong place
	-- fails a lane instead of a deploy.
	--
	-- The view is DROP + CREATE now, which removes the positional failure entirely. The ordering rule
	-- survives it, for two reasons: the ALTERs in the DO block above have no other option, so a fresh
	-- database and an upgraded one agree on the TABLE's order only if additions go last here too; and
	-- the rule is what keeps the two shapes interchangeable, which is the thing that actually broke.
	--
	-- What this rule costs is that the view's column order is historical rather than logical, and a
	-- reader who wants cached_tokens next to cache_hit_rate cannot have them. That is the right trade:
	-- order here is cosmetic, and a migration that only works on an empty database is not a migration.
	--
	-- prompt_tokens_computed is in this tail although it is a 003 column, and it was missing from this
	-- view entirely until the provenance pair was added - which is how the omission got noticed: the
	-- view was about to expose the field that says whether the computed count is a measurement or a
	-- subtraction, without the computed count. A projection that shows the explanation and hides the
	-- thing explained is worse than showing neither, because it invites a reader to believe they have
	-- been told something. It is first in the tail because it was added first.
	prompt_tokens_computed,
	engine_cache_hit_rate,
	expert_cache_hit_rate,
	first_content_ms,
	first_content_gap_ms,
	-- The provenance pair, for the reason everything else in this view is: a panel has to be able to
	-- ask how much of its denominator is arithmetic, and it cannot ask that of a projection that
	-- dropped the flag.
	runner_prompt_n_includes_cache,
	prompt_tokens_derived
FROM call_events;

-- ─── call_quality: the store's own trustworthiness, as data ────────────────────────────
--
-- Every surface in this system can produce a plausible number that is wrong: an inflight counter
-- that never decrements, a badge that returns 200 HTML, an average over rows that have no usage.
-- This view is how those show up as something to look at instead of something to discover late.
-- Dropped and recreated rather than CREATE OR REPLACE, and that is not decoration: Postgres will not
-- rename a view column through CREATE OR REPLACE, so the migration that split the fallback tiers into
-- calls_joined_by_content and calls_joined_by_timing_only failed on an existing database with
-- "cannot change name of view column". A schema file that only works on a database nobody has used
-- yet is a schema file that works in CI and nowhere else.
DROP VIEW IF EXISTS call_quality;
CREATE VIEW call_quality AS
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
	count(*) FILTER (WHERE join_method = 'usage_content_window') AS calls_joined_by_content,
	count(*) FILTER (WHERE join_method = 'backend_key_window') AS calls_joined_by_timing_only,
	count(*) FILTER (WHERE cache_rate_undefined) AS calls_with_undefined_cache_rate,
	-- How many rows carry the expert-cache rate at all. It is the axis most likely to be misread as
	-- a prompt-cache rate, so its coverage belongs next to the two prompt-cache rates.
	count(*) FILTER (WHERE expert_cache_hit_rate is not null) AS calls_with_expert_cache_rate,
	-- The two measurements of the same concept. If these drift apart, one of them is not what you
	-- think it is. The third average is a DIFFERENT concept, listed beside them only so that its
	-- distance from the other two stays visible instead of being quietly averaged with them.
	avg(cache_hit_rate) FILTER (WHERE cache_hit_rate is not null) AS mean_wire_cache_rate,
	avg(engine_cached_tokens::numeric / NULLIF(prompt_tokens, 0))
		FILTER (WHERE engine_cached_tokens is not null and prompt_tokens is not null and prompt_tokens > 0)
		AS mean_engine_cache_rate,
	avg(expert_cache_hit_rate) FILTER (WHERE expert_cache_hit_rate is not null) AS mean_expert_cache_rate,
	-- Coverage of the two halves of the thinking gap separately, because they fail independently: a
	-- streaming call that was cut off has a first byte and no content token, and a surface that never
	-- measured content at all has neither. A mean over the gap is only honest if you can see how many
	-- rows the mean was taken from.
	count(*) FILTER (WHERE first_content_ms is not null) AS calls_with_first_content_ms,
	count(*) FILTER (WHERE first_content_gap_ms is not null) AS calls_with_first_content_gap,
	avg(ttft_ms) FILTER (WHERE ttft_ms is not null) AS mean_ttft_ms,
	avg(first_content_ms) FILTER (WHERE first_content_ms is not null) AS mean_first_content_ms,
	avg(first_content_gap_ms) FILTER (WHERE first_content_gap_ms is not null) AS mean_first_content_gap_ms,
	-- The gap is `first_content - ttft` and both come from one observer's clock. A negative one means an
	-- observer recorded content before it recorded the first byte, which is a contradiction rather than
	-- a fast model - counted here rather than CHECK-rejected, because the alternative is dropping the
	-- call and the measurement with it.
	count(*) FILTER (WHERE first_content_gap_ms < 0) AS calls_with_negative_first_content_gap,
	-- The provenance of the prompt numbers, per backend. This is a population question, not a mean:
	-- `prompt_tokens_computed` means "the runner's own number" on one engine and "the router's
	-- subtraction" on another, and a panel that averages the two is describing neither. The unknowns
	-- are counted apart from the NOs, because a surface that has not said yet is not a surface that
	-- said no.
	count(*) FILTER (WHERE runner_prompt_n_includes_cache is not null) AS calls_with_prompt_n_flag,
	count(*) FILTER (WHERE runner_prompt_n_includes_cache) AS calls_with_prompt_n_including_cache,
	count(*) FILTER (WHERE prompt_tokens_derived is not null) AS calls_with_prompt_derivation_flag,
	count(*) FILTER (WHERE prompt_tokens_derived) AS calls_with_derived_prompt_tokens,
	-- The equality that looks like corroboration and is not: cached + computed == prompt holds on
	-- every live row, both where the engine reported it and where the router built it. Published so
	-- nobody uses that agreement as evidence without the two flags next to it.
	count(*) FILTER (WHERE prompt_tokens_computed is not null and prompt_tokens is not null
					AND prompt_tokens_computed = prompt_tokens - coalesce(cached_tokens, 0))
											AS calls_where_prompt_equals_cached_plus_computed
FROM call_events
GROUP BY backend;

COMMIT;
