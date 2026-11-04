-- contract_scenarios.sql — the write protocol, executed against a real Postgres 16.
--
-- Not decoration around the schema. Each step is a statement sequence the harvester will really
-- run, and each one states out loud what a correct outcome looks like. The Go contract test in
-- internal/store mirrors these scenarios through the production write path; where the two
-- disagree, the schema is lying somewhere.
--
-- The protocol, in the order a writer must try it:
--
--   1. same source, same native id        -> ON CONFLICT (event_key) merge. A re-pull.
--   2. different source, same chatcmpl id -> collides on call_events_upstream_key; merge into the
--                                           row that already holds that id. THIS is the join.
--   3. no collision, but a conservative candidate exists (same backend; exactly one side carries a
--      session key; candidate has no upstream_id and source_count = 1; the two call intervals
--      overlap within 2s; exactly one candidate) -> merge into it,
--      join_method = 'backend_key_window'.
--   4. none of the above                  -> insert a new row, join_method stays 'self'.
--
-- Order matters, and step 3 below is what the first draft of this file got wrong: if the router's
-- row has no upstream_id yet - which is exactly the case for an abandoned stream, because the
-- chatcmpl id lives in the response tail that never arrived - then nothing can collide, and the
-- runner-side event would happily create a second row for one call. The fallback exists for that
-- hole, and it is a heuristic, so it is deliberately narrow.
--
-- Run: psql -v ON_ERROR_STOP=1 -f test/contract_scenarios.sql
\set ON_ERROR_STOP on

BEGIN;

-- These scenarios assert absolute counts, so they need an empty table. Re-running them against a
-- database that already holds them reports a count that reads like a broken contract when nothing is
-- broken - which is the kind of failure that sends someone to debug the wrong thing.
DO $$
DECLARE n bigint;
BEGIN
	SELECT count(*) INTO n FROM call_events;
	IF n > 0 THEN
		RAISE EXCEPTION 'contract_scenarios.sql needs an empty call_events: found % rows. Run it against a fresh database (or TRUNCATE call_events first).', n;
	END IF;
END $$;

-- ══ A. a completed routed call, seen first by the router ════════════════════════════════════
-- rooter parsed the response tail, so it has usage AND the runner's chatcmpl id. This is the good
-- case: the join key is present from the first write.
INSERT INTO call_events (
	event_key, source, source_ids, id, upstream_id, ts, backend, backend_url, alias, profile,
	model, model_requested, kind, "key", key_source, path, method, stream, status, bytes,
	aborted, wait_ms, ttft_ms, total_ms, prompt_tokens, completion_tokens, cached_tokens,
	cache_hit_rate, usage_source, finish_reason, extra
) VALUES (
	'rooter|r-1001', 'rooter', '{rooter|r-1001}', 'r-1001', 'chatcmpl-aaa',
	'2026-11-04T09:00:00Z', 'strata', 'http://192.168.11.128:8088', 'strata', 'iq3_xxs',
	'qwen3.8-flash-next-iq3_xxs', 'auto', 'gen', 'pi-code', 'header', '/v1/chat/completions',
	'POST', true, 200, 98213, false, 3.2, 640.0, 10442.4, 106718, 700, 106677, 0.999616,
	'injected', 'stop', '{"rooter": {"raw": {"usage": {"prompt_tokens": 106718}}}}'::jsonb
);

-- ══ B. the same rooter event, seen again on a later pull ════════════════════════════════════
-- Cursor overlap after a reconnect is normal, not an error. Merging must be idempotent in the
-- strongest useful sense: no new row, and crucially no inflation of source_count. A surface
-- re-reporting itself is not a second surface confirming the call.
INSERT INTO call_events (
	event_key, source, source_ids, id, upstream_id, ts, backend, status, bytes, aborted,
	wait_ms, ttft_ms, total_ms, usage_source
) VALUES (
	'rooter|r-1001', 'rooter', '{rooter|r-1001}', 'r-1001', 'chatcmpl-aaa',
	'2026-11-04T09:00:00Z', 'strata', 200, 98213, false, 3.2, 640.0, 10442.4, 'injected'
) ON CONFLICT (event_key) DO UPDATE SET
	ttft_ms      = COALESCE(EXCLUDED.ttft_ms, call_events.ttft_ms),
	bytes        = COALESCE(EXCLUDED.bytes, call_events.bytes),
	wait_ms      = COALESCE(EXCLUDED.wait_ms, call_events.wait_ms),
	status       = COALESCE(EXCLUDED.status, call_events.status),
	-- Abandonment is sticky: one sighting that says "the client hung up" is enough, and a sighting
	-- that did not notice must not undo it.
	aborted      = call_events.aborted OR EXCLUDED.aborted,
	-- Deliberately NOT array_cat: re-reporting the same id must not grow the provenance list.
	source_ids   = call_events.source_ids,
	last_seen_at = now(),
	updated_at   = now();

-- ══ C. the join: the runner reports the same call as its own request ════════════════════════
-- The Strata scrape has the engine facts rooter can never see (engine build, expert/speculative
-- internals, the runner's own view of cached tokens). It must not become a second row.
DO $$
BEGIN
	INSERT INTO call_events (
		event_key, source, source_ids, upstream_id, ts, backend, alias, profile, engine, model,
		prompt_tokens, completion_tokens, cached_tokens, engine_cached_tokens, prompt_ms,
		decode_ms, prefill_tok_s, decode_tok_s, expert_cache_hit_rate, usage_source, finish_reason,
		drafts_offered, drafts_accepted, extra
	) VALUES (
		'strata|chatcmpl-aaa', 'strata', '{strata|chatcmpl-aaa}', 'chatcmpl-aaa',
		'2026-11-04T09:00:01Z', 'strata', 'strata', 'iq3_xxs', '0.1.51',
		'qwen3.8-flash-next-iq3_xxs', 106718, 700, 106677, 106677, 471.1, 9961.0, 2133.6,
		70.3, 0.802, 'runner_scrape', 'stop', 690, 431,
		'{"raw": {"id": "b01b3fd29419", "hit_rate": 0.802, "reused": 106677, "drafts_offered": 690, "drafts_accepted": 431}}'::jsonb
	);
	RAISE EXCEPTION 'step C: WRONG - the insert succeeded, so a runner-side event created a second row for a call already in the store';
EXCEPTION WHEN unique_violation THEN
	RAISE NOTICE 'step C: right - unique violation on call_events_upstream_key. That collision IS the join.';
END $$;

-- The merge that collision obliges. Note the direction of the key: the row that already existed
-- gains nothing it lacked here, but the principle is that a merge must carry the key it collided
-- on, because that key is what makes the join visible to everything that reads this table later.
UPDATE call_events SET
	upstream_id          = COALESCE(upstream_id, 'chatcmpl-aaa'),
	source_ids           = ARRAY(SELECT DISTINCT unnest(source_ids || '{strata|chatcmpl-aaa}')),
	join_method          = 'upstream_id',
	profile              = COALESCE(profile, 'iq3_xxs'),
	engine               = COALESCE(engine, '0.1.51'),
	model                = COALESCE(model, 'qwen3.8-flash-next-iq3_xxs'),
	prompt_tokens        = COALESCE(prompt_tokens, 106718),
	completion_tokens    = COALESCE(completion_tokens, 700),
	cached_tokens        = COALESCE(cached_tokens, 106677),
	engine_cached_tokens = COALESCE(engine_cached_tokens, 106677),
	prompt_ms            = COALESCE(prompt_ms, 471.1),
	decode_ms            = COALESCE(decode_ms, 9961.0),
	prefill_tok_s        = COALESCE(prefill_tok_s, 2133.6),
	decode_tok_s         = COALESCE(decode_tok_s, 70.3),
	expert_cache_hit_rate = COALESCE(expert_cache_hit_rate, 0.802),
	-- 'injected' is more informative than 'runner_scrape' and must survive the merge.
	usage_source         = CASE WHEN usage_source IS NULL OR usage_source = 'none' THEN 'runner_scrape' ELSE usage_source END,
	finish_reason        = COALESCE(finish_reason, 'stop'),
	drafts_offered       = COALESCE(drafts_offered, 690),
	drafts_accepted      = COALESCE(drafts_accepted, 431),
	-- Strata's raw object is kept under strata's own name, including hit_rate, whose meaning is
	-- not established (see the column comment in 001_initial.sql). Preserving an unexplained
	-- number is honest; promoting it into a column with a confident name is not.
	extra                = extra || '{"strata": {"id": "b01b3fd29419", "hit_rate": 0.802, "reused": 106677}}'::jsonb,
	last_seen_at         = now(),
	updated_at           = now()
WHERE event_key = 'rooter|r-1001';

-- ══ D. an abandoned stream: the envelope exists, the rest never will ════════════════════════
-- Client hung up mid-stream. rooter holds only a bounded tail, there was no final usage frame,
-- and the chatcmpl id was in the part of the response that never made it back. So: no tokens, no
-- timings beyond what the wire showed, and NO upstream_id to join on. The row is still worth
-- having, and this is the case the fallback join exists for.
INSERT INTO call_events (
	event_key, source, source_ids, id, ts, backend, backend_url, alias, profile,
	model, model_requested, kind, "key", key_source, path, method, stream, status, bytes,
	aborted, wait_ms, total_ms, usage_source, error, extra
) VALUES (
	'rooter|r-1002', 'rooter', '{rooter|r-1002}', 'r-1002', '2026-11-04T09:00:20Z', 'strata',
	'http://192.168.11.128:8088', 'strata', 'iq3_xxs', 'qwen3.8-flash-next-iq3_xxs', 'auto',
	'gen', 'pi-code', 'header', '/v1/chat/completions', 'POST', true, 200, 4096, true, 2.1,
	1800.0, 'none', NULL, '{"rooter": {"raw": {"note": "tail empty: client abort before final frame"}}}'::jsonb
);

-- ══ E. the same abandoned call, learned from the runner, with no key to collide on ══════════
-- Strata did finish this generation and kept its own record of it, so the engine facts exist.
-- There is nothing to collide with: the router's row has upstream_id NULL, and strata's own record
-- is keyed by its internal id, which no other surface has ever seen.
--
-- The predicate, and the history behind it. It tested "ts within 2 seconds", then "the two intervals
-- overlap", and the second was wrong in a way only a live run showed: a 125.6 s envelope absorbed an
-- unrelated 4.7 s engine record that began two minutes later, because a long interval touches a lot
-- of work that is not yours. Both surfaces actually stamp the START of a call - strata's `time` was
-- measured 10 ms from a client's own request start on a call that then ran 11.4 s, and rooter sets
-- `ts` in newCallRecord, whose comment is "everything here is known before a slot is even
-- requested" - so start-to-start proximity is the discriminating test, with wait_ms allowed for
-- because the router arrives before the engine does. Overlap stays as a secondary guard.
--
-- Also required: same backend; exactly one side carries a session key (a wire witness - two rows that
-- both have one are two calls, two that both lack one are two engine records); no upstream_id on the
-- candidate; source_count = 1 (a row already holding two surfaces is not a loose end); and EXACTLY
-- ONE candidate. Ambiguity is not resolved by guessing - it is left as two rows with join_method
-- 'self', which the quality view then reports as an unjoined pair.
DO $$
DECLARE
	candidate text;
	n bigint;
	-- The runner's own record of this call: start-stamped, no session key, 9599 ms of work.
	incoming timestamptz := '2026-11-04T09:00:22Z';
	incoming_ms double precision := 9599.2;
	-- One wire witness only. $4 is the incoming event's session key: NULL here, because this is the
	-- runner's own record and a runner does not know who asked. The mirrored arm (incoming has a key,
	-- candidate has none) is the other arrival order, and the Go suite tests both directions.
	sel text := '
		SELECT event_key FROM call_events
		 WHERE backend = ''strata''
		   AND (($4::text IS NOT NULL AND "key" IS NULL) OR ($4::text IS NULL AND "key" IS NOT NULL))
		   AND upstream_id IS NULL
		   AND source_count = 1
		   AND abs(extract(epoch from ts) - extract(epoch from $1))
		       <= ($3 + coalesce(wait_ms, 0) + coalesce($5, 0)) / 1000.0
		   AND ts + (coalesce(total_ms, 0) * interval ''1 millisecond'')
		       >= $1 - ($3 * interval ''1 millisecond'')
		   AND ts <= $1 + ($2 * interval ''1 millisecond'') + ($3 * interval ''1 millisecond'')
		 ORDER BY ts';
BEGIN
	EXECUTE sel INTO candidate USING incoming, incoming_ms, 2000.0, NULL, 0.0;
	IF candidate IS NULL THEN
		n := 0;
	ELSE
		EXECUTE 'SELECT count(*) FROM (' || sel || ') x' USING incoming, incoming_ms, 2000.0, NULL, 0.0 INTO n;
	END IF;

	IF n = 1 THEN
		UPDATE call_events SET
			-- The missing key arrives from the runner side. Recording it here is what makes the
			-- next sighting of this call collide properly instead of falling back again.
			upstream_id          = 'chatcmpl-bbb',
			source_ids           = ARRAY(SELECT DISTINCT unnest(source_ids || '{strata|chatcmpl-bbb}')),
			join_method          = 'backend_key_window',
			engine               = COALESCE(engine, '0.1.51'),
			engine_cached_tokens = COALESCE(engine_cached_tokens, 103582),
			prompt_tokens        = COALESCE(prompt_tokens, 103623),
			completion_tokens    = COALESCE(completion_tokens, 640),
			cached_tokens        = COALESCE(cached_tokens, 103582),
			prompt_ms            = COALESCE(prompt_ms, 498.9),
			decode_ms            = COALESCE(decode_ms, 9100.3),
			decode_tok_s         = COALESCE(decode_tok_s, 70.3),
			finish_reason        = COALESCE(finish_reason, 'stop'),
			usage_source         = CASE WHEN usage_source IS NULL OR usage_source = 'none' THEN 'runner_scrape' ELSE usage_source END,
			extra                = extra || '{"strata": {"id": "0ff5eee72d53", "hit_rate": 0.802, "reused": 103582}}'::jsonb,
			last_seen_at         = now(),
			updated_at           = now()
		WHERE event_key = candidate;
		RAISE NOTICE 'step E: right - merged into the orphan envelope by fallback, and the key it was missing is now present';
	ELSIF n = 0 THEN
		RAISE NOTICE 'step E: no candidate, inserting a new row';
	ELSE
		RAISE NOTICE 'step E: % candidates, refusing to guess - inserting a separate row', n;
	END IF;
END $$;

-- ══ F. two calls the router rejected; no runner ever saw either one ═════════════════════════
-- 401s rooter answered itself. No backend, no upstream_id, no id-shaped thing from a runner. They
-- must coexist as two rows, which is why both join indexes exclude NULL and empty string instead
-- of treating them as values that identify a call.
INSERT INTO call_events (
	event_key, source, source_ids, id, ts, backend, model_requested, kind, "key", status,
	aborted, wait_ms, usage_source, error, extra
) VALUES (
	'rooter|r-1003', 'rooter', '{rooter|r-1003}', 'r-1003', '2026-11-04T09:00:30Z', NULL,
	'auto', 'gen', 'pi-code', 401, false, 0.1, 'none',
	'{"type": "authentication_error", "message": "no key"}'::jsonb, '{}'::jsonb
);

INSERT INTO call_events (
	event_key, source, source_ids, id, ts, backend, model_requested, kind, "key", status,
	aborted, wait_ms, usage_source, error, extra
) VALUES (
	'rooter|r-1004', 'rooter', '{rooter|r-1004}', 'r-1004', '2026-11-04T09:00:31Z', NULL,
	'auto', 'gen', 'pi-code', 401, false, 0.2, 'none',
	'{"type": "authentication_error", "message": "no key"}'::jsonb, '{}'::jsonb
);

-- ══ F2. the same rejected call seen twice: must not become two rows either ══════════════════
INSERT INTO call_events (event_key, source, source_ids, id, ts, status, aborted, wait_ms, usage_source)
VALUES ('rooter|r-1003', 'rooter', '{rooter|r-1003}', 'r-1003', '2026-11-04T09:00:30Z', 401, false, 0.1, 'none')
ON CONFLICT (event_key) DO UPDATE SET last_seen_at = now(), updated_at = now();

-- ══ G. a sushi call, learned from a log line with no clock ══════════════════════════════════
-- Sushi's log lines carry no timestamp, so ts is the instant the probe read the line and
-- ts_is_observation says so out loud. The event key is a hash of the canonical fact set with the
-- timestamp excluded, which is the honest limit of a clock-less surface: identical facts on two
-- different calls collapse into one row with observations, and there is no way to tell that apart
-- from the log line. Recording it as an observation, and saying so in the row, is the fix.
INSERT INTO call_events (
	event_key, source, source_ids, ts, ts_is_observation, backend, backend_url, alias, profile,
	version, model, stream, status, aborted, prompt_tokens, completion_tokens, cached_tokens,
	prompt_ms, decode_ms, prefill_tok_s, decode_tok_s, cache_hit_rate, usage_source,
	finish_reason, extra
) VALUES (
	'sushi|9f2c1e4a7b0d', 'sushi', '{sushi|9f2c1e4a7b0d}', '2026-11-04T09:00:40Z', true, 'sushi',
	'http://192.168.137.111:8088', 'sushi', '2.6bpw', 'sushi 1.1.1 (MLX 0.32.3)',
	'Qwen3-Next-80B-A3B-Instruct-2.6bpw', true, 200, false, 113186, 217, 110674, 6804.7, 4617.0,
	369.2, 47.0, 0.977806, 'runner_scrape', 'stop',
	'{"sushi": {"raw": {"line": "113186+217 tokens streamed [prefill: 369.2 tok/s (110674 cached / 113186 total), decode: 47.0 tok/s]"}}}'::jsonb
);
-- Numbers derived from the fixture's own text so it cannot contradict itself: "113186+217" gives
-- 217 completion tokens; 113186-110674 = 2512 tokens actually prefilled, 2512 / 369.2 tok/s =
-- 6804.7 ms; 217 / 47.0 tok/s = 4617.0 ms; 110674 / 113186 = 0.977806.

-- ══ H. a re-pull of that sushi row ══════════════════════════════════════════════════════════
INSERT INTO call_events (event_key, source, source_ids, ts, backend, prompt_tokens, completion_tokens)
VALUES ('sushi|9f2c1e4a7b0d', 'sushi', '{sushi|9f2c1e4a7b0d}', '2026-11-04T09:00:40Z', 'sushi', 113186, 217)
ON CONFLICT (event_key) DO UPDATE SET last_seen_at = now(), updated_at = now();

-- ══ assertions ══════════════════════════════════════════════════════════════════════════════
DO $$
DECLARE
	r call_events%ROWTYPE;
	n bigint;
	q record;
BEGIN
	-- A + B + C: one row, both surfaces, envelope plus engine facts, no shadow row.
	SELECT * INTO r FROM call_events WHERE event_key = 'rooter|r-1001';
	IF r.id IS DISTINCT FROM 'r-1001' THEN RAISE EXCEPTION 'r-1001: id lost'; END IF;
	IF r.upstream_id IS DISTINCT FROM 'chatcmpl-aaa' THEN RAISE EXCEPTION 'r-1001: upstream_id %', r.upstream_id; END IF;
	IF r.source_count IS DISTINCT FROM 2 THEN RAISE EXCEPTION 'r-1001: source_count %, want 2', r.source_count; END IF;
	IF r.join_method IS DISTINCT FROM 'upstream_id' THEN RAISE EXCEPTION 'r-1001: join_method %', r.join_method; END IF;
	IF r.usage_source IS DISTINCT FROM 'injected' THEN RAISE EXCEPTION 'r-1001: usage_source % - the more informative value must survive the merge', r.usage_source; END IF;
	-- Both surfaces' raw payloads survive a merge, each under its own name. If the store merged raw
	-- into raw, the router's evidence would vanish the moment the runner reported the same call - and
	-- "who told us this?" would stop being answerable.
	IF (r.extra -> 'rooter') IS NULL OR (r.extra -> 'strata') IS NULL THEN RAISE EXCEPTION 'r-1001: extra lost a surface (rooter % / strata %)', r.extra -> 'rooter', r.extra -> 'strata'; END IF;
	IF r.engine IS DISTINCT FROM '0.1.51' THEN RAISE EXCEPTION 'r-1001: engine not merged'; END IF;
	IF r.drafts_accepted IS DISTINCT FROM 431 THEN RAISE EXCEPTION 'r-1001: speculative-decode facts not merged'; END IF;
	IF r.has_usage IS DISTINCT FROM true THEN RAISE EXCEPTION 'r-1001: has_usage false while tokens exist'; END IF;
	IF r.aborted IS DISTINCT FROM false THEN RAISE EXCEPTION 'r-1001: completed call marked aborted'; END IF;
	-- The two prompt-cache rates are derived by the database from this row's own components (the
	-- trigger recomputes them after a merge, which is why they are exact rather than the 0.999616
	-- that step A supplied). The expert rate is stored exactly as the runner reported it. Both are
	-- "a cache hit rate" in somebody's vocabulary, and they differ by 0.197 here.
	IF round(r.cache_hit_rate::numeric, 6) IS DISTINCT FROM 0.999616 THEN RAISE EXCEPTION 'r-1001: cache_hit_rate % not derived from cached_tokens/prompt_tokens', r.cache_hit_rate; END IF;
	IF round(r.engine_cache_hit_rate::numeric, 6) IS DISTINCT FROM 0.999616 THEN RAISE EXCEPTION 'r-1001: engine_cache_hit_rate % not derived from engine_cached_tokens/prompt_tokens', r.engine_cache_hit_rate; END IF;
	IF r.expert_cache_hit_rate IS DISTINCT FROM 0.802 THEN RAISE EXCEPTION 'r-1001: expert_cache_hit_rate %, want 0.802 exactly as reported', r.expert_cache_hit_rate; END IF;
	IF abs(r.cache_hit_rate - r.expert_cache_hit_rate) < 0.1 THEN RAISE EXCEPTION 'r-1001: prompt-reuse rate and expert-cache rate are within 0.1 of one another - one of them has been moved into the other column'; END IF;

	SELECT count(*) INTO n FROM call_events WHERE event_key = 'strata|chatcmpl-aaa';
	IF n <> 0 THEN RAISE EXCEPTION 'the runner-side sighting created its own row instead of merging'; END IF;
	SELECT count(*) INTO n FROM call_events WHERE upstream_id = 'chatcmpl-aaa';
	IF n <> 1 THEN RAISE EXCEPTION 'chatcmpl-aaa: % rows, want exactly 1', n; END IF;

	-- B is idempotent in the strong sense: a re-pull did not invent a second surface.
	IF r.source_count IS DISTINCT FROM 2 THEN RAISE EXCEPTION 'r-1001: source_count % after re-pull + join, want 2 (rooter re-reporting itself is not a second surface)', r.source_count; END IF;

	-- D + E: the abandoned call is one row, still aborted, now carrying the key it never had.
	SELECT * INTO r FROM call_events WHERE event_key = 'rooter|r-1002';
	IF r.aborted IS DISTINCT FROM true THEN RAISE EXCEPTION 'r-1002: abandonment lost in the fallback merge'; END IF;
	IF r.upstream_id IS DISTINCT FROM 'chatcmpl-bbb' THEN RAISE EXCEPTION 'r-1002: upstream_id % - the fallback must record the key that arrived', r.upstream_id; END IF;
	IF r.join_method IS DISTINCT FROM 'backend_key_window' THEN RAISE EXCEPTION 'r-1002: join_method %', r.join_method; END IF;
	IF r.source_count IS DISTINCT FROM 2 THEN RAISE EXCEPTION 'r-1002: source_count %, want 2', r.source_count; END IF;
	IF r.has_usage IS DISTINCT FROM true THEN RAISE EXCEPTION 'r-1002: has_usage false after engine facts arrived'; END IF;
	SELECT count(*) INTO n FROM call_events WHERE event_key = 'strata|chatcmpl-bbb' OR upstream_id = 'chatcmpl-bbb';
	IF n <> 1 THEN RAISE EXCEPTION 'chatcmpl-bbb: % rows, want 1 (the fallback merged instead of inserting)', n; END IF;

	-- F + F2: calls no runner ever saw are events, they do not collide with each other, and a
	-- re-report does not duplicate them.
	SELECT count(*) INTO n FROM call_events WHERE status = 401;
	IF n <> 2 THEN RAISE EXCEPTION 'rejected calls: % rows, want 2 (F2 re-report must not add a third)', n; END IF;
	SELECT count(*) INTO n FROM call_events WHERE backend IS NULL;
	IF n <> 2 THEN RAISE EXCEPTION 'backendless calls: % rows, want 2 (NULL must not collide on any index)', n; END IF;
	SELECT count(*) INTO n FROM calls WHERE call_id = 'rooter|r-1003';
	IF n <> 1 THEN RAISE EXCEPTION 'view calls: a call with no upstream_id must fall back to event_key'; END IF;

	-- G + H: one row for a clock-less surface, flagged as such.
	SELECT * INTO r FROM call_events WHERE event_key = 'sushi|9f2c1e4a7b0d';
	IF r.ts_is_observation IS DISTINCT FROM true THEN RAISE EXCEPTION 'sushi: observed-ts flag lost'; END IF;
	IF r.version IS DISTINCT FROM 'sushi 1.1.1 (MLX 0.32.3)' THEN RAISE EXCEPTION 'sushi: version banner lost'; END IF;
	IF r.source_count IS DISTINCT FROM 1 THEN RAISE EXCEPTION 'sushi: source_count %, want 1 (H is a re-pull, not a second surface)', r.source_count; END IF;
	IF round(r.cache_hit_rate::numeric, 6) IS DISTINCT FROM 0.977806 THEN RAISE EXCEPTION 'sushi: cache_hit_rate %, want 110674/113186 = 0.977806', r.cache_hit_rate; END IF;
	-- The store's own shape. 5 calls: 2 joined across surfaces, 2 that no runner ever saw, 1
	-- abandoned, 1 with an observed timestamp, 0 with a made-up backend.
	SELECT count(*) INTO n FROM calls;
	IF n <> 5 THEN RAISE EXCEPTION 'calls view: % rows, want 5', n; END IF;
	SELECT count(*) INTO n FROM calls WHERE has_usage;
	IF n <> 3 THEN RAISE EXCEPTION 'calls_with_usage: %, want 3 (r-1001, r-1002, sushi)', n; END IF;
	SELECT count(*) INTO n FROM calls WHERE NOT has_usage;
	IF n <> 2 THEN RAISE EXCEPTION 'calls_without_usage: %, want 2 (both rejected calls)', n; END IF;
	SELECT count(*) INTO n FROM calls WHERE aborted;
	IF n <> 1 THEN RAISE EXCEPTION 'calls_aborted: %, want 1 (the abandoned stream is now counted somewhere at all)', n; END IF;
	SELECT count(*) INTO n FROM calls WHERE source_count > 1;
	IF n <> 2 THEN RAISE EXCEPTION 'joined across surfaces: %, want 2', n; END IF;
	SELECT count(*) INTO n FROM calls WHERE join_method = 'backend_key_window';
	IF n <> 1 THEN RAISE EXCEPTION 'joined by fallback: %, want 1', n; END IF;
	SELECT count(*) INTO n FROM calls WHERE ts_is_observation;
	IF n <> 1 THEN RAISE EXCEPTION 'observed timestamps: %, want 1', n; END IF;

	-- The quality view must be able to answer "how much of what I display depends on a heuristic".
	SELECT * INTO q FROM call_quality WHERE backend = 'strata';
	IF q.calls <> 2 THEN RAISE EXCEPTION 'strata calls = %, want 2 (r-1001, r-1002)', q.calls; END IF;
	IF q.calls_joined_across_surfaces <> 2 THEN RAISE EXCEPTION 'strata joined = %, want 2', q.calls_joined_across_surfaces; END IF;
	IF q.calls_joined_by_fallback <> 1 THEN RAISE EXCEPTION 'strata by_fallback = %, want 1', q.calls_joined_by_fallback; END IF;
	IF q.calls_aborted <> 1 THEN RAISE EXCEPTION 'strata aborted = %, want 1', q.calls_aborted; END IF;
	IF q.calls_with_observed_ts <> 0 THEN RAISE EXCEPTION 'strata observed_ts = %, want 0', q.calls_with_observed_ts; END IF;
	IF q.calls_without_usage <> 0 THEN RAISE EXCEPTION 'strata without_usage = %, want 0 (both learned usage)', q.calls_without_usage; END IF;
	IF q.calls_with_expert_cache_rate <> 1 THEN RAISE EXCEPTION 'strata expert_cache coverage = %, want 1 (only r-1001 carries the runner''s expert rate)', q.calls_with_expert_cache_rate; END IF;

	SELECT * INTO q FROM call_quality WHERE backend IS NULL;
	IF q.calls <> 2 THEN RAISE EXCEPTION 'backendless quality = %, want 2', q.calls; END IF;
	IF q.calls_with_usage <> 0 THEN RAISE EXCEPTION 'rejected calls must never be usable in an aggregate'; END IF;

	SELECT * INTO q FROM call_quality WHERE backend = 'sushi';
	IF q.calls <> 1 THEN RAISE EXCEPTION 'sushi calls = %, want 1', q.calls; END IF;
	IF q.calls_with_observed_ts <> 1 THEN RAISE EXCEPTION 'sushi observed_ts = %, want 1', q.calls_with_observed_ts; END IF;

	RAISE NOTICE 'contract holds: 5 calls, 2 joined across surfaces (1 by upstream_id, 1 by fallback), 1 abandoned stream counted at all, 2 unusable for aggregates, 1 observed timestamp flagged';
END $$;

-- ══ I. the cache rates cannot be supplied ═════════════════════════════════════════════
-- The mistake this guard exists for is specific and plausible: Strata reports `hit_rate` between
-- 0.802 and 0.903, it is the MoE expert cache's rate (its own serve/prometheus.py says so), and
-- the same calls reuse 99.2-99.96% of their prompt. Both are 0-1, both get called a cache rate.
-- A writer able to set cache_hit_rate will eventually set it to the wrong one, and the row will
-- still look healthy.
DO $$
DECLARE
	rejected boolean;
BEGIN
	-- Smuggling the expert rate into the prompt-reuse column: right shape, right range, wrong
	-- quantity. Nothing about the row would reveal it afterwards.
	rejected := false;
	BEGIN
		UPDATE call_events SET expert_cache_hit_rate = 0.848, cache_hit_rate = 0.848
			WHERE event_key = 'rooter|r-1001';
	EXCEPTION WHEN raise_exception THEN
		rejected := true;
	END;
	IF NOT rejected THEN
		RAISE EXCEPTION 'rates: an UPDATE that put the expert rate into cache_hit_rate was accepted';
	END IF;

	-- A rate whose numerator was removed by the same statement. This is a merge that fills a gap
	-- with a number and never fills the components the number came from.
	rejected := false;
	BEGIN
		UPDATE call_events SET cached_tokens = NULL, cache_hit_rate = 0.9 WHERE event_key = 'rooter|r-1001';
	EXCEPTION WHEN raise_exception THEN
		rejected := true;
	END;
	IF NOT rejected THEN
		RAISE EXCEPTION 'rates: a cache_hit_rate with no cached_tokens was accepted';
	END IF;

	-- More cached than prompt: two surfaces describing different calls, agreeing by accident. This
	-- is the fallback join failing, and it must be loud.
	rejected := false;
	BEGIN
		UPDATE call_events SET cached_tokens = 120000 WHERE event_key = 'rooter|r-1001';
	EXCEPTION WHEN raise_exception THEN
		rejected := true;
	END;
	IF NOT rejected THEN
		RAISE EXCEPTION 'rates: cached_tokens exceeding prompt_tokens was accepted';
	END IF;

	-- No denominator, so nothing in the row supports a rate.
	rejected := false;
	BEGIN
		UPDATE call_events SET prompt_tokens = NULL, cache_hit_rate = 0.5 WHERE event_key = 'rooter|r-1001';
	EXCEPTION WHEN raise_exception THEN
		rejected := true;
	END;
	IF NOT rejected THEN
		RAISE EXCEPTION 'rates: a cache_hit_rate with no prompt_tokens was accepted';
	END IF;

	-- Positive control: change the numerator and the stored rate moves on its own, while the engine
	-- rate stays put because its own component did not change. The derivation follows the
	-- components, and the two rates stay separate measurements.
	UPDATE call_events SET cached_tokens = 100000 WHERE event_key = 'rooter|r-1001';
	IF round((SELECT cache_hit_rate FROM call_events WHERE event_key = 'rooter|r-1001')::numeric, 6)
		IS DISTINCT FROM round(100000.0 / 106718.0, 6) THEN
		RAISE EXCEPTION 'rates: cache_hit_rate did not follow cached_tokens';
	END IF;
	IF round((SELECT engine_cache_hit_rate FROM call_events WHERE event_key = 'rooter|r-1001')::numeric, 6)
		IS DISTINCT FROM 0.999616 THEN
		RAISE EXCEPTION 'rates: engine_cache_hit_rate moved when only cached_tokens changed';
	END IF;

	RAISE NOTICE 'rates: expert rate cannot be smuggled into a prompt-reuse column, an unsourced rate is rejected, a rate follows its own components';
END $$;

ROLLBACK;
