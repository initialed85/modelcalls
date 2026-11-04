-- 005_prompt_provenance.sql - say which prompt number is whose arithmetic.
--
-- This file exists because of one row. From rooter's live ring, 2026-11-04:
--
--   backend=sushi  prompt_tokens=72292  prompt_tokens_computed=742  cached_tokens=71550
--                  extra.prompt_n=72292  extra.cached_n=71550  runner_prompt_n_includes_cache=true
--
--   backend=strata prompt_tokens=60174  prompt_tokens_computed=140  cached_tokens=60034
--                  extra.prompt_n=140    extra.cache_n=60034         runner_prompt_n_includes_cache=<absent>
--
-- The two rows look like the same shape and are not. Strata's `prompt_n` is the work done this pass,
-- so `prompt_tokens_computed` is the runner's number verbatim. Sushi's `prompt_n` is the FULL prompt
-- length - the engine says so, with the field above - so `prompt_tokens_computed` is rooter's
-- subtraction, 72292 - 71550. Same column, two meanings, and the only thing that distinguishes them
-- is a field this store was not keeping.
--
-- Across the current event ring (67 rows, the last ~100 min): every row with a prompt count satisfies
-- `cached_tokens + prompt_tokens_computed = prompt_tokens`, which is the trap - the equality looks
-- like two independent measurements agreeing, and on most rows one of the three numbers was computed
-- from the other two. It is not evidence of anything, and the store's own history proves it harder
-- than the ring does. The 628 rows the harvester has kept show the same column meaning two things:
--
--   140 sushi rows, 06:34-07:21  prompt_tokens_computed == prompt_tokens - cached_tokens
--   174 sushi rows, 04:58-06:20  prompt_tokens_computed == prompt_tokens, cached_tokens large
--
-- So rooter's computed count changed meaning in the middle of one day, under one column name, and
-- both meanings are in the table now. The `version` column is NULL on every one of them, so the
-- change cannot be attributed to a build. That is the argument for this flag in one line: a column
-- whose meaning moves is not a fact, and an equality the writer produced is not corroboration.
--
-- What the flag does NOT do is fix the old rows, and it cannot - the information did not exist when
-- they were written, and the ring has already rolled past them. Any query spanning 06:20 today reads
-- two definitions, and the flag's absence is what tells you those rows are untrustworthy rather than
-- merely incomplete.
--
-- The store's whole design is that a computed estimate never silently becomes a reported count, and
-- that is what it does with prompt_tokens_computed vs prompt_tokens - but the provenance of the pair
-- was being inferred from the backend's identity, and the inference is wrong. Recording the flag is
-- cheaper than being sure.
--
-- prompt_tokens_derived is rooter's own declaration for the other side of the same question: whether
-- prompt_tokens is usage.prompt_tokens read from the wire, or prompt_n + cached_n that rooter added.
-- It does not exist yet (0 of 628 stored rows carry it), and the column is added now so the store has
-- somewhere to put it when it does. The reason it is a column and not a reading of `extra` is that
-- the field is load-bearing for the denominator of every cache rate in the store, and a dashboard
-- should not have to unpack JSONB to know whether its denominator is arithmetic.
--
-- Both are NULL rather than FALSE when absent. Absent means "the surface did not say", and reading it
-- as "not derived" would launder the unknown into a fact - the exact mistake this store documents
-- elsewhere. call_quality counts the three states separately so an unknown is visible.

-- Fresh databases get these from 001's CREATE TABLE; these statements are for databases that exist.
ALTER TABLE call_events ADD COLUMN IF NOT EXISTS runner_prompt_n_includes_cache boolean;
ALTER TABLE call_events ADD COLUMN IF NOT EXISTS prompt_tokens_derived boolean;

DO $$
DECLARE
	missing int;
BEGIN
	SELECT count(*) INTO missing FROM (
		VALUES ('runner_prompt_n_includes_cache'), ('prompt_tokens_derived')
	) AS wanted(column_name)
	WHERE NOT EXISTS (
		SELECT 1 FROM information_schema.columns c
		WHERE c.table_name = 'call_events' AND c.column_name = wanted.column_name
	);
	IF missing > 0 THEN
		raise exception '005 did not land: % column(s) of runner_prompt_n_includes_cache / prompt_tokens_derived are absent from call_events', missing;
	END IF;

	-- And the projection dashboards read exposes them, on this database, now.
	SELECT count(*) INTO missing FROM (
		VALUES ('runner_prompt_n_includes_cache'), ('prompt_tokens_derived')
	) AS wanted(column_name)
	WHERE NOT EXISTS (
		SELECT 1 FROM information_schema.columns c
		WHERE c.table_name = 'calls' AND c.column_name = wanted.column_name
	);
	IF missing > 0 THEN
		raise exception '005 incomplete: the calls view is missing % column(s) - a panel could not tell a verbatim prompt_n from a subtraction', missing;
	END IF;
END $$;
