-- 003_usage_semantics.sql - llama.cpp's prompt_n is not the prompt, and a rate that cannot be derived
-- is not a reason to lose a call.
--
-- Two changes from the same source (rooter's own contract notes, checked against its live ring), and
-- one of them reverses a decision this schema made confidently.
--
-- 1. prompt_tokens_computed.
--
-- llama.cpp reports `prompt_n` for a request. rooter's finding is that it is not the whole prompt - it
-- is the tokens the runner processed on this pass, which is not the same quantity as the prompt the
-- client sent. Publishing an arithmetic answer next to the reported count (rather than overwriting it)
-- is the only way to keep the discrepancy visible. Live from rooter's ring, first record after a
-- restart: prompt_tokens 98060, prompt_tokens_computed 98060, extra.prompt_n 98060 - equal, which is
-- the boring case. The columns exist apart so the unbalanced cases are countable instead of averaged.
--
-- 2. cached_tokens > prompt_tokens stopped being an exception.
--
-- This schema used to RAISE when the components made a ratio greater than 1, on the theory that only a
-- bad join could do that. The theory was wrong in a way that cost real data: llama.cpp's own numbers
-- can legitimately point that way when a cached span is larger than the tokens the runner counted on
-- this pass, and when it happened the whole INSERT was rejected. A router had fully witnessed a call,
-- the store dropped it, and the counter reported a write failure - not "this one rate is unusable".
-- rooter reached the same conclusion independently: its API returns a NULL cache_hit_rate rather than
-- a ratio above 1.
--
-- So the rate goes NULL, the components stay, and cache_rate_undefined names the state in the row. A
-- CLAIMED rate is still refused, in both directions: a number arriving with components that cannot
-- support it came from somewhere else, which is precisely how Strata's expert-cache hit_rate would
-- smuggle itself into a prompt-reuse column.

ALTER TABLE call_events ADD COLUMN IF NOT EXISTS
	prompt_tokens_computed integer check (prompt_tokens_computed is null or prompt_tokens_computed >= 0);

-- A generated column added to an existing table is computed for the rows already there, so an
-- installed database does not end up with a flag that is NULL on old rows and boolean on new ones.
ALTER TABLE call_events ADD COLUMN IF NOT EXISTS cache_rate_undefined boolean GENERATED ALWAYS AS (
	(prompt_tokens is not null and prompt_tokens > 0 and (
		(cached_tokens is not null and cached_tokens > prompt_tokens)
		or (engine_cached_tokens is not null and engine_cached_tokens > prompt_tokens)
	))
) STORED;

-- 001 owns the trigger body, and it re-runs safely, so the rate policy is fixed there rather than
-- duplicated here. What is checked below is that the fix actually landed in whichever database this
-- file just ran against - because the difference between "the exception was removed" and "the exception
-- is still there and you have not noticed yet" is a call going missing in production.
DO $$
DECLARE
	body text;
BEGIN
	SELECT pg_get_functiondef('call_events_derive_rates'::regproc) INTO body;
	IF body IS NULL THEN
		raise exception 'call_events_derive_rates is missing; 001 did not run';
	END IF;
	-- Precise sentinels, because the wording survived into the new messages on purpose: the claim guards
	-- still say "exceeds prompt_tokens". What must NOT be present is the old unconditional refusal, which
	-- began its message with the components rather than with the claimed rate.
	IF position('EXCEPTION ''call_events: cached_tokens %' in body) > 0 THEN
		raise exception '003 wrong: the trigger still refuses a row whose cached_tokens exceed prompt_tokens. The rate should go NULL and cache_rate_undefined should say so; keeping that exception is what lost fully witnessed calls';
	END IF;
	IF position('EXCEPTION ''call_events: engine_cached_tokens %' in body) > 0 THEN
		raise exception '003 wrong: the trigger still refuses a row whose engine_cached_tokens exceed prompt_tokens, for the same reason as above';
	END IF;
	IF position('NEW.cache_hit_rate := NULL' in body) = 0 THEN
		raise exception '003 wrong: the trigger does not null an undeterminable cache rate';
	END IF;
	RAISE NOTICE 'usage semantics: prompt_tokens_computed stored apart from prompt_tokens, and an undeterminable rate is NULL and flagged rather than a rejected row';
END $$;
