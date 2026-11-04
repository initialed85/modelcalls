-- legacy_upgrade_path.sql — the migration path a fresh CI database never takes, and a running one takes
-- every time a migration file is edited.
--
-- test/contract_scenarios.sql always runs against an empty database (it says so and refuses to run
-- otherwise). That is right for the join semantics, and it means those scenarios never touch the one thing
-- that actually breaks in production: 001 running a SECOND time on a database that already has rows,
-- already has views with columns, and already has a named CHECK constraint. On a fresh install everything
-- in 001 is a CREATE and Postgres is permissive. On an installed database it is ALTER TABLE, a constraint
-- swap, and CREATE OR REPLACE VIEW over an existing view - and Postgres is strict there.
--
-- So this file builds the installed database instead of assuming it. test/legacy_schema_snapshot.sql is a
-- verbatim copy of the first schema ever shipped (431c17e); it is applied, given a row of real data, and
-- then the current migrations are run on top of it in the order the harvester runs them. The assertions
-- are about the upgrade: that columns were ADDED rather than renumbered, that a view gained its new
-- columns at the END because Postgres allows it nowhere else, that a row written before the upgrade is
-- still readable afterwards, and that the join tier a live database was using is not invalidated by the
-- constraint being replaced.
--
-- Everything below rolls back, so the schema stays upgraded for whoever runs next and the fixture rows do
-- not leak into the Go tests that follow.

-- A database that has been in use since the first schema shipped.
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

\ir legacy_schema_snapshot.sql

-- A live database is not an empty one. This row is deliberately written in the old vocabulary - the
-- fallback tier name that no longer exists, a timing-only join with no content proof, and none of the
-- columns that did not exist yet - because it is the row that has to survive every ALTER in 001, and it
-- is the row that would expose a CHECK constraint being re-added while a violation still exists.
INSERT INTO call_events (event_key, source, source_ids, id, ts, backend, "key", status, aborted,
	bytes, ttft_ms, total_ms, prompt_ms, decode_ms, join_method)
VALUES ('rooter|r-legacy-1', 'rooter', ARRAY['rooter|r-legacy-1']::text[], 'r-legacy-1',
	'2026-09-01T00:00:00Z', 'strata', 'claude-code', 200, false,
	1048576, 115.8, 43632.0, 43000.0, 620.0, 'backend_key_window');

-- The upgrade itself, in the order the harvester applies it. No BEGIN/ROLLBACK wrapper here: each file
-- commits in its own transaction, which is what MigrateLatest does. Wrapping them would make this test
-- exercise a rollback-capable upgrade that no process performs.
\ir ../internal/store/migrations/001_initial.sql
\ir ../internal/store/migrations/002_join_tiers.sql
\ir ../internal/store/migrations/003_usage_semantics.sql
\ir ../internal/store/migrations/004_first_content.sql

-- ══ the assertions ═══════════════════════════════════════════════════════════════════
BEGIN;

DO $$
DECLARE
	c text;
BEGIN
	-- 1. Columns were ADDED. A migration that renames or renumbers a column on a database with rows in
	-- it is a migration that loses data, and "IF NOT EXISTS" is what says so.
	FOREACH c IN ARRAY ARRAY['prompt_tokens_computed', 'cache_rate_undefined',
		'engine_cache_hit_rate', 'expert_cache_hit_rate', 'first_content_ms', 'first_content_gap_ms']
	LOOP
		IF NOT EXISTS (SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'call_events' AND column_name = c) THEN
			RAISE EXCEPTION 'upgrade: call_events is missing % after migrating from the first shipped schema', c;
		END IF;
	END LOOP;
	RAISE NOTICE 'upgrade: the columns that were added later all exist on a database that started without them';
END $$;

DO $$
DECLARE
	c text;
	n int;
	tail_cols text[];
BEGIN
	-- 2. The `calls` view survived being replaced, and its new columns went at the END - which is not a
	-- style preference. Postgres refuses to change the name or position of an existing view column, so a
	-- column slotted into the middle of the select list fails on an installed database and succeeds on a
	-- fresh one. That asymmetry is the whole trap: the lane is green and the deploy dies at boot, because
	-- the harvester migrates before it serves.
	IF NOT EXISTS (SELECT 1 FROM information_schema.columns
		WHERE table_name = 'calls' AND column_name = 'prompt_ms') THEN
		RAISE EXCEPTION 'upgrade: calls lost prompt_ms, i.e. an existing view column was renamed or moved';
	END IF;

	-- The additions have to be the LAST columns, in the order they were added. That is the whole
	-- constraint, and the fact that this view was replaced at all proves the columns before them kept
	-- their positions: CREATE OR REPLACE VIEW refuses any other change to an existing column.
	SELECT array_agg(x.column_name ORDER BY x.ordinal_position) INTO tail_cols
		FROM (SELECT column_name, ordinal_position FROM information_schema.columns
			WHERE table_name = 'calls' ORDER BY ordinal_position DESC LIMIT 4) x;
	IF tail_cols <> ARRAY['engine_cache_hit_rate', 'expert_cache_hit_rate', 'first_content_ms',
		'first_content_gap_ms'] THEN
		RAISE EXCEPTION 'upgrade: the last four columns of calls are %, and they should be the four added since the first schema - anything else means a new column was inserted before existing ones, and Postgres only accepts additions at the end', tail_cols;
	END IF;

	SELECT count(*) INTO n FROM information_schema.columns WHERE table_name = 'calls';
	RAISE NOTICE 'upgrade: right - calls now has % columns, and the four newest are the four that were added since the first schema, in order', n;
END $$;

DO $$
DECLARE
	r record;
	gap double precision;
BEGIN
	-- 3. A row written before the upgrade is still readable, with its old join tier intact, and the
	-- columns it never had are NULL rather than 0.
	SELECT * INTO r FROM calls WHERE call_id = 'rooter|r-legacy-1';
	IF r.call_id IS NULL THEN
		RAISE EXCEPTION 'upgrade: the row that predates the migration disappeared';
	END IF;
	IF r.join_method <> 'backend_key_window' THEN
		RAISE EXCEPTION 'upgrade: a pre-upgrade row reports join_method %; the constraint swap rewrote history it should not touch', r.join_method;
	END IF;
	IF r.first_content_ms IS NOT NULL OR r.first_content_gap_ms IS NOT NULL THEN
		RAISE EXCEPTION 'upgrade: a call that predates the first-content columns has a measurement of % / %', r.first_content_ms, r.first_content_gap_ms;
	END IF;
	RAISE NOTICE 'upgrade: right - the pre-upgrade row is intact: % on % measured % ms total, with no thinking gap because nobody measured one',
		r.join_method, r.backend, r.total_ms;
END $$;

DO $$
DECLARE
	accepted boolean := false;
	msg text;
	gap double precision;
BEGIN
	-- 4. The join tier that did not exist when this database was built is accepted after the upgrade,
	-- even though the table already holds rows: that is why 001 re-adds the constraint NOT VALID and lets
	-- a later validation pass scan the existing rows rather than failing the ALTER that has to happen at
	-- process start.
	BEGIN
		INSERT INTO call_events (event_key, source, source_ids, id, ts, backend, status, ttft_ms,
			first_content_ms, join_method)
		VALUES ('rooter|r-legacy-2', 'rooter', ARRAY['rooter|r-legacy-2']::text[], 'r-legacy-2',
			'2026-11-04T11:00:00Z', 'sushi', 200, 24.0, 488.7, 'usage_content_window');
		accepted := true;
	EXCEPTION WHEN others THEN
		GET STACKED DIAGNOSTICS msg = message_text;
		RAISE EXCEPTION 'upgrade: the usage_content_window join tier was rejected on a database that predates it: %', msg;
	END;
	IF NOT accepted THEN
		RAISE EXCEPTION 'upgrade: usage_content_window was not accepted';
	END IF;

	SELECT first_content_gap_ms INTO gap FROM calls WHERE call_id = 'rooter|r-legacy-2';
	IF gap IS NULL OR round(gap::numeric, 6) <> round((488.7 - 24.0)::numeric, 6) THEN
		RAISE EXCEPTION 'upgrade: calls reports gap % for an upgraded table, expected 464.7 - the generated column did not survive the view being replaced', gap;
	END IF;
	RAISE NOTICE 'upgrade: right - a call written after the upgrade gets its thinking gap through the view: % ms', round(gap::numeric, 1);
END $$;

DO $$
DECLARE
	c text;
	n int;
BEGIN
	-- 5. call_quality is replaced rather than replaced-with-renames, so both the renamed tier count and
	-- the new coverage counts are visible to whoever reads it - and the name that was retired stays
	-- retired, because a view that quietly keeps an old column is a panel that quietly keeps reporting it.
	IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'call_quality'
		AND column_name = 'calls_joined_by_timing_only') THEN
		RAISE EXCEPTION 'upgrade: call_quality has no calls_joined_by_timing_only';
	END IF;
	IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'call_quality'
		AND column_name = 'calls_joined_by_fallback') THEN
		RAISE EXCEPTION 'upgrade: call_quality still reports calls_joined_by_fallback, the name that was retired';
	END IF;
	FOREACH c IN ARRAY ARRAY['calls_with_first_content_ms',
		'calls_with_first_content_gap', 'mean_first_content_ms', 'mean_first_content_gap_ms',
		'calls_with_negative_first_content_gap']
	LOOP
		SELECT count(*) INTO n FROM information_schema.columns
			WHERE table_name = 'call_quality' AND column_name = c;
		IF n = 0 THEN
			RAISE EXCEPTION 'upgrade: call_quality is missing %, so the metric a panel would read does not exist', c;
		END IF;
	END LOOP;
	RAISE NOTICE 'upgrade: call_quality carries the renamed tier counts and the first-content coverage';
END $$;

ROLLBACK;
