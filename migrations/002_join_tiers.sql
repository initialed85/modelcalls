-- 002_join_tiers.sql - split the fallback join into the two things it actually is.
--
-- WHY THIS EXISTS AS A SEPARATE FILE
--
-- 001 is the initial schema, written as CREATE TABLE IF NOT EXISTS / CREATE ... IF NOT EXISTS so it is
-- safe to re-run. That property is also a trap: on a database that already has call_events, 001's new
-- join_method CHECK is never applied, because the table already exists and CREATE TABLE IF NOT EXISTS
-- does not edit constraints. A fresh install would get four allowed join methods and an installed one
-- would keep three, and the failure would show up as a rejected INSERT at the moment the store starts
-- doing the interesting thing - not as a migration error at the moment you can still do something about
-- it. Nothing is deployed yet, so this could have been folded into 001 and nobody would have noticed.
-- Splitting it is the difference between "works on my empty database" and "works on the one you have".
--
-- WHAT CHANGED, AND WHY IT IS TWO NAMES AND NOT ONE
--
-- Live capture: 35 rooter ring events against 12 strata engine records. Some pairs agreed to the token
-- (prompt 66933/66933, cached 66509/66509, completion 549/549) while the router's total_ms differed by
-- 450 ms, because a router's number includes the router. Others agreed on nothing but a clock - rooter
-- event /32 90 ms from a strata record reporting 49903 prompt tokens against its 66933, rooter event
-- /40 100 ms from another reporting 183 completion tokens against 1395. Calling those the same call was
-- wrong, and the join did it silently and reported success.
--
-- So a join that rests on measured agreement and a join that rests on a clock are recorded under
-- different names. Nothing else here changes: no rows are rewritten, no column is added, and the old
-- vocabulary is a strict subset of the new one, which is why this migration cannot fail on data.
--
-- The store is not deployed yet, so a database this runs against is a development one. It is written to
-- be a no-op on a database that 001 already brought up to date, which is the state CI will see.

-- The constraint Postgres generated for us is named after the table and column, but that name is not a
-- promise - so find it by what it checks rather than by what it is called.
DO $$
DECLARE
	c record;
	found boolean := false;
BEGIN
	FOR c IN
		SELECT conname FROM pg_constraint
		 WHERE conrelid = 'call_events'::regclass
		   AND contype = 'c'
		   AND pg_get_constraintdef(oid) LIKE '%join_method%'
	LOOP
		EXECUTE format('ALTER TABLE call_events DROP CONSTRAINT %I', c.conname);
		found := true;
	END LOOP;
	IF NOT found THEN
		RAISE NOTICE 'join tiers: call_events has no join_method check to replace; adding one';
	END IF;
END $$;

ALTER TABLE call_events ADD CONSTRAINT call_events_join_method_check CHECK (
	join_method in ('self', 'upstream_id', 'usage_content_window', 'backend_key_window')
);

DO $$
BEGIN
	IF EXISTS (
		SELECT 1 FROM pg_constraint
		 WHERE conrelid = 'call_events'::regclass
		   AND conname = 'call_events_join_method_check'
		   AND pg_get_constraintdef(oid) LIKE '%usage_content_window%'
	) THEN
		RAISE NOTICE 'join tiers: usage_content_window and backend_key_window are separate join methods';
	ELSE
		RAISE NOTICE 'join tiers: constraint applied, but it does not name both tiers - check by hand';
	END IF;
END $$;
