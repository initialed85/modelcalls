-- 004_first_content.sql - the gap between "the runner answered" and "the reader saw a token".
--
-- rooter publishes two timings from the same anchor: ttft_ms is the first byte of the response,
-- first_content_ms is the first token of *content*. For a reasoning model they are not close
-- together, and the difference is the number that says a box was busy while nothing had failed - the
-- measured range from its live ring today is a ttft of 24ms against a first content token 488ms later
-- on one call, and 2350ms against 32666ms on another. A dashboard on ttft_ms alone reports a healthy
-- fast box in the second case.
--
-- This store was dropping the field: the harvester polls a ring that carries it and the schema had no
-- column for it, which is the quietest kind of data loss - no error, no counter, just a fact that was
-- measured and never stored.
--
-- first_content_gap_ms is a generated column rather than something the writer computes, for the same
-- reason the rates are: the two components can arrive at different times. A row written by rooter's
-- ring may have ttft_ms and not first_content_ms (an aborted stream), and a later merge fills the
-- other half. A stored generated column recalculates on that UPDATE and cannot be set by a client, so
-- the gap can never disagree with the two numbers it is derived from, and no surface can publish its
-- own idea of it.
--
-- No CHECK on the gap. It is `first_content_ms - ttft_ms` and those two come from one observer's
-- clock, but if a surface ever produced a content token recorded *before* the first byte, that is a
-- contradiction worth seeing as a negative number and counting in call_quality - not a row to reject
-- and a measurement to lose.

-- Fresh databases get these from 001's CREATE TABLE. These statements are for the databases that
-- already exist, which is every database this store has ever had.
ALTER TABLE call_events ADD COLUMN IF NOT EXISTS
	first_content_ms double precision check (first_content_ms is null or first_content_ms >= 0);

-- A generated column added to an existing table is computed for the rows already in it, so an installed
-- database does not end up with a column that is NULL on the old calls and real on the new ones.
ALTER TABLE call_events ADD COLUMN IF NOT EXISTS first_content_gap_ms double precision
	GENERATED ALWAYS AS (first_content_ms - ttft_ms) STORED;

-- The point of this file existing rather than only editing 001: an installed database runs 001 again on
-- every boot (CREATE TABLE IF NOT EXISTS adds nothing), so 001's views reference columns 001 does not
-- add. The DO block below is what proves the ordering actually resolves in a database that predates
-- them, because "the migration order works" is the kind of claim that is only ever tested on a database
-- that was created after the fact.
DO $$
DECLARE
	missing int;
BEGIN
	SELECT count(*) INTO missing FROM (
		VALUES ('first_content_ms'), ('first_content_gap_ms')
	) AS wanted(column_name)
	WHERE NOT EXISTS (
		SELECT 1 FROM information_schema.columns c
		WHERE c.table_name = 'call_events' AND c.column_name = wanted.column_name
	);
	IF missing > 0 THEN
		raise exception '004 did not land: % column(s) of first_content_ms / first_content_gap_ms are absent from call_events', missing;
	END IF;

	-- Generated, not merely present: a plain column here would be settable by a writer, which is how a
	-- gap that disagrees with its own two components gets into a dashboard.
	IF (SELECT attgenerated FROM pg_attribute
		WHERE attrelid = 'call_events'::regclass AND attname = 'first_content_gap_ms') <> 's' THEN
		raise exception '004 wrong: call_events.first_content_gap_ms is not a STORED generated column, so a surface could write its own idea of it';
	END IF;

	-- And the view dashboards actually read has to expose both, on this database, now.
	SELECT count(*) INTO missing FROM (
		VALUES ('first_content_ms'), ('first_content_gap_ms')
	) AS wanted(column_name)
	WHERE NOT EXISTS (
		SELECT 1 FROM information_schema.columns c
		WHERE c.table_name = 'calls' AND c.column_name = wanted.column_name
	);
	IF missing > 0 THEN
		raise exception '004 incomplete: the calls view is missing % column(s) - a panel reading calls would not see the gap at all', missing;
	END IF;
END $$;
