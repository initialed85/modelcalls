# modelcalls

A store of **model calls** — one row per call to a local LLM runner, assembled from whatever
surfaces know about it.

Strata, sushi and [rooter](https://github.com/initialed85/rooter) all describe the same call with
different facts at different times. Rooter knows who asked, which backend it chose, how long the
call queued, what status came back, and whether the client hung up. The runner knows what it
actually did: tokens, cache reuse, prefill and decode rates, speculative-decode acceptance, its own
build. Neither one can answer the questions I want to ask alone.

**One row per call, not one row per sighting.** A row per sighting would make every well-observed
call count twice, which is not missing data but fabricated data — and the fabrication would land
exactly where the data is best, so the graphs would look most trustworthy when they were most wrong.

## The write protocol

Everything here is one question, asked in a fixed order: *have I already seen this call?*

| # | condition | outcome |
|---|---|---|
| 1 | same `event_key` | merge. This source is re-reporting; it is **not** a second witness |
| 2 | same `upstream_id`, different source | merge into the row that holds that id, and record `join_method = upstream_id`. **This is the join** |
| 3 | narrow heuristic, **exactly one** candidate | merge, `join_method = backend_key_window` |
| 4 | otherwise | insert, `join_method = self` |

All four run through one SQL statement: they all merge a fact-set into a row that either exists or
does not, and the only differences are which `event_key` is named and whether `join_method` is
overridden.

The merge rule, applied everywhere:

> **a fact that is now known fills a gap; a fact that is not known must not overwrite a fact that
> was.** A NULL from a surface that never had the information is absence, not evidence.

Path 3 exists for one specific hole. When a client abandons a stream, the runner's `chatcmpl` id
lived in the part of the response that never arrived, so the router's row has no `upstream_id` and
nothing can collide — and the engine that *did* finish the work has its own record. Without a
fallback, one call becomes two rows. The predicate is deliberately narrow (same backend, same
session key, timestamp within 2s, a row with no id and no usage yet) and it **refuses to guess**:
two candidates stays two visibly unjoined rows.

## What the schema refuses to do

- **`has_usage` is generated** (`prompt_tokens IS NOT NULL AND completion_tokens IS NOT NULL`), so
  no writer can disagree with it. Without it, "average tokens/sec this week" divides a denominator
  built from finished calls by a count that includes abandoned ones, and the answer is plausible
  enough to put on a dashboard.
- **`ts_is_observation`** for surfaces with no clock. sushi's log lines carry no timestamp, so `ts`
  is when the probe read the line — and a graph that does not say that is a graph with invented
  precision. A real timestamp replaces an observed one, in both directions.
- **`cache_hit_rate` is `cached_tokens / prompt_tokens`, and nothing else.** Strata's own `hit_rate`
  is *not* that ratio: measured across its 12 retained requests, `reused/prompt_tokens` sits at
  0.9924–0.9996 while `hit_rate` sits at 0.802–0.903, and none of the twelve agree within 0.02.
  Strata runs an MoE expert cache (`expert_slots`, `expert_cache_mib`), which is the obvious
  candidate for what `hit_rate` counts — but it is not established, so it gets no column. It stays
  at `extra.strata.hit_rate` until its meaning is known. An unexplained number promoted to a
  first-class column becomes a legend that lies.
- **`usage_source` names the mechanism, not the component**: `none | client | injected |
  runner_scrape`. `injected` means a usage frame exists because the observer added
  `include_usage=true` — measurement affecting the measurement, worth knowing when you read the
  token totals. There is deliberately no `wire`: every usage frame arrives over the wire, so the
  value would be unfalsifiable.
- **`extra` is namespaced by source by the store.** A writer's payload lands under its `source`
  name. If two surfaces both call their payload `raw`, a merge would silently turn *who said what*
  into *who wrote last*.
- **`call_quality` reports the store's own weaknesses as data**: `calls_without_usage`,
  `calls_joined_by_fallback`, `calls_with_observed_ts`. Any panel built on `calls` can be asked how
  much of its number depends on a heuristic.

## Reading the store

`calls` is one row per call, with `call_id` as the stable handle: `upstream_id` when one exists, the
store key when nothing identified the call. `call_quality` aggregates the shape per backend and
day.

## Running the contract

Two halves of one contract. `test/contract_scenarios.sql` is the protocol as Postgres sees it;
`internal/store` mirrors it through the production write path. If the SQL passes and the Go fails,
the code is doing something the schema never promised.

```bash
docker run -d --name mc-pg -e POSTGRES_USER=mc -e POSTGRES_PASSWORD=mc \
  -e POSTGRES_DB=modelcalls -p 127.0.0.1:15432:5432 postgres:16

docker exec -i mc-pg psql -U mc -d modelcalls -v ON_ERROR_STOP=1 \
  -f migrations/001_initial.sql           # then -f test/contract_scenarios.sql

MODELCALLS_TEST_DSN='postgres://mc:mc@127.0.0.1:15432/modelcalls?sslmode=disable' \
  go test ./...
```

A real Postgres 16 is required. The contract *is* Postgres — partial unique indexes, `ON CONFLICT`
merge arithmetic, generated columns, jsonb concatenation — and there is no useful version of this
against a fake database. [Woodpecker CI](https://woodpecker.dev.initialed85.cc/initialed85/modelcalls)
runs both halves against a `postgres:16` service on every push.

## Not built yet

The harvester that reads rooter's event ring and the runners' surfaces, the probe that tails sushi's
log, and the Postgres this all writes to in home-dev. Publishing a container that writes to a
database nobody has provisioned would only be a way to be wrong in more places.
