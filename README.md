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
| 3 | same call, measured agreement, **exactly one** candidate | merge, `join_method = usage_content_window` |
| 4 | same call, nothing measured, **exactly one** candidate | merge, `join_method = backend_key_window` |
| 5 | otherwise | insert, `join_method = self` |

All four run through one SQL statement: they all merge a fact-set into a row that either exists or
does not, and the only differences are which `event_key` is named and whether `join_method` is
overridden.

The merge rule, applied everywhere:

> **a fact that is now known fills a gap; a fact that is not known must not overwrite a fact that
> was.** A NULL from a surface that never had the information is absence, not evidence.

Path 3 and path 4 exist for one specific hole. When a client abandons a stream, the runner's
`chatcmpl` id lived in the part of the response that never arrived, so the router's row has no
`upstream_id` and nothing can collide — and the engine that *did* finish the work has its own record.
Without a fallback, one call becomes two rows. The predicate is deliberately narrow, and it has two
tiers because a measurement and a clock are not the same kind of evidence:

- same backend;
- **exactly one side carries a session key**. A key is a witness that a client asked. Two rows that
  both have one are two calls (or they would have joined on the runner's id); two rows that both
  lack one are two engine records with nothing claiming they are the same work. Without this rule
  two router envelopes in one session merge into each other, which fabricates a call rather than
  joining one;
- `source_count = 1`: a row that already holds two surfaces is not a loose end, and pulling a third
  into it is how one wrong guess propagates into a row that looks well attested;
- **starts within 2 seconds**, with each side's `wait_ms` added to the allowance — the router arrives
  before the engine does, and queueing is the legitimate part of a start-to-start difference. Both
  surfaces in this system stamp the **start** of a call, which is now checked rather than assumed:
  strata's record `time` sat 10 ms from a client's own request start on a call that then ran 11.4 s,
  and rooter sets `ts` in `newCallRecord`, whose own comment is "everything here is known before a
  slot is even requested". The invariant *`ts` is the start* is enforced in the probe, so a surface
  that stamps the other end normalises it at translation time;
- the two call intervals **also** overlap, as a secondary guard — and this guard earned its place the
  hard way. An earlier revision tested overlap *instead of* proximity, and against live strata a
  125.6 s envelope absorbed an unrelated 4.7 s engine record that began two minutes later. A long
  interval touches a lot of work that is not yours, so the longer a call runs the more it would
  swallow; the join reported itself as successful and was wrong. Proximity is what discriminates.

Then the tiers, which is what a live capture changed. Rooter's ring records carry the runner's
`chatcmpl` id, so most rows the fallback sees are rows that **already name their call** — and a rule
that refused every identified candidate joined nothing while continuing to look correct:

- **`usage_content_window`** — both sides report prompt and completion tokens and those numbers are
  equal. This is the only way to reach a row that already has an `upstream_id`. It is what the pairs
  that survived a same-process capture looked like: prompt 66933 against 66933, cached 66509 against
  66509, completion 549 against 549 — while the router's `total_ms` differed by 450 ms, because a
  router's number includes the router. Content agreed; latency did not.
- **`backend_key_window`** — timing plus the wire witness, allowed only when at least one side
  measured no usage and nobody has identified the candidate yet. This is the abandoned-stream case
  the path was built for, and it is named differently so a reader can tell a join that rests on a
  clock from one that rests on a measurement.
- **veto** — when both sides measured and the numbers disagree, neither tier fires. Two live false
  positives were exactly this: 90 ms apart with 49903 prompt tokens against 66933, and 100 ms apart
  with 183 completion tokens against 1395. Timing called both of them one call, silently.

And it **refuses to guess**: two candidates stays two visibly unjoined rows that `call_quality`
reports as unjoined, and `calls_joined_by_content` is reported apart from `calls_joined_by_timing_only`
so a panel cannot average the two as if they were the same kind of fact.

## Who is allowed to create a call

`cmd/mcharvest` polls the surfaces and writes. One rule in it is not cosmetic:

- **rooter may insert.** Its record says a client asked, and a call with no engine numbers is still a
  call.
- **strata may not.** Its record is keyed by an internal id, has no session key, and cannot say a
  client asked. It is written only when it joins a row that can (`RequireAttribution`). Everything
  else is counted as `unattributed` and reported, not written.

The reason is the whole point of the store: a completed call already has a rooter row, and if the
engine's record of the same work is allowed to insert its own, the store counts one generation twice
— once with a client and once without. That is not a gap in the data, it is invented data, and it is
invisible in a total. Strata's per-request records stay available at `/metrics` for a reader who wants
the engine's side of an unattributed call; they just do not become calls here.

The permanent fix removes the guess entirely. Rooter's record already carries `upstream_id` (the
chatcmpl id the runner generated) and `upstream_created`; strata's own record does not carry the
chatcmpl id it minted, so the two cannot be joined on a key that means the same thing to both sides.
Once strata exposes that id, this source sets `UpstreamID`, the join becomes path 2, and
`unattributed` becomes the small number it should be: calls nobody routed.

## What the schema refuses to do

- **`has_usage` is generated** (`prompt_tokens IS NOT NULL AND completion_tokens IS NOT NULL`), so
  no writer can disagree with it. Without it, "average tokens/sec this week" divides a denominator
  built from finished calls by a count that includes abandoned ones, and the answer is plausible
  enough to put on a dashboard.
- **Rates are derived, not supplied.** `cache_hit_rate` is computed in a trigger from the row's own
  components, because the two cache numbers people want are not the same number and are not close.
  Measured on the same calls: strata's `hit_rate` is 0.802–0.903 (expert-attention cache: of the
  tokens it reused, 80–90% came from the expert cache) while `cached_tokens / prompt_tokens` is
  0.992–0.9996 (prompt reuse). Averaging the wrong one into a week is a lie that looks like a
  saving. A rate that arrives with no named source is rejected outright; a rate that arrives with a
  source must match the row's own components to 1e-6 or the insert fails; components move and the
  derived rate follows silently.
- **A rate the components cannot produce is NULL, and the row says so.** `cache_rate_undefined` is a
  generated column, true when `cached_tokens` or `engine_cached_tokens` exceeds `prompt_tokens`. This
  schema used to raise an exception in that case, on the theory that only a bad join could produce it.
  That theory was wrong and it cost data: llama.cpp's `prompt_n` is the tokens the runner processed on
  this pass, not the whole prompt, so a cached span can legitimately be larger than the count the
  runner reported — and when it happened, the whole INSERT was rejected. A router had fully witnessed a
  call, the store dropped it, and the counter said "write error" instead of "one rate is unusable".
  Rooter reached the same conclusion independently: its API returns a NULL `cache_hit_rate` rather than
  a ratio above 1. Components stay so the ratio can be recomputed when a better prompt count turns up;
  a *claimed* rate on those components is still refused, because components that support no rate mean
  the number came from somewhere else.
- **`ts_is_observation`** for surfaces with no clock. sushi's log lines carry no timestamp, so `ts`
  is when the probe read the line — and a graph that does not say that is a graph with invented
  precision. A real timestamp replaces an observed one, in both directions.
- **`cache_hit_rate` is `cached_tokens / prompt_tokens`, and nothing else.** Strata's own `hit_rate`
  is *not* that ratio: measured across its 12 retained requests, `reused/prompt_tokens` sits at
  0.9924–0.9996 while `hit_rate` sits at 0.802–0.903, and none of the twelve agree within 0.02. Strata
  runs an MoE expert cache (`expert_slots`, `expert_cache_mib`) and its own metrics say which of those
  numbers it is — the gauge is documented as "the last request's expert cache hit rate" — so it gets a
  named column, `expert_cache_hit_rate`, and never a shared one. An unexplained number promoted to a
  first-class column becomes a legend that lies; an explained one gets a name that stops the next reader
  conflating it. A 107k-token prompt can be ~99% prompt-reused and ~86% expert-resident at the same
  time, both true, and one "cache hit rate" column would make one of those sentences false.
- **`prompt_tokens_computed` is kept apart from `prompt_tokens`.** llama.cpp reports `prompt_n`; rooter
  publishes its arithmetic answer beside it rather than replacing it, and this table does the same.
  Live from rooter's ring: `prompt_tokens 98060`, `prompt_tokens_computed 98060`, `extra.prompt_n 98060`
  — equal, which is the boring case. Collapsing the two would destroy the only evidence of how often
  they are not, and a computed estimate never becomes a reported count: a row with only a computed
  prompt keeps `has_usage = false` and stays out of token aggregates.
- **`usage_source` names the mechanism, not the component**: `none | client | injected |
  runner_scrape`. `injected` means a usage frame exists because the observer added
  `include_usage=true` — measurement affecting the measurement, worth knowing when you read the
  token totals. In practice rooter reports that the injection is a no-op on llama.cpp (the usage tail
  arrives either way) and its API only ever names `none` or `client`; live agrees — 56 rooter rows in
  the store, none of them `injected`, and rooter's ring records carry no provenance field at all, which
  is why this store writes `none` for them rather than guessing. There is deliberately no `wire`: every
  usage frame arrives over the wire, so the value would be unfalsifiable.
- **`extra` is namespaced by source by the store.** A writer's payload lands under its `source`
  name. If two surfaces both call their payload `raw`, a merge would silently turn *who said what*
  into *who wrote last*.
- **`call_quality` reports the store's own weaknesses as data**: `calls_without_usage`,
  `calls_joined_by_content`, `calls_joined_by_timing_only`, `calls_with_undefined_cache_rate`,
  `calls_with_observed_ts`. Any panel built on `calls` can be asked how much of its number depends on a
  heuristic or on a rate that could not be computed.

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

# Migrations in name order; the binary does this itself on boot, so doing it by hand is for the
# moment before a binary exists.
for f in internal/store/migrations/*.sql; do
  docker exec -i mc-pg psql -U mc -d modelcalls -v ON_ERROR_STOP=1 -f "$f"
done
docker exec -i mc-pg psql -U mc -d modelcalls -v ON_ERROR_STOP=1 -f test/contract_scenarios.sql

MODELCALLS_TEST_DSN='postgres://mc:mc@127.0.0.1:15432/modelcalls?sslmode=disable' \
  go test ./...
```

A real Postgres 16 is required. The contract *is* Postgres — partial unique indexes, `ON CONFLICT`
merge arithmetic, generated columns, jsonb concatenation — and there is no useful version of this
against a fake database. [Woodpecker CI](https://woodpecker.dev.initialed85.cc/initialed85/modelcalls)
runs both halves against a `postgres:16` service on every push, applies the migrations **twice**,
applies the scenarios **twice**, and then drops the whole schema and rebuilds it with the compiled
binary (`./entrypoint -migrate-only`) before it is allowed to publish anything. Re-runnability is not
a claim this repo makes in a comment; a Deployment applies these migrations on every start, so a
migration that only works once is a rollout that crashes on the second boot.

## Deployed

`home-dev`, namespace `modelcalls`, reconciled by Argo from `home-ops/clusters/dev/applications/modelcalls`:

| piece | what it is |
|---|---|
| `modelcalls-postgres` | Postgres 16 pinned by digest, 2 GiB Longhorn volume, ClusterIP only — nothing outside the namespace writes to the store except the harvester |
| `modelcalls-harvester` | one pod, `Recreate`, polls strata's `/metrics` on the GPU host and rooter's in-cluster Service every 10s, serves `/healthz` and `/metrics` on `:9138` |

The harvester carries its schema. `internal/store/migrate.go` embeds the migration files and applies
them on every boot, so the database a pod writes against is the database its code was written for —
a schema applied by something else can lag a binary quietly, where the writes keep succeeding and the
new facts simply never arrive. `-migrate=false` is there for the case where the database really is
owned elsewhere.

`/metrics` publishes the store's own account of itself, read out of `call_quality`: totals plus a
per-backend breakdown of `modelcalls_calls_with_usage`, `calls_without_usage`, `calls_aborted`,
`calls_joined_by_content`, `calls_joined_by_timing_only`, `calls_with_undefined_cache_rate`,
`calls_with_observed_ts`, alongside `modelcalls_harvest_*` for what this process did (cycles, the last
cycle's timestamp, the last error text, and a tally per write path). vmagent already scrapes any pod
carrying `prometheus.io/scrape`, so these land in VictoriaMetrics with no scrape config to maintain.
The split matters: a harvester polling a router that has gone quiet looks healthy until you ask how
many rows it actually wrote today.

**Still missing, and it is someone else's code:** rooter's `/rooter/events` and
`strata-exporter`'s `/events` are 404 in the images currently deployed (measured from inside a pod).
The harvester is pointed at rooter's ring anyway, so it logs the 404 every cycle and keeps capturing
strata rather than shipping half a picture. Capture becomes two-sided the moment those endpoints land,
with no manifest change needed.

Panels are not built. The store is the layer that can answer the questions; deciding which questions
deserve a graph is a separate job, and doing it before the data has been accumulating for a week would
be designing a dashboard off a sample of an afternoon.
