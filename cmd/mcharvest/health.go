package main

// The harvester's read-only surface: /healthz for the kubelet and /metrics for a scraper.
//
// Two different things are reported here, deliberately, because they fail for different reasons and a
// single "is it working" number would hide which one broke:
//
//   modelcalls_harvest_*  what this process did — cycles run, records taken, joins fired, records it
//                         refused to attribute, and whether the last cycle hit a source at all.
//   modelcalls_calls_*    what the store contains, read straight out of call_quality.
//
// The second group is the interesting one on a cluster: a harvester can be healthy, polling a router
// that has gone quiet, and quietly recording nothing new. A panel that only watches the process says
// "green". A panel that watches rows-per-source says "nobody has called since 03:11", which is the
// sentence you actually needed.
//
// Nothing here writes. The store is the only writer, the store is the contract, and a monitoring
// surface that could mutate rows would be a second way to break the contract.

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/initialed85/modelcalls/internal/store"
)

// snapshot is what a cycle left behind. Guarded by a mutex because the cycle loop writes it and a
// scrape reads it at an unrelated moment.
type snapshot struct {
	mu        sync.Mutex
	cycles    int
	lastCycle time.Time
	lastError string
	counts    map[string]int
}

func (s *snapshot) cycle(err error, counts map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cycles++
	s.lastCycle = time.Now().UTC()
	s.lastError = ""
	if err != nil {
		s.lastError = err.Error()
	}
	s.counts = map[string]int{}
	for k, v := range counts {
		s.counts[k] = v
	}
}

func (s *snapshot) copy() (cycles int, lastCycle time.Time, lastError string, counts map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	counts = map[string]int{}
	for k, v := range s.counts {
		counts[k] = v
	}
	return s.cycles, s.lastCycle, s.lastError, counts
}

// storeRow is one row of call_quality. The view is grouped by backend, so "the store's totals" are a
// sum over these rows and a per-backend breakdown comes for free - which is the shape a panel needs,
// because the disagreement between surfaces is per backend.
type storeRow struct {
	Backend                     string
	Calls                       int64
	CallsWithUsage              int64
	CallsWithoutUsage           int64
	CallsAborted                int64
	CallsWithUsageInjected      int64
	CallsWithObservedTS         int64
	CallsJoinedAcrossSurfaces   int64
	CallsWithUpstreamID         int64
	CallsJoinedByContent        int64
	CallsJoinedByTimingOnly     int64
	CallsWithUndefinedCacheRate int64
	CallsWithExpertCacheRate    int64
}

func (r *storeRow) fields() []any {
	return []any{&r.Backend, &r.Calls, &r.CallsWithUsage, &r.CallsWithoutUsage, &r.CallsAborted,
		&r.CallsWithUsageInjected, &r.CallsWithObservedTS, &r.CallsJoinedAcrossSurfaces,
		&r.CallsWithUpstreamID, &r.CallsJoinedByContent, &r.CallsJoinedByTimingOnly,
		&r.CallsWithUndefinedCacheRate, &r.CallsWithExpertCacheRate}
}

// readCallQuality names the view's columns explicitly rather than SELECT *. A wildcard would let a
// renamed column shift every position silently and put the wrong number under the right name - the
// failure mode this whole store exists to avoid.
func readCallQuality(ctx context.Context, db *sql.DB) ([]storeRow, error) {
	rows, err := db.QueryContext(ctx, `SELECT backend, calls, calls_with_usage, calls_without_usage,
		calls_aborted, calls_with_usage_injected, calls_with_observed_ts, calls_joined_across_surfaces,
		calls_with_upstream_id, calls_joined_by_content, calls_joined_by_timing_only,
		calls_with_undefined_cache_rate, calls_with_expert_cache_rate
		FROM call_quality ORDER BY backend`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []storeRow
	for rows.Next() {
		var r storeRow
		if err := rows.Scan(r.fields()...); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func total(rows []storeRow) storeRow {
	var t storeRow
	t.Backend = ""
	for _, r := range rows {
		t.Calls += r.Calls
		t.CallsWithUsage += r.CallsWithUsage
		t.CallsWithoutUsage += r.CallsWithoutUsage
		t.CallsAborted += r.CallsAborted
		t.CallsWithUsageInjected += r.CallsWithUsageInjected
		t.CallsWithObservedTS += r.CallsWithObservedTS
		t.CallsJoinedAcrossSurfaces += r.CallsJoinedAcrossSurfaces
		t.CallsWithUpstreamID += r.CallsWithUpstreamID
		t.CallsJoinedByContent += r.CallsJoinedByContent
		t.CallsJoinedByTimingOnly += r.CallsJoinedByTimingOnly
		t.CallsWithUndefinedCacheRate += r.CallsWithUndefinedCacheRate
		t.CallsWithExpertCacheRate += r.CallsWithExpertCacheRate
	}
	return t
}

// storeMetrics describes each gauge once: the name, what it means, and which field of a row it reads.
// A table rather than a wall of Fprintf calls, so the total and the per-backend series cannot drift
// apart from each other - they are the same descriptor rendered twice.
type storeMetric struct {
	name string
	help string
	get  func(storeRow) int64
}

var storeMetrics = []storeMetric{
	{"calls_total", "Rows in the store.", func(r storeRow) int64 { return r.Calls }},
	{"calls_with_usage", "Rows with prompt and completion token counts, i.e. usable in a token aggregate.", func(r storeRow) int64 { return r.CallsWithUsage }},
	{"calls_without_usage", "Rows no surface measured tokens for. Any token total is built on the complement of this.", func(r storeRow) int64 { return r.CallsWithoutUsage }},
	{"calls_aborted", "Streams that ended before finishing.", func(r storeRow) int64 { return r.CallsAborted }},
	{"calls_with_usage_injected", "Rows whose usage frame exists because an observer asked for it.", func(r storeRow) int64 { return r.CallsWithUsageInjected }},
	{"calls_with_observed_ts", "Rows whose timestamp is when a probe read a record, not when the call happened.", func(r storeRow) int64 { return r.CallsWithObservedTS }},
	{"calls_joined_across_surfaces", "Rows witnessed by more than one surface.", func(r storeRow) int64 { return r.CallsJoinedAcrossSurfaces }},
	{"calls_joined_by_content", "Rows joined because two surfaces measured the same tokens. Strongest join there is.", func(r storeRow) int64 { return r.CallsJoinedByContent }},
	{"calls_joined_by_timing_only", "Rows joined on timing plus one wire witness, because neither side could measure the work.", func(r storeRow) int64 { return r.CallsJoinedByTimingOnly }},
	{"calls_with_upstream_id", "Rows carrying a runner-assigned id.", func(r storeRow) int64 { return r.CallsWithUpstreamID }},
	{"calls_with_undefined_cache_rate", "Rows whose cache rate is NULL because the components cannot produce one.", func(r storeRow) int64 { return r.CallsWithUndefinedCacheRate }},
	{"calls_with_expert_cache_rate", "Rows carrying the engine's expert-cache rate.", func(r storeRow) int64 { return r.CallsWithExpertCacheRate }},
}

// renderMetrics is the whole exposition, built as text so a test can assert on it without a scraper.
// Gauges, not counters: this process is short-lived next to a week of calls, and a reset on restart is
// normal - the durable history is the store's, and the modelcalls_calls_* group is what reports it.
func renderMetrics(rows []storeRow, cycles int, lastCycle time.Time, lastError string, counts map[string]int) string {
	var b strings.Builder
	sums := total(rows)

	// The totals first, then the per-backend breakdown under the same metric name. A panel that wants
	// one number and a panel that wants the split both read this body without asking for two scrapes.
	for _, m := range storeMetrics {
		fmt.Fprintf(&b, "# HELP modelcalls_%s %s Summed over backends; the per-backend series carry a backend label.\n# TYPE modelcalls_%s gauge\nmodelcalls_%s %d\n",
			m.name, m.help, m.name, m.name, m.get(sums))
		for _, r := range rows {
			fmt.Fprintf(&b, "modelcalls_%s{backend=%q} %d\n", m.name, r.Backend, m.get(r))
		}
	}

	gauge := func(name, help string, v any) {
		fmt.Fprintf(&b, "# HELP modelcalls_%s %s\n# TYPE modelcalls_%s gauge\nmodelcalls_%s %v\n",
			name, help, name, name, v)
	}
	gauge("up", "1 when the store answered the query behind these numbers.", 1)
	gauge("backends", "How many backends the store has rows for.", len(rows))
	gauge("harvest_cycles_total", "Cycles this process has run since start.", cycles)
	gauge("harvest_last_cycle_timestamp_seconds", "When the last cycle finished. Stale with a live harvester means a source stopped answering.", unixOrZero(lastCycle))
	gauge("harvest_last_cycle_ok", "1 when the last cycle completed without an error, 0 otherwise.", boolInt(lastError == ""))
	if lastError != "" {
		fmt.Fprintf(&b, "# TYPE modelcalls_harvest_last_error_info gauge\nmodelcalls_harvest_last_error_info{error=%q} 1\n", lastError)
	}

	// Per-path tallies, sorted so the exposition is stable and a diff of two scrapes means something.
	paths := make([]string, 0, len(counts))
	for k := range counts {
		paths = append(paths, k)
	}
	sort.Strings(paths)
	b.WriteString("# HELP modelcalls_harvest_path_total Records this process handled, by what the store did with them.\n")
	b.WriteString("# TYPE modelcalls_harvest_path_total gauge\n")
	for _, p := range paths {
		fmt.Fprintf(&b, "modelcalls_harvest_path_total{path=%q} %d\n", p, counts[p])
	}
	return b.String()
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func unixOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

// startHealth serves the read-only surface and returns immediately. A nil return means "no listener
// requested", which is how a one-shot -once run (CI, a manual check) stays a one-shot run.
func startHealth(listen string, st *store.Store, snap *snapshot) (*http.Server, error) {
	if listen == "" {
		return nil, nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := st.DB().PingContext(ctx); err != nil {
			http.Error(w, "store unreachable: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		row, err := readCallQuality(ctx, st.DB())
		if err != nil {
			// A scrape that cannot read the store must not look like a store with zero calls in it:
			// the absence of these series is the signal.
			log.Printf("metrics: call_quality read failed: %v", err)
			http.Error(w, "call_quality read failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		cycles, lastCycle, lastError, counts := snap.copy()
		w.Header().Set("Content-Type", "text/plain; version=0.4; charset=utf-8")
		_, _ = w.Write([]byte(renderMetrics(row, cycles, lastCycle, lastError, counts)))
	})
	srv := &http.Server{Addr: listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("health listener on %s: %v", listen, err)
		}
	}()
	return srv, nil
}
