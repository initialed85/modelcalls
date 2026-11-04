package main

import (
	"context"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/initialed85/modelcalls/internal/store"
)

// The exposition is the interface a dashboard sees, so it is worth asserting on directly: which
// numbers exist, and that a failing cycle is visible rather than folded into "the harvester is fine".
func TestRenderMetricsReportsTheStoreOwnWeaknesses(t *testing.T) {
	rows := []storeRow{
		{Backend: "strata", Calls: 58, CallsWithUsage: 56, CallsWithoutUsage: 2, CallsAborted: 1,
			CallsJoinedAcrossSurfaces: 9, CallsWithUpstreamID: 54, CallsJoinedByContent: 9,
			CallsWithUndefinedCacheRate: 0, CallsWithExpertCacheRate: 12,
			CallsWithFirstContentMs: 30, CallsWithFirstContentGap: 29, CallsWithNegativeFirstContentGap: 1,
			// Strata's prompt_n is the work done this pass, so the flag is absent (unanswered) or false
			// and the computed column is the runner's own number. The equality still holds on most rows,
			// which is the trap: it is agreement there, not arithmetic.
			CallsWithPromptNFlag: 14, CallsWithPromptNIncludingCache: 0,
			CallsWithPromptDerivationFlag: 0, CallsWithDerivedPromptTokens: 0,
			CallsWherePromptEqualsCachedPlusComputed: 55,
			MeanTTFTMs:                               f64(140.5), MeanFirstContentMs: f64(30456.4), MeanFirstContentGap: f64(30315.9)},
		// Sushi has no first-content measurement at all, which is the state worth testing: a mean over
		// nothing must not be published as 0, because 0 is the number a panel reads as "no thinking".
		// Every sushi row carries the flag and satisfies the equality, because on this engine the
		// equality is the router's subtraction - 12 of 12 - so it is a tautology, not corroboration.
		{Backend: "sushi", Calls: 12, CallsWithUsage: 12, CallsWithObservedTS: 0,
			CallsJoinedAcrossSurfaces: 2, CallsWithUpstreamID: 12, CallsJoinedByTimingOnly: 2,
			CallsWithFirstContentMs: 0, CallsWithFirstContentGap: 0,
			CallsWithPromptNFlag: 12, CallsWithPromptNIncludingCache: 12,
			CallsWithPromptDerivationFlag: 0, CallsWithDerivedPromptTokens: 0,
			CallsWherePromptEqualsCachedPlusComputed: 12},
	}
	counts := map[string]int{"insert": 40, "re_pull": 20, "unattributed": 1}
	text := renderMetrics(rows, 7, time.Unix(1793761423, 0).UTC(), "", counts)

	wants := []string{
		// Totals, summed over the backends the view reports.
		"modelcalls_calls_total 70",
		"modelcalls_calls_joined_by_content 9",
		"modelcalls_calls_joined_by_timing_only 2",
		"modelcalls_calls_without_usage 2",
		"modelcalls_calls_with_undefined_cache_rate 0",
		"modelcalls_backends 2",
		// The same metric names with a backend label, which is where the per-surface disagreement is
		// visible at all.
		`modelcalls_calls_total{backend="strata"} 58`,
		`modelcalls_calls_joined_by_timing_only{backend="sushi"} 2`,
		`modelcalls_calls_with_expert_cache_rate{backend="strata"} 12`,
		// The thinking gap's population, so no mean over it can be read without the denominator.
		"modelcalls_calls_with_first_content_gap 29",
		`modelcalls_calls_with_first_content_gap{backend="sushi"} 0`,
		`modelcalls_call_mean_first_content_gap_ms{backend="strata"} 30315.900000`,
		`modelcalls_call_mean_ttft_ms{backend="strata"} 140.500000`,
		// The prompt provenance, published as populations. The equality is deliberately the last of
		// these: it is the number that looks like corroboration and is not, and it has to be readable
		// only alongside the flags that explain it.
		"modelcalls_calls_with_prompt_n_flag 26",
		`modelcalls_calls_with_prompt_n_including_cache{backend="sushi"} 12`,
		`modelcalls_calls_with_prompt_n_including_cache{backend="strata"} 0`,
		"modelcalls_calls_with_derived_prompt_tokens 0",
		`modelcalls_calls_where_prompt_equals_cached_plus_computed{backend="sushi"} 12`,
		"modelcalls_harvest_cycles_total 7",
		"modelcalls_harvest_last_cycle_timestamp_seconds 1793761423",
		"modelcalls_harvest_last_cycle_ok 1",
		`modelcalls_harvest_path_total{path="insert"} 40`,
		`modelcalls_harvest_path_total{path="unattributed"} 1`,
	}
	for _, w := range wants {
		if !strings.Contains(text, w) {
			t.Errorf("exposition is missing %q\n%s", w, text)
		}
	}

	// Every data line has to be parseable, because a single malformed line makes the whole scrape fail
	// and the panel goes "no data" rather than "wrong data". Floats are allowed because the timing means
	// are seconds-to-microsecond quantities and truncating them to integers would be a lie of its own.
	line := regexp.MustCompile(`^[a-z_]+(\{[a-z_]+="[^"]*"\})? [-0-9]+(\.[0-9]+)?$`)
	for i, l := range strings.Split(strings.TrimSpace(text), "\n") {
		if strings.HasPrefix(l, "#") {
			continue
		}
		if !line.MatchString(l) {
			t.Errorf("line %d is not a valid exposition line: %q", i+1, l)
		}
	}

	t.Run("a mean is only published where there is a population to mean", func(t *testing.T) {
		// Sushi reported nothing for first_content_ms, so it must not have a series: the scrape that
		// shows 0 for a box nobody measured is the scrape that makes an absence look like a result.
		if strings.Contains(text, `modelcalls_call_mean_first_content_gap_ms{backend="sushi"}`) {
			t.Error("a mean was published for a backend with no first-content rows, so the panel averages a number that is not there")
		}
		if strings.Contains(text, `modelcalls_call_mean_ttft_ms{backend="sushi"}`) {
			t.Error("a mean was published for a backend with no measurement")
		}
		// And no unlabelled version of any mean exists: the mean of two per-backend means is not the
		// mean over the calls, and averaging a box that served 60 calls with one that served 3 is a
		// number about neither.
		for _, m := range []string{"call_mean_ttft_ms", "call_mean_first_content_ms", "call_mean_first_content_gap_ms"} {
			if strings.Contains(text, "\nmodelcalls_"+m+" ") {
				t.Errorf("modelcalls_%s is published without a backend label; a total of per-backend means is not a mean over calls", m)
			}
		}
	})

	t.Run("a failing cycle is visible", func(t *testing.T) {
		bad := renderMetrics(rows, 8, time.Unix(1793761500, 0).UTC(), "rooter: 404 Not Found", counts)
		if !strings.Contains(bad, "modelcalls_harvest_last_cycle_ok 0") {
			t.Error("last_cycle_ok stayed 1 while a source was erroring")
		}
		if !strings.Contains(bad, `modelcalls_harvest_last_error_info{error="rooter: 404 Not Found"} 1`) {
			t.Error("the error text is not exposed, so a 404 and a timeout would look identical")
		}
	})

	t.Run("an empty store says so without pretending to be a zero-traffic week", func(t *testing.T) {
		fresh := renderMetrics(nil, 0, time.Time{}, "", map[string]int{})
		if !strings.Contains(fresh, "modelcalls_calls_total 0") {
			t.Error("no rows should still report a total of 0, not omit the series")
		}
		if !strings.Contains(fresh, "modelcalls_backends 0") {
			t.Error("backends gauge missing")
		}
		if strings.Contains(fresh, "backend=") {
			t.Error("per-backend series appeared with no backends to report")
		}
		if !strings.Contains(fresh, "modelcalls_harvest_last_cycle_timestamp_seconds 0") {
			t.Error("zero time should render as 0, not as a real-looking epoch")
		}
		if strings.Contains(fresh, "modelcalls_harvest_path_total{") {
			t.Error("an empty tally produced label lines anyway")
		}
	})

	t.Run("the same state always renders the same body", func(t *testing.T) {
		one := renderMetrics(rows, 1, time.Unix(1793761423, 0).UTC(), "", counts)
		two := renderMetrics(rows, 1, time.Unix(1793761423, 0).UTC(), "", counts)
		if one != two {
			t.Error("the same state rendered two different bodies, so a diff of two scrapes is meaningless")
		}
	})
}

func TestSnapshotKeepsItsOwnCopyOfTheTally(t *testing.T) {
	// The cycle loop mutates one map for the process's whole life. If the snapshot aliased it, a
	// scrape could read a tally mid-update and the exposition would mix two cycles.
	counts := map[string]int{"insert": 1}
	snap := &snapshot{}
	snap.cycle(nil, counts)
	counts["insert"] = 99

	_, _, _, got := snap.copy()
	if got["insert"] != 1 {
		t.Errorf("insert = %d, want the value at cycle time (1): the snapshot shares the caller's map", got["insert"])
	}
	if _, _, lastErr, _ := snap.copy(); lastErr != "" {
		t.Errorf("lastErr = %q, want empty after a clean cycle", lastErr)
	}
	snap.cycle(assertionError("boom"), counts)
	if _, _, lastErr, _ := snap.copy(); lastErr != "boom" {
		t.Errorf("lastErr = %q, want boom", lastErr)
	}
	snap.cycle(nil, counts)
	if _, _, lastErr, _ := snap.copy(); lastErr != "" {
		t.Errorf("lastErr = %q after a clean cycle, want the old error cleared", lastErr)
	}
}

type assertionError string

func (e assertionError) Error() string { return string(e) }

// TestReadCallQualityMatchesTheView is a schema-drift check, and the drift it catches is quiet: this
// package lists call_quality's columns by name in a SELECT. Rename a column in a migration and nothing
// else breaks until a scrape returns 500 in production. Run against the same DSN the store tests use.
func TestReadCallQualityMatchesTheView(t *testing.T) {
	dsn := os.Getenv("MODELCALLS_TEST_DSN")
	if dsn == "" {
		t.Skip("MODELCALLS_TEST_DSN not set")
	}
	ctx := context.Background()
	st, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	rows, err := readCallQuality(ctx, st.DB())
	if err != nil {
		t.Fatalf("readCallQuality: %v", err)
	}
	t.Logf("call_quality: %d backend row(s), %d calls total", len(rows), total(rows).Calls)
	for _, r := range rows {
		if r.Calls < 0 || r.Calls < r.CallsJoinedByContent {
			t.Errorf("implausible row: %+v", r)
		}
	}
}

func f64(v float64) *float64 { return &v }
