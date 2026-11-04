// Command mcharvest feeds modelcalls.
//
// It is deliberately small: poll each surface, hand the translated events to the store, count what
// happened. All the hard decisions live in the store, because they are the contract, and a harvester
// that makes its own decisions about what counts as one call is a second contract in a different
// language.
//
// Two rules this program encodes, both of them load-bearing:
//
//  1. Rooter is polled before strata, every cycle. A strata record is only writable when it can be
//     attributed to a row that knows a client asked, so reading the router first is what makes the
//     attribution possible in the same cycle instead of a cycle later - and a strata record that
//     finds nothing is reported, not inserted.
//
//  2. Poll order does not decide join order. Both surfaces stamp opposite ends of a call, so the
//     store joins on intervals; this program's job is only to keep handing both sides over.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/initialed85/modelcalls/internal/sources"
	"github.com/initialed85/modelcalls/internal/store"
)

func main() {
	var (
		dsn      = flag.String("dsn", envOr("MODELCALLS_DSN", ""), "Postgres DSN for modelcalls")
		interval = flag.Duration("interval", 5*time.Second, "how often to poll every surface")
		once     = flag.Bool("once", false, "one pass, then exit (what the CI smoke run and a manual check use)")

		strataURL     = flag.String("strata-url", envOr("STRATA_METRICS_URL", ""), "strata /metrics URL, e.g. http://192.168.11.128:8088/metrics")
		strataToken   = flag.String("strata-token", envOr("STRATA_TOKEN", ""), "bearer token for strata's /metrics")
		strataBackend = flag.String("strata-backend", "strata", "backend label to write for strata records (strata's own document does not know which alias it is mounted under)")
		strataAlias   = flag.String("strata-alias", "strata", "alias label to write for strata records")
		strataProfile = flag.String("strata-profile", "", "profile label to write for strata records (empty writes none)")

		rooterURL   = flag.String("rooter-url", envOr("ROOTER_URL", ""), "rooter base URL, e.g. https://rooter.dev.initialed85.cc")
		rooterToken = flag.String("rooter-token", envOr("ROOTER_API_KEY", ""), "bearer token for rooter's /rooter/events")
		rooterLimit = flag.Int("rooter-limit", 200, "records per rooter poll")

		maxCycles = flag.Int("cycles", 0, "stop after this many cycles (0 runs until interrupted)")
	)
	flag.Parse()

	if *dsn == "" {
		log.Fatal("-dsn (or MODELCALLS_DSN) is required: the store is not optional")
	}
	if *strataURL == "" && *rooterURL == "" {
		log.Fatal("nothing to poll: set -strata-url and/or -rooter-url")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st, err := store.Open(ctx, *dsn)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	var strata *sources.Strata
	if *strataURL != "" {
		strata = &sources.Strata{URL: *strataURL, Token: *strataToken,
			Backend: *strataBackend, Alias: *strataAlias, Profile: *strataProfile}
	}
	var rooter *sources.Rooter
	if *rooterURL != "" {
		rooter = &sources.Rooter{URL: *rooterURL, Token: *rooterToken, Limit: *rooterLimit}
	}

	counts := map[string]int{}
	for cycle := 1; ; cycle++ {
		if err := pass(ctx, st, rooter, strata, counts); err != nil {
			// One source being down does not stop the other. A router that is restarting is not a
			// reason to stop recording what the engine said about the calls it did serve.
			log.Printf("cycle %d: %v", cycle, err)
		}
		if *once || (*maxCycles > 0 && cycle >= *maxCycles) {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(*interval):
		}
	}

	fmt.Printf("modelcalls harvest done: %s\n", summary(counts))
}

// pass runs one cycle and reports counters, so a run says out loud how many of its rows were joins
// and how many engine records could not be attributed to a client.
func pass(ctx context.Context, st *store.Store, rooter *sources.Rooter, strata *sources.Strata, counts map[string]int) error {
	var errs []error

	if rooter != nil {
		events, err := rooter.Pull(ctx)
		if err != nil {
			errs = append(errs, err)
		}
		for _, ev := range events {
			res, err := st.Write(ctx, ev)
			if err != nil {
				errs = append(errs, fmt.Errorf("write %s: %w", ev.EventKey, err))
				continue
			}
			counts[res.Path]++
		}
	}

	if strata != nil {
		events, err := strata.Pull(ctx)
		if err != nil {
			errs = append(errs, err)
		}
		for _, ev := range events {
			res, err := st.Write(ctx, ev)
			if err != nil {
				errs = append(errs, fmt.Errorf("write %s: %w", ev.EventKey, err))
				continue
			}
			counts[res.Path]++
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%d problem(s): %v", len(errs), errs[0])
	}
	if n := counts[store.PathUnattributed]; n > 0 {
		log.Printf("cycle: %s - engine records with no client behind them were reported and NOT written; "+
			"they are real work by the engine that this store cannot attribute to a call", summary(counts))
	}
	return nil
}

func summary(counts map[string]int) string {
	order := []string{store.PathRePull, store.PathJoinUpstream, store.PathJoinFallback,
		store.PathInsert, store.PathUnattributed}
	out := ""
	for _, k := range order {
		if counts[k] > 0 {
			out += fmt.Sprintf("%s=%d ", k, counts[k])
		}
	}
	if out == "" {
		return "nothing new"
	}
	return out
}

func envOr(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
