// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metricsv2 // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal/metricsv2"

import (
	"math"
	"sync"
	"time"

	"go.uber.org/zap"
)

// SeriesCache tracks which (day, SeriesHash) pairs have already had a series
// row written, so the exporter only writes one series row per series per day.
//
// The cache is strictly a write-reduction optimization, never a correctness
// mechanism: a miss (capacity, eviction, restart, or a replica that has not
// seen the series) only causes a duplicate series row, which the
// AggregatingMergeTree series table collapses at merge time. Multiple
// collector replicas can therefore each keep an independent cache.
//
// One generation is kept per day (data-time days, not wall-clock), up to
// maxDays generations, evicted least-recently-USED first. Eviction is by
// recency, not by day value: a stream actively writing an older day (a
// backfill, a delayed agent, a clock-skewed client) keeps its generation
// alive while idle past/future days age out, so mixed timelines cannot
// permanently uncache each other. Any positive day is cacheable regardless
// of what other days are in flight; non-positive days (zero or negative
// timestamps from broken clients) are never cached.
//
// Worst-case memory: maxDays x maxEntriesPerDay entries at roughly 50 bytes
// of Go map overhead per entry — about 400 MiB at the defaults (8 days x
// 1<<20 entries per day).
type SeriesCache struct {
	mu               sync.Mutex
	maxEntriesPerDay int
	maxDays          int
	days             map[int32]*dayGen
	clock            uint64 // logical time for LRU recency; bumped on each day use

	logger  *zap.Logger
	stats   CacheStats
	ops     uint // cache operations since the last stats-log clock check
	lastLog time.Time
}

// dayGen is one day's generation: the set of hashes already written for that
// day and the logical time the day was last used (a Has on it or an Add to it).
type dayGen struct {
	set      map[uint64]struct{}
	lastUsed uint64
}

// CacheStats are cumulative counters for measuring cache effectiveness.
type CacheStats struct {
	Hits            uint64 // Has calls that found the hash cached
	Misses          uint64 // Has calls that did not (day untracked or hash unseen)
	UncacheableAdds uint64 // entries passed to Add with day <= 0 (never cached)
	EvictedDays     uint64 // day generations evicted to stay within maxDays
}

const (
	defaultMaxEntriesPerDay = 1 << 20
	defaultMaxDays          = 8

	// Stats are logged at debug level at most once per statsLogInterval; the
	// wall clock is consulted only every statsOpsPerCheck cache operations so
	// the hot path stays cheap.
	statsLogInterval = time.Minute
	statsOpsPerCheck = 256
)

// NewSeriesCache returns a cache holding at most maxEntriesPerDay series per
// day for up to maxDays least-recently-used days. Non-positive sizes select
// the defaults (1<<20 entries, 8 days). logger (nil for none) receives a
// periodic debug-level effectiveness snapshot.
func NewSeriesCache(maxEntriesPerDay, maxDays int, logger *zap.Logger) *SeriesCache {
	if maxEntriesPerDay <= 0 {
		maxEntriesPerDay = defaultMaxEntriesPerDay
	}
	if maxDays <= 0 {
		maxDays = defaultMaxDays
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SeriesCache{
		maxEntriesPerDay: maxEntriesPerDay,
		maxDays:          maxDays,
		days:             make(map[int32]*dayGen),
		logger:           logger,
		lastLog:          time.Now(),
	}
}

// Has reports whether a series row for (day, hash) is known to have been
// written already, and marks the day as recently used. Non-positive days are
// never cached and are not counted in the hit/miss statistics.
func (c *SeriesCache) Has(day int32, hash uint64) bool {
	if day <= 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	hit := false
	if gen, ok := c.days[day]; ok {
		c.clock++
		gen.lastUsed = c.clock
		_, hit = gen.set[hash]
	}
	if hit {
		c.stats.Hits++
	} else {
		c.stats.Misses++
	}
	c.maybeLogStats()
	return hit
}

// Add records series rows as written and returns the number of entries that
// could NOT be cached because the day is at its per-day capacity (callers
// should surface that as a saturation signal). Call only after the series
// insert succeeded. Non-positive days are ignored (write-through, not
// saturation) and counted as uncacheable.
func (c *SeriesCache) Add(day int32, hashes []uint64) int {
	if len(hashes) == 0 {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if day <= 0 {
		c.stats.UncacheableAdds += uint64(len(hashes))
		c.maybeLogStats()
		return 0
	}

	gen := c.genFor(day)
	dropped := 0
	for i, h := range hashes {
		if len(gen.set) >= c.maxEntriesPerDay {
			dropped = len(hashes) - i
			break
		}
		gen.set[h] = struct{}{}
	}
	c.maybeLogStats()
	return dropped
}

// Stats returns a snapshot of the cumulative counters.
func (c *SeriesCache) Stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

// genFor returns the generation for day, creating it — and evicting the
// least-recently-used day when maxDays are already tracked — if absent.
// Called with c.mu held.
func (c *SeriesCache) genFor(day int32) *dayGen {
	c.clock++
	if gen, ok := c.days[day]; ok {
		gen.lastUsed = c.clock
		return gen
	}
	for len(c.days) >= c.maxDays {
		c.evictLRU()
	}
	gen := &dayGen{set: make(map[uint64]struct{}), lastUsed: c.clock}
	c.days[day] = gen
	return gen
}

// evictLRU drops the least-recently-used day generation. maxDays is small,
// so a linear scan beats maintaining a linked list. Called with c.mu held.
func (c *SeriesCache) evictLRU() {
	var (
		lruDay  int32
		lruUsed uint64 = math.MaxUint64
	)
	for d, g := range c.days {
		if g.lastUsed < lruUsed {
			lruUsed, lruDay = g.lastUsed, d
		}
	}
	delete(c.days, lruDay)
	c.stats.EvictedDays++
}

// maybeLogStats emits a debug-level effectiveness snapshot at most once per
// statsLogInterval. Called with c.mu held.
func (c *SeriesCache) maybeLogStats() {
	c.ops++
	if c.ops < statsOpsPerCheck {
		return
	}
	c.ops = 0
	now := time.Now()
	if now.Sub(c.lastLog) < statsLogInterval {
		return
	}
	c.lastLog = now

	entries := 0
	for _, g := range c.days {
		entries += len(g.set)
	}
	lookups := c.stats.Hits + c.stats.Misses
	hitRatio := 0.0
	if lookups > 0 {
		hitRatio = float64(c.stats.Hits) / float64(lookups)
	}
	c.logger.Debug("metrics v2 series cache stats",
		zap.Uint64("hits", c.stats.Hits),
		zap.Uint64("misses", c.stats.Misses),
		zap.Float64("hit_ratio", hitRatio),
		zap.Uint64("uncacheable_adds", c.stats.UncacheableAdds),
		zap.Uint64("evicted_days", c.stats.EvictedDays),
		zap.Int("tracked_days", len(c.days)),
		zap.Int("entries", entries))
}
