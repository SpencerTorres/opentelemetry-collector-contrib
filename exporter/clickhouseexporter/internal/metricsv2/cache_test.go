// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metricsv2

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestCache(maxEntriesPerDay, maxDays int) *SeriesCache {
	return NewSeriesCache(maxEntriesPerDay, maxDays, nil)
}

func TestSeriesCacheHasAdd(t *testing.T) {
	c := newTestCache(10, 0)

	assert.False(t, c.Has(100, 1))
	c.Add(100, []uint64{1, 2})
	assert.True(t, c.Has(100, 1))
	assert.True(t, c.Has(100, 2))
	assert.False(t, c.Has(100, 3))
	assert.False(t, c.Has(101, 1), "a different day is a different generation")
}

// TestSeriesCacheMixedTimelines is the regression test for the production
// failure mode of the old fixed-two-generation design: a realtime stream
// stays on one day while a backfill-style stream advances through far-future
// days. The realtime day must stay cached — hit rate back to 100% after first
// sight — no matter what other days are in flight.
func TestSeriesCacheMixedTimelines(t *testing.T) {
	c := newTestCache(0, 8)

	const dayX = int32(19_000)
	streamA := make([]uint64, 100)
	for i := range streamA {
		streamA[i] = uint64(i + 1)
	}

	// First sight of the realtime stream: all misses, then cached.
	for _, h := range streamA {
		assert.False(t, c.Has(dayX, h))
	}
	require.Zero(t, c.Add(dayX, streamA))

	// Stream B advances through days X+30..X+40, interleaved with realtime
	// pushes on day X. Under the old design, dayX fell out of the two-newest
	// generations as soon as stream B passed two future days and became
	// permanently uncacheable.
	for d := dayX + 30; d <= dayX+40; d++ {
		for _, h := range streamA {
			assert.True(t, c.Has(dayX, h),
				"realtime day %d must stay cached while backfill day %d is in flight", dayX, d)
		}
		require.Zero(t, c.Add(d, []uint64{uint64(d)}))
	}

	stats := c.Stats()
	assert.Equal(t, uint64(11*len(streamA)), stats.Hits, "every realtime lookup after first sight must hit")
	assert.Equal(t, uint64(len(streamA)), stats.Misses, "only the first-sight lookups may miss")
	assert.Equal(t, uint64(4), stats.EvictedDays, "12 distinct days through an 8-day cache evict the 4 idle ones")
}

// TestSeriesCacheOldDaysCacheable: any positive day is cacheable regardless
// of newer days already tracked (the old design wrote days older than the
// two newest through uncached, forever).
func TestSeriesCacheOldDaysCacheable(t *testing.T) {
	c := newTestCache(10, 0)

	c.Add(100, []uint64{1})
	c.Add(101, []uint64{2})
	c.Add(50, []uint64{9})
	assert.True(t, c.Has(50, 9), "days older than the newest tracked days must still cache")
	assert.True(t, c.Has(100, 1))
	assert.True(t, c.Has(101, 2))
}

func TestSeriesCacheLRUEvictionOrder(t *testing.T) {
	c := newTestCache(0, 3)

	c.Add(1, []uint64{1})
	c.Add(2, []uint64{2})
	c.Add(3, []uint64{3})

	// Reading day 1 makes day 2 the least-recently-used.
	assert.True(t, c.Has(1, 1))

	c.Add(4, []uint64{4})
	assert.True(t, c.Has(1, 1), "recently used day survives even though it is the smallest day value")
	assert.False(t, c.Has(2, 2), "least-recently-USED day is evicted, not the smallest")
	assert.True(t, c.Has(3, 3))
	assert.True(t, c.Has(4, 4))
	assert.Equal(t, uint64(1), c.Stats().EvictedDays)

	// The reads above touched days 3 and 4 after day 1, so day 1 is now the
	// LRU: an idle day ages out once it stops being used.
	c.Add(5, []uint64{5})
	assert.False(t, c.Has(1, 1))
	assert.Equal(t, uint64(2), c.Stats().EvictedDays)
}

func TestSeriesCacheCapacitySaturationReporting(t *testing.T) {
	c := newTestCache(2, 0)

	assert.Equal(t, 2, c.Add(100, []uint64{1, 2, 3, 4}), "entries past the per-day cap are reported as dropped")
	assert.True(t, c.Has(100, 1))
	assert.True(t, c.Has(100, 2))
	assert.False(t, c.Has(100, 3), "entries past the per-day cap are written through uncached")
	assert.False(t, c.Has(100, 4))

	assert.Equal(t, 1, c.Add(100, []uint64{5}), "a full day keeps reporting saturation")

	// The cap applies per day, not globally.
	assert.Equal(t, 0, c.Add(101, []uint64{6}))
	assert.True(t, c.Has(101, 6))
}

func TestSeriesCacheNonPositiveDays(t *testing.T) {
	c := newTestCache(10, 0)

	assert.Zero(t, c.Add(0, []uint64{1}), "day 0 is write-through, not saturation")
	assert.Zero(t, c.Add(-3, []uint64{2, 3}))
	assert.False(t, c.Has(0, 1))
	assert.False(t, c.Has(-3, 2))

	stats := c.Stats()
	assert.Equal(t, uint64(3), stats.UncacheableAdds)
	assert.Zero(t, stats.EvictedDays, "uncacheable days must not occupy or evict generations")

	c.Add(7, []uint64{7})
	assert.True(t, c.Has(7, 7))
}

func TestSeriesCacheConcurrent(t *testing.T) {
	c := newTestCache(1000, 4)

	const goroutines = 8
	const opsEach = 2000
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func(g int) {
			defer wg.Done()
			for i := range opsEach {
				day := int32(1 + (g+i)%6) // 6 days through a 4-day cache forces eviction churn
				hash := uint64(g*opsEach + i)
				c.Add(day, []uint64{hash})
				c.Has(day, hash) // may miss if another goroutine evicted the day; must not race
				c.Add(0, []uint64{hash})
			}
		}(g)
	}
	wg.Wait()

	stats := c.Stats()
	assert.Equal(t, uint64(goroutines*opsEach), stats.Hits+stats.Misses)
	assert.Equal(t, uint64(goroutines*opsEach), stats.UncacheableAdds)
}
