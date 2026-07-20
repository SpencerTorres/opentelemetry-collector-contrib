// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package clickhouseexporter

// This is a temporary validation suite for the experimental v2 metrics schema.
// It writes deterministic data through the real exporter path (two exporter
// instances with independent series caches, simulating two collector
// replicas), forces part merges with OPTIMIZE TABLE ... FINAL, and then runs
// the query shapes a user (or a PromQL translation layer) would actually
// write, asserting hand-computed expected values.
//
// Run against an existing server:
//
//	CLICKHOUSE_TEST_ENDPOINT="tcp://127.0.0.1:19000?database=otel_int_test" \
//	  go test -tags integration -run TestMetricsV2Math -v ./exporter/clickhouseexporter/
//
// or without the env var to spin up a container via testcontainers.
//
// Data layout (T0 = 2024-01-01T00:00:00Z, one point per series every 15s,
// 40 scrapes = 10 minutes; scrapes 0-19 pushed by exporter 1, 20-39 by
// exporter 2):
//
//	gauge   test.cpu.utilization  svc-a{core=0} v=10+i   svc-a{core=1} v=100+i   svc-b{core=0} v=1000+i
//	sum     test.requests (cumulative, monotonic)
//	        svc-a{code=200} v=5i (clean)
//	        svc-a{code=500} v=5i for i<20, v=5(i-20) for i>=20 (counter reset at i=20)
//	        svc-b{code=200} v=7i
//	hist    test.duration (cumulative) svc-a{route=/api}
//	        bounds [0.1 0.5 1 5]; point i: buckets [2 3 4 1 0]*(i+1), count 10(i+1), sum 6(i+1)
//	hist    test.batch.duration (delta) svc-b{job=batch}
//	        bounds [1 10]; point i: buckets [i 2i 3], count 3i+3, sum 0.5i
//	exph    test.rpc.duration (delta) svc-b{method=Get}
//	        every point: scale=2, positive offset 0, counts [10 20 30], count 60, sum 100
//	summary test.legacy.summary svc-a{}: q[0.5 0.9 0.99] v[1 2 3], count 10(i+1), sum 5(i+1)
import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var mathT0 = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

const mathScrapeInterval = 15 * time.Second

func TestMetricsV2Math(t *testing.T) {
	endpoint := os.Getenv("CLICKHOUSE_TEST_ENDPOINT")
	if endpoint == "" {
		c, chEnv, err := createClickhouseContainer("clickhouse/clickhouse-server:26.2-alpine")
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Terminate(context.Background()) })
		endpoint = chEnv.NativeEndpoint
	}

	requireFullTextSearch(t, endpoint)

	// Two exporter instances = two independent series caches = duplicate
	// series rows, like two collector replicas. Each pushes half the scrapes.
	exp1 := newTestMetricsV2Exporter(t, endpoint)
	require.NoError(t, exp1.pushMetricsData(t.Context(), mathTestPayload(0, 20)))

	exp2 := newTestMetricsV2Exporter(t, endpoint)
	require.NoError(t, exp2.pushMetricsData(t.Context(), mathTestPayload(20, 40)))

	db := exp1.db
	v2 := &exp1.cfg.MetricsV2

	// Force merges so AggregatingMergeTree/ReplacingMergeTree collapse and
	// the rollup argMax states combine, as they would in steady state.
	for _, table := range []string{
		v2.SeriesTableName, v2.FamiliesTableName, v2.PointsTableName,
		v2.HistogramPointsTableName, v2.ExpHistogramPointsTableName, v2.SummaryPointsTableName,
		v2.ExemplarsTableName, v2.PointsTableName + "_5m", v2.PointsTableName + "_1h",
		v2.HistogramPointsTableName + "_5m", v2.HistogramPointsTableName + "_1h",
	} {
		require.NoError(t, db.Exec(t.Context(), fmt.Sprintf("OPTIMIZE TABLE otel_int_test.%q FINAL", table)))
	}

	t.Run("SeriesTableSemantics", func(t *testing.T) { mathTestSeriesTable(t, exp1) })
	t.Run("LabelDiscovery", func(t *testing.T) { mathTestLabelDiscovery(t, exp1) })
	t.Run("GaugeQueries", func(t *testing.T) { mathTestGauges(t, exp1) })
	t.Run("CounterRates", func(t *testing.T) { mathTestCounterRates(t, exp1) })
	t.Run("HistogramQuantiles", func(t *testing.T) { mathTestHistograms(t, exp1) })
	t.Run("ExpHistogramQuantiles", func(t *testing.T) { mathTestExpHistograms(t, exp1) })
	t.Run("SummaryQueries", func(t *testing.T) { mathTestSummary(t, exp1) })
	t.Run("Rollups", func(t *testing.T) { mathTestRollups(t, exp1) })
	t.Run("HistogramRollups", func(t *testing.T) { mathTestHistogramRollups(t, exp1) })
	t.Run("RangeQueryShape", func(t *testing.T) { mathTestRangeQueryShape(t, exp1) })
	t.Run("PromQLFunctionFamily", func(t *testing.T) { mathTestPromQLFunctionFamily(t, exp1) })
	t.Run("FiltersAndMatchers", func(t *testing.T) { mathTestFiltersAndMatchers(t, exp1) })
	t.Run("TopK", func(t *testing.T) { mathTestTopK(t, exp1) })
	t.Run("WindowedHistogramQuantile", func(t *testing.T) { mathTestWindowedHistogramQuantile(t, exp1) })
	t.Run("QuirkFixViews", func(t *testing.T) { mathTestQuirkFixViews(t, exp1) })
	t.Run("DeltaSums", func(t *testing.T) { mathTestDeltaSums(t, exp1) })
	t.Run("AdvancedPromQL", func(t *testing.T) { mathTestAdvancedPromQL(t, exp1) })
	t.Run("InsertScaleAndParts", func(t *testing.T) { mathTestInsertScaleAndParts(t, exp1) })
	t.Run("StalenessMarkers", func(t *testing.T) { mathTestStalenessMarkers(t, exp1) })
}

// mathTestPayload builds scrapes [from, to) for every series described above.
func mathTestPayload(from, to int) pmetric.Metrics {
	md := pmetric.NewMetrics()

	ts := func(i int) pcommon.Timestamp {
		return pcommon.NewTimestampFromTime(mathT0.Add(time.Duration(i) * mathScrapeInterval))
	}
	start := pcommon.NewTimestampFromTime(mathT0)

	type resource struct {
		rm pmetric.ResourceMetrics
		sm pmetric.ScopeMetrics
	}
	newResource := func(service, host, region string) resource {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service.name", service)
		rm.Resource().Attributes().PutStr("host.name", host)
		rm.Resource().Attributes().PutStr("region", region)
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("mathtest")
		sm.Scope().SetVersion("1.0")
		return resource{rm: rm, sm: sm}
	}

	svcA := newResource("svc-a", "host-1", "us-east")
	svcB := newResource("svc-b", "host-2", "eu-west")

	gauge := func(r resource, value func(i int) float64, attrs map[string]string) {
		m := r.sm.Metrics().AppendEmpty()
		m.SetName("test.cpu.utilization")
		m.SetUnit("1")
		g := m.SetEmptyGauge()
		for i := from; i < to; i++ {
			dp := g.DataPoints().AppendEmpty()
			dp.SetTimestamp(ts(i))
			dp.SetStartTimestamp(start)
			dp.SetDoubleValue(value(i))
			for k, v := range attrs {
				dp.Attributes().PutStr(k, v)
			}
			// Exemplars make the exemplar table non-empty so a full push
			// exercises all seven concurrent inserts (deadlock regression
			// coverage for Batch.Insert).
			ex := dp.Exemplars().AppendEmpty()
			ex.SetTimestamp(ts(i))
			ex.SetDoubleValue(value(i))
			ex.FilteredAttributes().PutStr("exemplar.key", "v")
		}
	}
	gauge(svcA, func(i int) float64 { return 10 + float64(i) }, map[string]string{"core": "0"})
	gauge(svcA, func(i int) float64 { return 100 + float64(i) }, map[string]string{"core": "1"})
	gauge(svcB, func(i int) float64 { return 1000 + float64(i) }, map[string]string{"core": "0"})

	counter := func(r resource, value func(i int) float64, attrs map[string]string) {
		m := r.sm.Metrics().AppendEmpty()
		m.SetName("test.requests")
		m.SetUnit("{requests}")
		s := m.SetEmptySum()
		s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		s.SetIsMonotonic(true)
		for i := from; i < to; i++ {
			dp := s.DataPoints().AppendEmpty()
			dp.SetTimestamp(ts(i))
			dp.SetStartTimestamp(start)
			dp.SetDoubleValue(value(i))
			for k, v := range attrs {
				dp.Attributes().PutStr(k, v)
			}
		}
	}
	counter(svcA, func(i int) float64 { return 5 * float64(i) }, map[string]string{"code": "200"})
	counter(svcA, func(i int) float64 { // reset to 0 at scrape 20
		if i < 20 {
			return 5 * float64(i)
		}
		return 5 * float64(i-20)
	}, map[string]string{"code": "500"})
	counter(svcB, func(i int) float64 { return 7 * float64(i) }, map[string]string{"code": "200"})

	// Classic histogram, cumulative.
	{
		m := svcA.sm.Metrics().AppendEmpty()
		m.SetName("test.duration")
		m.SetUnit("s")
		h := m.SetEmptyHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		for i := from; i < to; i++ {
			n := uint64(i + 1)
			dp := h.DataPoints().AppendEmpty()
			dp.SetTimestamp(ts(i))
			dp.SetStartTimestamp(start)
			dp.ExplicitBounds().FromRaw([]float64{0.1, 0.5, 1, 5})
			dp.BucketCounts().FromRaw([]uint64{2 * n, 3 * n, 4 * n, 1 * n, 0})
			dp.SetCount(10 * n)
			dp.SetSum(6 * float64(n))
			dp.SetMin(0.05)
			dp.SetMax(4.9)
			dp.Attributes().PutStr("route", "/api")
		}
	}

	// Second histogram series on the same metric: different route, different
	// distribution (everything lands in the first bucket). Exists so the
	// metric-level quantile queries must aggregate across series correctly.
	{
		m := svcA.sm.Metrics().AppendEmpty()
		m.SetName("test.duration")
		m.SetUnit("s")
		h := m.SetEmptyHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		for i := from; i < to; i++ {
			n := uint64(i + 1)
			dp := h.DataPoints().AppendEmpty()
			dp.SetTimestamp(ts(i))
			dp.SetStartTimestamp(start)
			dp.ExplicitBounds().FromRaw([]float64{0.1, 0.5, 1, 5})
			dp.BucketCounts().FromRaw([]uint64{5 * n, 0, 0, 0, 0})
			dp.SetCount(5 * n)
			dp.SetSum(1 * float64(n))
			dp.SetMin(0.01)
			dp.SetMax(0.09)
			dp.Attributes().PutStr("route", "/web")
		}
	}

	// Delta-temporality histogram: each point carries independent per-bucket
	// increments, so per-bucket window totals are plain element-wise sums (the
	// rollup SumBuckets column is exact for it).
	{
		m := svcB.sm.Metrics().AppendEmpty()
		m.SetName("test.batch.duration")
		m.SetUnit("s")
		h := m.SetEmptyHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		for i := from; i < to; i++ {
			dp := h.DataPoints().AppendEmpty()
			dp.SetTimestamp(ts(i))
			dp.SetStartTimestamp(ts(i - 1))
			dp.ExplicitBounds().FromRaw([]float64{1, 10})
			dp.BucketCounts().FromRaw([]uint64{uint64(i), uint64(2 * i), 3})
			dp.SetCount(uint64(3*i + 3))
			dp.SetSum(0.5 * float64(i)) // 0.5 is binary-exact: float sums stay exact
			dp.SetMin(0.001)
			dp.SetMax(float64(i))
			dp.Attributes().PutStr("job", "batch")
		}
	}

	// Delta-temporality monotonic sum: each point is an independent increment,
	// so windowed increase is a plain SQL sum (no rate function, no resets).
	{
		m := svcB.sm.Metrics().AppendEmpty()
		m.SetName("test.jobs")
		m.SetUnit("{jobs}")
		s := m.SetEmptySum()
		s.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		s.SetIsMonotonic(true)
		for i := from; i < to; i++ {
			dp := s.DataPoints().AppendEmpty()
			dp.SetTimestamp(ts(i))
			dp.SetStartTimestamp(ts(i - 1))
			dp.SetDoubleValue(2)
			dp.Attributes().PutStr("queue", "q1")
		}
	}

	// Exponential histogram, delta: every point identical.
	{
		m := svcB.sm.Metrics().AppendEmpty()
		m.SetName("test.rpc.duration")
		m.SetUnit("s")
		h := m.SetEmptyExponentialHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		for i := from; i < to; i++ {
			dp := h.DataPoints().AppendEmpty()
			dp.SetTimestamp(ts(i))
			dp.SetStartTimestamp(ts(i - 1))
			dp.SetScale(2)
			dp.SetZeroCount(0)
			dp.Positive().SetOffset(0)
			dp.Positive().BucketCounts().FromRaw([]uint64{10, 20, 30})
			dp.SetCount(60)
			dp.SetSum(100)
			dp.Attributes().PutStr("method", "Get")
		}
	}

	// Summary.
	{
		m := svcA.sm.Metrics().AppendEmpty()
		m.SetName("test.legacy.summary")
		m.SetUnit("s")
		s := m.SetEmptySummary()
		for i := from; i < to; i++ {
			n := float64(i + 1)
			dp := s.DataPoints().AppendEmpty()
			dp.SetTimestamp(ts(i))
			dp.SetStartTimestamp(start)
			dp.SetCount(uint64(10 * n))
			dp.SetSum(5 * n)
			for qi, q := range []float64{0.5, 0.9, 0.99} {
				qv := dp.QuantileValues().AppendEmpty()
				qv.SetQuantile(q)
				qv.SetValue(float64(qi + 1))
			}
		}
	}

	return md
}

// scanFloat runs a query expected to return a single float-able value.
func scanFloat(t *testing.T, e *metricsV2Exporter, query string, args ...any) float64 {
	t.Helper()
	ctx := clickhouse.Context(t.Context(), clickhouse.WithSettings(clickhouse.Settings{
		"allow_experimental_time_series_aggregate_functions": 1,
	}))
	row := e.db.QueryRow(ctx, query, args...)
	require.NoError(t, row.Err())
	var v float64
	require.NoError(t, row.Scan(&v))
	return v
}

func scanUInt(t *testing.T, e *metricsV2Exporter, query string, args ...any) uint64 {
	t.Helper()
	row := e.db.QueryRow(t.Context(), query, args...)
	require.NoError(t, row.Err())
	var v uint64
	require.NoError(t, row.Scan(&v))
	return v
}

func mathTestSeriesTable(t *testing.T, e *metricsV2Exporter) {
	v2 := &e.cfg.MetricsV2

	// Two replicas wrote duplicate series rows; OPTIMIZE FINAL must collapse
	// them to exactly one row per series.
	require.Equal(t, uint64(3), scanUInt(t, e,
		fmt.Sprintf(`SELECT count() FROM otel_int_test.%q WHERE MetricName = 'test.cpu.utilization'`, v2.SeriesTableName)))

	// SimpleAggregateFunction(min/max): FirstSeen from replica 1 (scrape 0),
	// LastSeen from replica 2's first sighting (scrape 20 = T0+300s).
	// LastSeen is day-granular by design: it is NOT the last point (T0+585s).
	var firstSeen, lastSeen time.Time
	row := e.db.QueryRow(t.Context(), fmt.Sprintf(
		`SELECT FirstSeen, LastSeen FROM otel_int_test.%q
		 WHERE MetricName = 'test.cpu.utilization' AND ServiceName = 'svc-a' AND Attributes['core'] = '0'`,
		v2.SeriesTableName))
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&firstSeen, &lastSeen))
	assert.Equal(t, mathT0.UTC(), firstSeen.UTC())
	assert.Equal(t, mathT0.Add(20*mathScrapeInterval).UTC(), lastSeen.UTC())

	// Referential integrity: every point resolves to a series row.
	require.Equal(t, uint64(0), scanUInt(t, e, fmt.Sprintf(
		`SELECT count() FROM otel_int_test.%q AS p LEFT ANTI JOIN otel_int_test.%q AS s ON p.SeriesHash = s.SeriesHash`,
		v2.PointsTableName, v2.SeriesTableName)))

	// Families collapse to one row per (name, type, unit) after FINAL.
	require.Equal(t, uint64(7), scanUInt(t, e,
		fmt.Sprintf(`SELECT count() FROM otel_int_test.%q`, v2.FamiliesTableName)))
}

func mathTestLabelDiscovery(t *testing.T, e *metricsV2Exporter) {
	v2 := &e.cfg.MetricsV2

	// "Which label keys does this metric have?" — series table only.
	rows, err := e.db.Query(t.Context(), fmt.Sprintf(
		`SELECT DISTINCT arrayJoin(mapKeys(Attributes)) AS key FROM otel_int_test.%q
		 WHERE MetricName = 'test.requests' ORDER BY key`, v2.SeriesTableName))
	require.NoError(t, err)
	var keys []string
	for rows.Next() {
		var k string
		require.NoError(t, rows.Scan(&k))
		keys = append(keys, k)
	}
	require.NoError(t, rows.Close())
	assert.Equal(t, []string{"code"}, keys)

	// "Which values does label `code` take, filtered by service?"
	rows, err = e.db.Query(t.Context(), fmt.Sprintf(
		`SELECT DISTINCT Attributes['code'] AS v FROM otel_int_test.%q
		 WHERE MetricName = 'test.requests' AND ServiceName = 'svc-a' ORDER BY v`, v2.SeriesTableName))
	require.NoError(t, err)
	var vals []string
	for rows.Next() {
		var v string
		require.NoError(t, rows.Scan(&v))
		vals = append(vals, v)
	}
	require.NoError(t, rows.Close())
	assert.Equal(t, []string{"200", "500"}, vals)
}

func mathTestGauges(t *testing.T, e *metricsV2Exporter) {
	v2 := &e.cfg.MetricsV2

	// Instant view: latest value per series, filtered by a resource-level
	// attribute that lives on the series table (two-phase join shape).
	rows, err := e.db.Query(t.Context(), fmt.Sprintf(
		`SELECT s.Attributes['core'] AS core, r.last
		 FROM (
		     SELECT SeriesHash, argMax(Value, TimeUnix) AS last
		     FROM otel_int_test.%q
		     WHERE MetricName = 'test.cpu.utilization' AND SeriesHash IN (
		         SELECT SeriesHash FROM otel_int_test.%q
		         WHERE MetricName = 'test.cpu.utilization' AND ServiceName = 'svc-a' AND Date = toDate('2024-01-01'))
		     GROUP BY SeriesHash
		 ) AS r
		 ANY INNER JOIN otel_int_test.%q AS s ON r.SeriesHash = s.SeriesHash
		 ORDER BY core`, v2.PointsTableName, v2.SeriesTableName, v2.SeriesTableName))
	require.NoError(t, err)
	got := map[string]float64{}
	for rows.Next() {
		var core string
		var last float64
		require.NoError(t, rows.Scan(&core, &last))
		got[core] = last
	}
	require.NoError(t, rows.Close())
	// v_39 = 10+39 and 100+39.
	assert.Equal(t, map[string]float64{"0": 49, "1": 139}, got)

	// Aggregates over one series, resolved via IN subquery (the other
	// two-phase shape; avoids the join).
	q := fmt.Sprintf(
		`SELECT %%s FROM otel_int_test.%q
		 WHERE MetricName = 'test.cpu.utilization' AND SeriesHash IN (
		     SELECT SeriesHash FROM otel_int_test.%q
		     WHERE MetricName = 'test.cpu.utilization' AND ServiceName = 'svc-a' AND Attributes['core'] = '0' AND Date = toDate('2024-01-01'))`,
		v2.PointsTableName, v2.SeriesTableName)
	assert.InDelta(t, 10, scanFloat(t, e, fmt.Sprintf(q, "min(Value)")), 1e-9)
	assert.InDelta(t, 49, scanFloat(t, e, fmt.Sprintf(q, "max(Value)")), 1e-9)
	assert.InDelta(t, 29.5, scanFloat(t, e, fmt.Sprintf(q, "avg(Value)")), 1e-9) // mean of 10..49

	// PromQL-style "last value at evaluation time with staleness": grid
	// function over raw points. Eval at T0+600s, staleness 30s; last sample
	// at T0+585s is inside the staleness window.
	last := scanFloat(t, e, fmt.Sprintf(
		`SELECT (timeSeriesResampleToGridWithStaleness(toDateTime('2024-01-01 00:10:00', 'UTC'), toDateTime('2024-01-01 00:10:00', 'UTC'), 15, 30)(TimeUnix, Value))[1]
		 FROM otel_int_test.%q
		 WHERE MetricName = 'test.cpu.utilization'
		   AND TimeUnix > toDateTime('2024-01-01 00:09:30', 'UTC') AND TimeUnix <= toDateTime('2024-01-01 00:10:00', 'UTC')
		   AND SeriesHash IN (
		     SELECT SeriesHash FROM otel_int_test.%q
		     WHERE MetricName = 'test.cpu.utilization' AND ServiceName = 'svc-a' AND Attributes['core'] = '0' AND Date = toDate('2024-01-01'))`,
		v2.PointsTableName, v2.SeriesTableName))
	assert.InDelta(t, 49, last, 1e-9)
}

func mathTestCounterRates(t *testing.T, e *metricsV2Exporter) {
	v2 := &e.cfg.MetricsV2

	rateQuery := func(code, service string, evalTime string, window int) string {
		return fmt.Sprintf(
			`SELECT (timeSeriesRateToGrid(toDateTime('%s', 'UTC'), toDateTime('%s', 'UTC'), %d, %d)(TimeUnix, Value))[1]
			 FROM otel_int_test.%q
			 WHERE MetricName = 'test.requests'
			   AND TimeUnix > subtractSeconds(toDateTime('%s', 'UTC'), %d) AND TimeUnix <= toDateTime('%s', 'UTC')
			   AND SeriesHash IN (
			     SELECT SeriesHash FROM otel_int_test.%q
			     WHERE MetricName = 'test.requests' AND ServiceName = '%s' AND Attributes['code'] = '%s' AND Date = toDate('2024-01-01'))`,
			evalTime, evalTime, window, window, v2.PointsTableName, evalTime, window, evalTime, v2.SeriesTableName, service, code)
	}

	// Clean counter, rate over (T0+300, T0+600]: samples i=21..39,
	// v: 105..195, delta=90 over 270s sampled; PromQL extrapolation extends
	// to the full 300s window => rate = 90/270 = 1/3. (= 5 per 15s scrape)
	assert.InDelta(t, 1.0/3.0, scanFloat(t, e, rateQuery("200", "svc-a", "2024-01-01 00:10:00", 300)), 1e-6)

	// Second series, same shape: 7 per 15s => 7/15.
	assert.InDelta(t, 7.0/15.0, scanFloat(t, e, rateQuery("200", "svc-b", "2024-01-01 00:10:00", 300)), 1e-6)

	// Reset series over (T0+300, T0+600]: the reset (i=20, t=300s) falls on
	// the window boundary and PromQL windows are left-open, so the window
	// sees a clean monotone segment: rate = 1/3 again.
	assert.InDelta(t, 1.0/3.0, scanFloat(t, e, rateQuery("500", "svc-a", "2024-01-01 00:10:00", 300)), 1e-6)

	// Reset INSIDE the window (T0+150, T0+600]: samples i=11..39,
	// first=55 @165s, pre-reset last=95 @285s, reset to 0 @300s, last=95 @585s.
	// PromQL reset-corrected delta = (95 - 55) + 95 = 135 over 420s sampled,
	// extrapolated to 450s => rate = 135 * (450/420) / 450 = 135/420.
	assert.InDelta(t, 135.0/420.0, scanFloat(t, e, rateQuery("500", "svc-a", "2024-01-01 00:10:00", 450)), 1e-6)

	// increase() over the same window is rate * window.
	increase := scanFloat(t, e, fmt.Sprintf(
		`SELECT (timeSeriesDeltaToGrid(toDateTime('2024-01-01 00:10:00', 'UTC'), toDateTime('2024-01-01 00:10:00', 'UTC'), 300, 300)(TimeUnix, Value))[1]
		 FROM otel_int_test.%q
		 WHERE MetricName = 'test.requests'
		   AND TimeUnix > toDateTime('2024-01-01 00:05:00', 'UTC') AND TimeUnix <= toDateTime('2024-01-01 00:10:00', 'UTC')
		   AND SeriesHash IN (
		     SELECT SeriesHash FROM otel_int_test.%q
		     WHERE MetricName = 'test.requests' AND ServiceName = 'svc-a' AND Attributes['code'] = '200' AND Date = toDate('2024-01-01'))`,
		v2.PointsTableName, v2.SeriesTableName))
	assert.InDelta(t, 100.0, increase, 1e-4, "delta over (300,600] extrapolated to the window")

	// Aggregation across series: sum(rate(...)) by service — the PromQL
	// translation shape for `sum by (service) (rate(test_requests[5m]))`.
	rows, err := e.db.Query(clickhouse.Context(t.Context(), clickhouse.WithSettings(clickhouse.Settings{
		"allow_experimental_time_series_aggregate_functions": 1,
	})), fmt.Sprintf(
		`SELECT s.ServiceName AS service, sum(rate_arr[1]) AS total_rate
		 FROM (
		     SELECT SeriesHash,
		            timeSeriesRateToGrid(toDateTime('2024-01-01 00:10:00', 'UTC'), toDateTime('2024-01-01 00:10:00', 'UTC'), 300, 300)(TimeUnix, Value) AS rate_arr
		     FROM otel_int_test.%q
		     WHERE MetricName = 'test.requests'
		       AND TimeUnix > toDateTime('2024-01-01 00:05:00', 'UTC') AND TimeUnix <= toDateTime('2024-01-01 00:10:00', 'UTC')
		     GROUP BY SeriesHash
		 ) AS r
		 ANY INNER JOIN otel_int_test.%q AS s ON r.SeriesHash = s.SeriesHash
		 GROUP BY service ORDER BY service`, v2.PointsTableName, v2.SeriesTableName))
	require.NoError(t, err)
	rates := map[string]float64{}
	for rows.Next() {
		var svc string
		var rate float64
		require.NoError(t, rows.Scan(&svc, &rate))
		rates[svc] = rate
	}
	require.NoError(t, rows.Close())
	require.Len(t, rates, 2)
	assert.InDelta(t, 1.0/3.0+1.0/3.0, rates["svc-a"], 1e-6) // code=200 + code=500
	assert.InDelta(t, 7.0/15.0, rates["svc-b"], 1e-6)
}

func mathTestHistograms(t *testing.T, e *metricsV2Exporter) {
	v2 := &e.cfg.MetricsV2

	// PromQL histogram_quantile at the latest point. OTLP bucket counts are
	// per-bucket; Prometheus buckets are cumulative-in-le, so the query takes
	// arrayCumSum and appends the +Inf bucket. Bounds come from the series
	// table (stored once per series).
	quantile := func(phi float64) float64 {
		return scanFloat(t, e, fmt.Sprintf(
			`WITH latest AS (
			     SELECT s.ExplicitBounds AS bounds, r.counts
			     FROM (
			         SELECT SeriesHash, argMax(BucketCounts, TimeUnix) AS counts
			         FROM otel_int_test.%q
			         WHERE MetricName = 'test.duration' AND SeriesHash IN (
			             SELECT SeriesHash FROM otel_int_test.%q
			             WHERE MetricName = 'test.duration' AND Attributes['route'] = '/api' AND Date = toDate('2024-01-01'))
			         GROUP BY SeriesHash
			     ) AS r
			     ANY INNER JOIN otel_int_test.%q AS s ON r.SeriesHash = s.SeriesHash
			 )
			 SELECT quantilePrometheusHistogram(%f)(le, cum)
			 FROM (
			     SELECT tup.1 AS le, toFloat64(tup.2) AS cum
			     FROM latest
			     ARRAY JOIN arrayZip(arrayConcat(bounds, [toFloat64(inf)]), arrayCumSum(counts)) AS tup
			 )`,
			v2.HistogramPointsTableName, v2.SeriesTableName, v2.SeriesTableName, phi))
	}
	// Last point: per-bucket counts [80 120 160 40 0] => cumulative
	// [80 200 360 400 400] over bounds [0.1 0.5 1 5 +Inf], total 400.
	// q50: rank 200 -> bucket (0.1,0.5], 0.1 + 0.4*(200-80)/120 = 0.5
	// q90: rank 360 -> bucket (0.5,1],   0.5 + 0.5*(360-200)/160 = 1.0
	// q99: rank 396 -> bucket (1,5],     1 + 4*(396-360)/40 = 4.6
	assert.InDelta(t, 0.5, quantile(0.5), 1e-9)
	assert.InDelta(t, 1.0, quantile(0.9), 1e-9)
	assert.InDelta(t, 4.6, quantile(0.99), 1e-9)

	// Average latency at the latest point: Sum/Count = 240/400.
	avg := scanFloat(t, e, fmt.Sprintf(
		`SELECT argMax(Sum, TimeUnix) / argMax(Count, TimeUnix)
		 FROM otel_int_test.%q WHERE MetricName = 'test.duration' AND SeriesHash IN (
		     SELECT SeriesHash FROM otel_int_test.%q
		     WHERE MetricName = 'test.duration' AND Attributes['route'] = '/api' AND Date = toDate('2024-01-01'))`,
		v2.HistogramPointsTableName, v2.SeriesTableName))
	assert.InDelta(t, 0.6, avg, 1e-9)

	// Request rate from the histogram Count column — plain counter math:
	// Count_i = 10(i+1): first-in-window (i=21) 220, last (i=39) 400,
	// delta=180 over 270s, extrapolated => 180/270 = 2/3.
	countRate := scanFloat(t, e, fmt.Sprintf(
		`SELECT (timeSeriesRateToGrid(toDateTime('2024-01-01 00:10:00', 'UTC'), toDateTime('2024-01-01 00:10:00', 'UTC'), 300, 300)(TimeUnix, toFloat64(Count)))[1]
		 FROM otel_int_test.%q WHERE MetricName = 'test.duration'
		   AND TimeUnix > toDateTime('2024-01-01 00:05:00', 'UTC') AND TimeUnix <= toDateTime('2024-01-01 00:10:00', 'UTC')
		   AND SeriesHash IN (
		     SELECT SeriesHash FROM otel_int_test.%q
		     WHERE MetricName = 'test.duration' AND Attributes['route'] = '/api' AND Date = toDate('2024-01-01'))`,
		v2.HistogramPointsTableName, v2.SeriesTableName))
	assert.InDelta(t, 2.0/3.0, countRate, 1e-6)
}

func mathTestExpHistograms(t *testing.T, e *metricsV2Exporter) {
	v2 := &e.cfg.MetricsV2

	// Delta temporality: sums over a window are plain SQL sums.
	assert.InDelta(t, 4000, scanFloat(t, e, fmt.Sprintf(
		`SELECT sum(Sum) FROM otel_int_test.%q WHERE MetricName = 'test.rpc.duration'`,
		v2.ExpHistogramPointsTableName)), 1e-9)
	require.Equal(t, uint64(2400), scanUInt(t, e, fmt.Sprintf(
		`SELECT sum(Count) FROM otel_int_test.%q WHERE MetricName = 'test.rpc.duration'`,
		v2.ExpHistogramPointsTableName)))

	// Exp-histogram quantile from raw buckets, computed in SQL. Bucket k
	// covers (base^(offset+k), base^(offset+k+1)] with base = 2^(2^-scale).
	// One point: scale=2 => base=2^0.25, counts [10 20 30], total 60.
	// q50: rank=30, cum=[10 30 60] => bucket k=1: lower=base^1, upper=base^2,
	// interp = lower + (upper-lower)*(30-10)/20 = base^2 = sqrt(2).
	q50 := scanFloat(t, e, fmt.Sprintf(
		`WITH point AS (
		     SELECT Scale, PositiveOffset, PositiveBucketCounts AS counts
		     FROM otel_int_test.%q
		     WHERE MetricName = 'test.rpc.duration' ORDER BY TimeUnix DESC LIMIT 1
		 )
		 SELECT lower + (upper - lower) * (rank - prevCum) / bucketCount
		 FROM (
		     SELECT
		         pow(2, pow(2, -Scale)) AS base,
		         arrayCumSum(counts) AS cum,
		         arraySum(counts) * 0.5 AS rank,
		         arrayFirstIndex(c -> c >= rank, cum) AS idx,
		         if(idx = 1, 0, cum[idx - 1]) AS prevCum,
		         toFloat64(counts[idx]) AS bucketCount,
		         pow(base, PositiveOffset + idx - 1) AS lower,
		         pow(base, PositiveOffset + idx) AS upper
		     FROM point
		 )`, v2.ExpHistogramPointsTableName))
	assert.InDelta(t, math.Sqrt2, q50, 1e-9)
}

func mathTestSummary(t *testing.T, e *metricsV2Exporter) {
	v2 := &e.cfg.MetricsV2

	// Latest quantile values, positionally aligned with the series-table
	// quantile levels.
	rows, err := e.db.Query(t.Context(), fmt.Sprintf(
		`SELECT s.Quantiles AS levels, r.values
		 FROM (
		     SELECT SeriesHash, argMax(QuantileValues, TimeUnix) AS values
		     FROM otel_int_test.%q
		     WHERE MetricName = 'test.legacy.summary'
		     GROUP BY SeriesHash
		 ) AS r
		 ANY INNER JOIN otel_int_test.%q AS s ON r.SeriesHash = s.SeriesHash`,
		v2.SummaryPointsTableName, v2.SeriesTableName))
	require.NoError(t, err)
	require.True(t, rows.Next())
	var levels, values []float64
	require.NoError(t, rows.Scan(&levels, &values))
	require.NoError(t, rows.Close())
	assert.Equal(t, []float64{0.5, 0.9, 0.99}, levels)
	assert.Equal(t, []float64{1, 2, 3}, values)
}

func mathTestRollups(t *testing.T, e *metricsV2Exporter) {
	v2 := &e.cfg.MetricsV2
	table5m := v2.PointsTableName + "_5m"
	table1h := v2.PointsTableName + "_1h"

	seriesFilter := fmt.Sprintf(
		`SeriesHash IN (SELECT SeriesHash FROM otel_int_test.%q
		 WHERE MetricName = 'test.cpu.utilization' AND ServiceName = 'svc-a' AND Attributes['core'] = '0' AND Date = toDate('2024-01-01'))`,
		v2.SeriesTableName)

	// Gauge over the 5m rollup. The GROUP BY + -Merge form works regardless
	// of merge state, so it's the shape a query layer should always emit.
	type bucket struct {
		first, last, min, max, sum float64
		count                      uint64
	}
	rows, err := e.db.Query(t.Context(), fmt.Sprintf(
		`SELECT TimeBucket, argMinMerge(First) AS fst, argMaxMerge(Last) AS last, min(Min) AS mn, max(Max) AS mx, sum(Sum) AS sm, sum(Count) AS cnt
		 FROM otel_int_test.%q
		 WHERE MetricName = 'test.cpu.utilization' AND %s
		 GROUP BY TimeBucket ORDER BY TimeBucket`, table5m, seriesFilter))
	require.NoError(t, err)
	var buckets []bucket
	for rows.Next() {
		var ts time.Time
		var b bucket
		require.NoError(t, rows.Scan(&ts, &b.first, &b.last, &b.min, &b.max, &b.sum, &b.count))
		buckets = append(buckets, b)
	}
	require.NoError(t, rows.Close())
	// Bucket 0 = scrapes 0..19 (v=10..29), bucket 1 = scrapes 20..39 (v=30..49).
	require.Len(t, buckets, 2)
	assert.Equal(t, bucket{first: 10, last: 29, min: 10, max: 29, sum: 390, count: 20}, buckets[0])
	assert.Equal(t, bucket{first: 30, last: 49, min: 30, max: 49, sum: 790, count: 20}, buckets[1])

	// Quantiles over the rollup tier come from the mergeable BFloat16 sketch;
	// any level can be requested at query time. Median of bucket 0 (values
	// 10..29) is ~19.5 within bfloat16/rank resolution.
	medianB0 := scanFloat(t, e, fmt.Sprintf(
		`SELECT quantileBFloat16Merge(0.5)(ValueSketch) FROM otel_int_test.%q
		 WHERE MetricName = 'test.cpu.utilization' AND %s AND TimeBucket = toDateTime('2024-01-01 00:00:00', 'UTC')`,
		table5m, seriesFilter))
	assert.InDelta(t, 19.5, medianB0, 1.0)

	// The same sketch survives the cascade into the 1h rollup: p90 over all
	// 40 points (10..49) is ~45.5 within sketch resolution.
	p901h := scanFloat(t, e, fmt.Sprintf(
		`SELECT quantileBFloat16Merge(0.9)(ValueSketch) FROM otel_int_test.%q
		 WHERE MetricName = 'test.cpu.utilization' AND %s`, table1h, seriesFilter))
	assert.InDelta(t, 45.5, p901h, 1.5)

	// Raw tier is unrestricted: any ClickHouse aggregate runs directly on the
	// plain Float64 Value column, including quantileBFloat16.
	p90raw := scanFloat(t, e, fmt.Sprintf(
		`SELECT quantileBFloat16(0.9)(Value) FROM otel_int_test.%q
		 WHERE MetricName = 'test.cpu.utilization' AND %s`, v2.PointsTableName, seriesFilter))
	assert.InDelta(t, 45.5, p90raw, 1.5)

	// 1h rollup (cascaded from the 5m table through the second MV).
	var last1h, sum1h float64
	var count1h uint64
	row := e.db.QueryRow(t.Context(), fmt.Sprintf(
		`SELECT argMaxMerge(Last), sum(Sum), sum(Count) FROM otel_int_test.%q
		 WHERE MetricName = 'test.cpu.utilization' AND %s GROUP BY TimeBucket`, table1h, seriesFilter))
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&last1h, &sum1h, &count1h))
	assert.InDelta(t, 49, last1h, 1e-9)
	assert.InDelta(t, 1180, sum1h, 1e-9) // sum of 10..49
	assert.Equal(t, uint64(40), count1h)

	// Long-range counter increase from the rollup tier, the Thanos-style
	// downsampled-counter shape: chain per-bucket (First, Last) pairs in
	// bucket order. A drop between one bucket's Last and the next bucket's
	// First is a detected reset (count the next First from zero). This is
	// exact for resets at or between bucket boundaries; a reset strictly
	// inside one bucket still needs the raw tier (or an offline rollup
	// rebuild from raw, which is always possible since rollups are derived).
	counterRollupIncrease := func(code string) float64 {
		return scanFloat(t, e, fmt.Sprintf(
			`SELECT sum(bucket_increase) FROM (
			     SELECT
			         F, L,
			         lagInFrame(L, 1) OVER w AS prevL,
			         row_number() OVER w AS rn,
			         if(rn = 1, L - F, if(F >= prevL, F - prevL, F) + (L - F)) AS bucket_increase
			     FROM (
			         SELECT TimeBucket, argMinMerge(First) AS F, argMaxMerge(Last) AS L
			         FROM otel_int_test.%q
			         WHERE MetricName = 'test.requests' AND SeriesHash IN (
			             SELECT SeriesHash FROM otel_int_test.%q
			             WHERE MetricName = 'test.requests' AND ServiceName = 'svc-a' AND Attributes['code'] = '%s' AND Date = toDate('2024-01-01'))
			         GROUP BY TimeBucket
			     )
			     WINDOW w AS (ORDER BY TimeBucket ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
			 )`, table5m, v2.SeriesTableName, code))
	}
	// Clean counter: buckets (F=0,L=95),(F=100,L=195):
	// 95 + (100-95) + (195-100) = 195 — the exact total increase.
	assert.InDelta(t, 195.0, counterRollupIncrease("200"), 1e-9)

	// Reset counter: buckets (F=0,L=95),(F=0,L=95). The second bucket's
	// First (0) < previous Last (95) reveals the reset; the chained increase
	// is 95 + 0 + 95 = 190 — matching PromQL's reset-corrected increase over
	// the raw samples.
	assert.InDelta(t, 190.0, counterRollupIncrease("500"), 1e-9)
}

// mathTestHistogramRollups validates the explicit-bounds histogram rollup
// tiers (5m + 1h): exact delta per-bucket window sums from the sumForEach
// state, cumulative per-le First/Last chaining, the 1h cascade, avg latency
// from the scalar columns, and quantilePrometheusHistogram over
// rollup-derived increases matching the raw-tier value. Exponential
// histograms are deliberately not rolled up (variable per-point Scale/Offset
// makes element-wise bucket aggregation unsafe without downscale-merge).
func mathTestHistogramRollups(t *testing.T, e *metricsV2Exporter) {
	v2 := &e.cfg.MetricsV2
	table5m := v2.HistogramPointsTableName + "_5m"
	table1h := v2.HistogramPointsTableName + "_1h"

	// --- Delta temporality (test.batch.duration, buckets [i 2i 3]) ---
	// SumBuckets is the exact element-wise per-bucket sum over any bucket
	// window. 5m bucket 0 = scrapes 0..19 (sum i = 190) => [190 380 60],
	// bucket 1 = scrapes 20..39 (sum i = 590) => [590 1180 60]. Scalars:
	// SumCount = 3*sum(i)+3*20, SumSum = 0.5*sum(i), PointCount = 20.
	type deltaBucket struct {
		buckets []uint64
		count   uint64
		sum     float64
		points  uint64
	}
	rows, err := e.db.Query(t.Context(), fmt.Sprintf(
		`SELECT sumForEachMerge(SumBuckets) AS b, sum(SumCount) AS c, sum(SumSum) AS s, sum(PointCount) AS p
		 FROM otel_int_test.%q
		 WHERE MetricName = 'test.batch.duration'
		 GROUP BY TimeBucket ORDER BY TimeBucket`, table5m))
	require.NoError(t, err)
	var deltaBuckets []deltaBucket
	for rows.Next() {
		var b deltaBucket
		require.NoError(t, rows.Scan(&b.buckets, &b.count, &b.sum, &b.points))
		deltaBuckets = append(deltaBuckets, b)
	}
	require.NoError(t, rows.Close())
	require.Len(t, deltaBuckets, 2)
	assert.Equal(t, deltaBucket{buckets: []uint64{190, 380, 60}, count: 630, sum: 95, points: 20}, deltaBuckets[0])
	assert.Equal(t, deltaBucket{buckets: []uint64{590, 1180, 60}, count: 1830, sum: 295, points: 20}, deltaBuckets[1])

	// The same totals from the 1h cascade (single hour bucket): sum i for
	// i=0..39 is 780 => [780 1560 120], count 3*780+120 = 2460, sum 390.
	var buckets1h []uint64
	var count1h, points1h uint64
	var sum1h float64
	row := e.db.QueryRow(t.Context(), fmt.Sprintf(
		`SELECT sumForEachMerge(SumBuckets), sum(SumCount), sum(SumSum), sum(PointCount)
		 FROM otel_int_test.%q WHERE MetricName = 'test.batch.duration'`, table1h))
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&buckets1h, &count1h, &sum1h, &points1h))
	assert.Equal(t, []uint64{780, 1560, 120}, buckets1h)
	assert.Equal(t, uint64(2460), count1h)
	assert.InDelta(t, 390.0, sum1h, 1e-9)
	assert.Equal(t, uint64(40), points1h)

	// Delta avg latency from the rollup: sum(Sum)/sum(Count) = 390/2460.
	assert.InDelta(t, 390.0/2460.0, scanFloat(t, e, fmt.Sprintf(
		`SELECT sum(SumSum) / sum(SumCount) FROM otel_int_test.%q
		 WHERE MetricName = 'test.batch.duration'`, table5m)), 1e-9)

	// --- Cumulative temporality (test.duration{route=/api}) ---
	apiSeries := fmt.Sprintf(
		`SeriesHash IN (SELECT SeriesHash FROM otel_int_test.%q
		 WHERE MetricName = 'test.duration' AND Attributes['route'] = '/api' AND Date = toDate('2024-01-01'))`,
		v2.SeriesTableName)

	// Per-5m-bucket First/Last arrays. Bucket 0: first point i=0 has buckets
	// [2 3 4 1 0], last point i=19 has [40 60 80 20 0]; bucket 1: i=20 =>
	// [42 63 84 21 0], i=39 => [80 120 160 40 0].
	rows, err = e.db.Query(t.Context(), fmt.Sprintf(
		`SELECT argMinMerge(FirstBuckets), argMaxMerge(LastBuckets)
		 FROM otel_int_test.%q WHERE MetricName = 'test.duration' AND %s
		 GROUP BY TimeBucket ORDER BY TimeBucket`, table5m, apiSeries))
	require.NoError(t, err)
	var firsts, lasts [][]uint64
	for rows.Next() {
		var f, l []uint64
		require.NoError(t, rows.Scan(&f, &l))
		firsts = append(firsts, f)
		lasts = append(lasts, l)
	}
	require.NoError(t, rows.Close())
	require.Len(t, firsts, 2)
	assert.Equal(t, [][]uint64{{2, 3, 4, 1, 0}, {42, 63, 84, 21, 0}}, firsts)
	assert.Equal(t, [][]uint64{{40, 60, 80, 20, 0}, {80, 120, 160, 40, 0}}, lasts)

	// Chained per-le increase across bucket windows (the histogram analog of
	// the counter First/Last chaining): within-bucket (L-F) plus cross-bucket
	// (F - prev L, or F alone on a detected reset). Expected = raw
	// argMax-argMin = [80-2 120-3 160-4 40-1 0] = [78 117 156 39 0].
	chained := func(table string) string {
		return fmt.Sprintf(
			`SELECT groupArray(inc) FROM (
			     SELECT idx, sum(bucket_increase) AS inc FROM (
			         SELECT idx, F, L,
			             lagInFrame(L, 1) OVER w AS prevL,
			             row_number() OVER w AS rn,
			             if(rn = 1, L - F, if(F >= prevL, F - prevL, F) + (L - F)) AS bucket_increase
			         FROM (
			             SELECT TimeBucket, idx, toFloat64(FB[idx]) AS F, toFloat64(LB[idx]) AS L
			             FROM (
			                 SELECT TimeBucket, argMinMerge(FirstBuckets) AS FB, argMaxMerge(LastBuckets) AS LB
			                 FROM otel_int_test.%q
			                 WHERE MetricName = 'test.duration' AND %s
			                 GROUP BY TimeBucket
			             ) ARRAY JOIN arrayEnumerate(FB) AS idx
			         )
			         WINDOW w AS (PARTITION BY idx ORDER BY TimeBucket ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
			     ) GROUP BY idx ORDER BY idx
			 )`, table, apiSeries)
	}
	for _, table := range []string{table5m, table1h} {
		var increases []float64
		row := e.db.QueryRow(t.Context(), chained(table))
		require.NoError(t, row.Err())
		require.NoError(t, row.Scan(&increases))
		assert.Equal(t, []float64{78, 117, 156, 39, 0}, increases, "chained per-le increase from %s", table)
	}

	// Cumulative avg latency over the range from the rollup scalars:
	// (Last Sum - First Sum) / (Last Count - First Count)
	// = (240-6) / (400-10) = 234/390 = 0.6 (no reset in the data).
	avg := scanFloat(t, e, fmt.Sprintf(
		`SELECT (argMaxMerge(LastSum) - argMinMerge(FirstSum)) / (argMaxMerge(LastCount) - argMinMerge(FirstCount))
		 FROM otel_int_test.%q WHERE MetricName = 'test.duration' AND %s`, table5m, apiSeries))
	assert.InDelta(t, 0.6, avg, 1e-9)

	// --- Multi-series histogram_quantile from the rollup tier ---
	// Full general recipe: per-series per-le chained increases, reassembled
	// into ONE increases-array row per series before the label join (rule
	// 5.0-3: an ANY join against a per-le row set collapses to one row per
	// key — silently wrong), then cumulative-in-le, sum per le ACROSS series,
	// then quantilePrometheusHistogram. Increases: /api [78 117 156 39 0],
	// /web [195 0 0 0 0]; per-le sums [273 117 156 39 0], cumulative
	// [273 390 546 585 585], total 585. q95: rank 555.75 -> bucket (1,5]:
	// 1 + 4*(555.75-546)/39 = 2.0 — identical to the raw-tier value in
	// mathTestHistograms.
	q95 := scanFloat(t, e, fmt.Sprintf(
		`WITH per_series AS (
		     SELECT SeriesHash,
		            arrayMap(t -> t.2, arraySort(t -> t.1, groupArray((idx, inc)))) AS increases
		     FROM (
		         SELECT SeriesHash, idx, sum(bucket_increase) AS inc
		         FROM (
		             SELECT SeriesHash, idx, F, L,
		                 lagInFrame(L, 1) OVER w AS prevL,
		                 row_number() OVER w AS rn,
		                 if(rn = 1, L - F, if(F >= prevL, F - prevL, F) + (L - F)) AS bucket_increase
		             FROM (
		                 SELECT SeriesHash, TimeBucket, idx, toFloat64(FB[idx]) AS F, toFloat64(LB[idx]) AS L
		                 FROM (
		                     SELECT SeriesHash, TimeBucket, argMinMerge(FirstBuckets) AS FB, argMaxMerge(LastBuckets) AS LB
		                     FROM otel_int_test.%q
		                     WHERE MetricName = 'test.duration'
		                     GROUP BY SeriesHash, TimeBucket
		                 ) ARRAY JOIN arrayEnumerate(FB) AS idx
		             )
		             WINDOW w AS (PARTITION BY SeriesHash, idx ORDER BY TimeBucket ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
		         ) GROUP BY SeriesHash, idx
		     ) GROUP BY SeriesHash
		 )
		 SELECT quantilePrometheusHistogram(0.95)(le, cum)
		 FROM (
		     SELECT le, sum(cumInc) AS cum
		     FROM (
		         SELECT tup.1 AS le, tup.2 AS cumInc
		         FROM (
		             SELECT r.increases AS increases, s.ExplicitBounds AS bounds
		             FROM per_series AS r
		             ANY INNER JOIN otel_int_test.%q AS s ON r.SeriesHash = s.SeriesHash
		         )
		         ARRAY JOIN arrayZip(arrayConcat(bounds, [toFloat64(inf)]), arrayCumSum(increases)) AS tup
		     ) GROUP BY le
		 )`, table5m, v2.SeriesTableName))
	assert.InDelta(t, 2.0, q95, 1e-9)

	// The 5m tier must cover every raw histogram point exactly once.
	require.Equal(t, scanUInt(t, e, fmt.Sprintf(
		`SELECT count() FROM otel_int_test.%q WHERE MetricName = 'test.duration'`, v2.HistogramPointsTableName)),
		scanUInt(t, e, fmt.Sprintf(
			`SELECT sum(PointCount) FROM otel_int_test.%q WHERE MetricName = 'test.duration'`, table5m)))
}

// mathTestRangeQueryShape is the Grafana range-query shape: one rate value per
// grid step, unfolded into (timestamp, value) rows.
func mathTestRangeQueryShape(t *testing.T, e *metricsV2Exporter) {
	v2 := &e.cfg.MetricsV2

	// rate(test_requests{code="200",service="svc-a"}[5m]) from 00:05 to 00:10
	// step 60s => 6 grid points. The counter increases 5 per 15s everywhere,
	// so every point must be exactly 1/3.
	ctx := clickhouse.Context(t.Context(), clickhouse.WithSettings(clickhouse.Settings{
		"allow_experimental_time_series_aggregate_functions": 1,
	}))
	rows, err := e.db.Query(ctx, fmt.Sprintf(
		`SELECT grid.1 AS ts, grid.2 AS value
		 FROM (
		     SELECT arrayZip(
		         timeSeriesRange(toDateTime('2024-01-01 00:05:00', 'UTC'), toDateTime('2024-01-01 00:10:00', 'UTC'), 60),
		         timeSeriesRateToGrid(toDateTime('2024-01-01 00:05:00', 'UTC'), toDateTime('2024-01-01 00:10:00', 'UTC'), 60, 300)(TimeUnix, Value)
		     ) AS zipped
		     FROM otel_int_test.%q
		     WHERE MetricName = 'test.requests'
		       AND TimeUnix > toDateTime('2024-01-01 00:00:00', 'UTC') AND TimeUnix <= toDateTime('2024-01-01 00:10:00', 'UTC')
		       AND SeriesHash IN (
		         SELECT SeriesHash FROM otel_int_test.%q
		         WHERE MetricName = 'test.requests' AND ServiceName = 'svc-a' AND Attributes['code'] = '200' AND Date = toDate('2024-01-01'))
		 )
		 ARRAY JOIN zipped AS grid
		 ORDER BY ts`, v2.PointsTableName, v2.SeriesTableName))
	require.NoError(t, err)

	var timestamps []time.Time
	var values []float64
	for rows.Next() {
		var ts time.Time
		var v *float64
		require.NoError(t, rows.Scan(&ts, &v))
		require.NotNil(t, v)
		timestamps = append(timestamps, ts.UTC())
		values = append(values, *v)
	}
	require.NoError(t, rows.Close())

	require.Len(t, values, 6)
	for i, v := range values {
		assert.InDelta(t, 1.0/3.0, v, 1e-6, "grid point %d", i)
		assert.Equal(t, mathT0.Add(5*time.Minute).Add(time.Duration(i)*time.Minute), timestamps[i])
	}
}

// mathTestPromQLFunctionFamily exercises the remaining PromQL range-function
// analogs: irate, idelta, changes, resets, deriv, predict_linear.
func mathTestPromQLFunctionFamily(t *testing.T, e *metricsV2Exporter) {
	v2 := &e.cfg.MetricsV2

	gridFn := func(fn, params, metric, service, attrKey, attrVal string, window int) string {
		return fmt.Sprintf(
			`SELECT (%s(toDateTime('2024-01-01 00:10:00', 'UTC'), toDateTime('2024-01-01 00:10:00', 'UTC'), %d, %d%s)(TimeUnix, Value))[1]
			 FROM otel_int_test.%q
			 WHERE MetricName = '%s'
			   AND TimeUnix > subtractSeconds(toDateTime('2024-01-01 00:10:00', 'UTC'), %d) AND TimeUnix <= toDateTime('2024-01-01 00:10:00', 'UTC')
			   AND SeriesHash IN (
			     SELECT SeriesHash FROM otel_int_test.%q
			     WHERE MetricName = '%s' AND ServiceName = '%s' AND Attributes['%s'] = '%s' AND Date = toDate('2024-01-01'))`,
			fn, window, window, params, v2.PointsTableName, metric, window, v2.SeriesTableName, metric, service, attrKey, attrVal)
	}

	// irate: last two samples of the clean counter: (195-190)/15s = 1/3.
	assert.InDelta(t, 1.0/3.0,
		scanFloat(t, e, gridFn("timeSeriesInstantRateToGrid", "", "test.requests", "svc-a", "code", "200", 300)), 1e-6)

	// idelta: last two samples differ by 5.
	assert.InDelta(t, 5.0,
		scanFloat(t, e, gridFn("timeSeriesInstantDeltaToGrid", "", "test.requests", "svc-a", "code", "200", 300)), 1e-6)

	// changes over (0,600]: samples i=1..39 (t=0 is excluded by the left-open
	// window) => 38 value changes on the strictly increasing gauge.
	assert.InDelta(t, 38.0,
		scanFloat(t, e, gridFn("timeSeriesChangesToGrid", "", "test.cpu.utilization", "svc-a", "core", "0", 600)), 1e-6)

	// resets over (0,600] on the resetting counter: exactly one reset.
	assert.InDelta(t, 1.0,
		scanFloat(t, e, gridFn("timeSeriesResetsToGrid", "", "test.requests", "svc-a", "code", "500", 600)), 1e-6)

	// deriv: least-squares slope of the linear gauge = 1 per 15s.
	//
	// FINDING (verified on 26.6.1): timeSeriesDerivToGrid returns the slope
	// per TIMESTAMP TICK, not per second — with DateTime64(3) input the
	// result is 1000x smaller (per millisecond). rate/delta DO normalize to
	// per-second, and predict_linear scales its offset by the same tick unit
	// so it stays self-consistent. Workaround: cast to second precision.
	// Worth reporting upstream (comp-promql) as a unit inconsistency.
	derivPerTick := scanFloat(t, e, gridFn("timeSeriesDerivToGrid", "", "test.cpu.utilization", "svc-a", "core", "0", 300))
	assert.InDelta(t, 1.0/15.0/1000.0, derivPerTick, 1e-9, "deriv over DateTime64(3) is per-millisecond")
	derivPerSecond := scanFloat(t, e, fmt.Sprintf(
		`SELECT (timeSeriesDerivToGrid(toDateTime('2024-01-01 00:10:00', 'UTC'), toDateTime('2024-01-01 00:10:00', 'UTC'), 300, 300)(toDateTime(TimeUnix), Value))[1]
		 FROM otel_int_test.%q
		 WHERE MetricName = 'test.cpu.utilization'
		   AND TimeUnix > toDateTime('2024-01-01 00:05:00', 'UTC') AND TimeUnix <= toDateTime('2024-01-01 00:10:00', 'UTC')
		   AND SeriesHash IN (
		     SELECT SeriesHash FROM otel_int_test.%q
		     WHERE MetricName = 'test.cpu.utilization' AND ServiceName = 'svc-a' AND Attributes['core'] = '0' AND Date = toDate('2024-01-01'))`,
		v2.PointsTableName, v2.SeriesTableName))
	assert.InDelta(t, 1.0/15.0, derivPerSecond, 1e-6)

	// predict_linear 60s ahead: v(600s)=50, slope 1/15 => 50 + 60/15 = 54.
	assert.InDelta(t, 54.0,
		scanFloat(t, e, gridFn("timeSeriesPredictLinearToGrid", ", 60", "test.cpu.utilization", "svc-a", "core", "0", 300)), 1e-6)
}

// mathTestFiltersAndMatchers covers the PromQL label-matcher surface on the
// series table: =, !=, =~, !~, and key-existence.
func mathTestFiltersAndMatchers(t *testing.T, e *metricsV2Exporter) {
	v2 := &e.cfg.MetricsV2

	seriesCount := func(where string) uint64 {
		return scanUInt(t, e, fmt.Sprintf(
			`SELECT count() FROM otel_int_test.%q WHERE %s`, v2.SeriesTableName, where))
	}

	// {code="200"} equality.
	require.Equal(t, uint64(2), seriesCount(`MetricName = 'test.requests' AND Attributes['code'] = '200'`))

	// {code=~"^2.."} regex matcher.
	require.Equal(t, uint64(2), seriesCount(`MetricName = 'test.requests' AND match(Attributes['code'], '^2..$')`))

	// {code!="500"} negative matcher. PromQL != also matches series without
	// the label; restricting to series that have it needs mapContains.
	require.Equal(t, uint64(2), seriesCount(`MetricName = 'test.requests' AND mapContains(Attributes, 'code') AND Attributes['code'] != '500'`))

	// {code!~"^2.."} negative regex.
	require.Equal(t, uint64(1), seriesCount(`MetricName = 'test.requests' AND NOT match(Attributes['code'], '^2..$')`))

	// {core=~".+"} key existence.
	require.Equal(t, uint64(3), seriesCount(`MetricName = 'test.cpu.utilization' AND mapContains(Attributes, 'core')`))

	// Combined resource-level + point-level matchers, resolving to points:
	// svc-a AND code=~2xx => 1 series x 40 points.
	require.Equal(t, uint64(40), scanUInt(t, e, fmt.Sprintf(
		`SELECT count() FROM otel_int_test.%q
		 WHERE MetricName = 'test.requests' AND SeriesHash IN (
		     SELECT SeriesHash FROM otel_int_test.%q
		     WHERE MetricName = 'test.requests' AND ServiceName = 'svc-a' AND match(Attributes['code'], '^2..$') AND Date = toDate('2024-01-01'))`,
		v2.PointsTableName, v2.SeriesTableName)))
}

// mathTestTopK is PromQL topk(1, rate(test_requests[5m])) — per-series rates
// ordered and limited, labels rehydrated from the series table.
func mathTestTopK(t *testing.T, e *metricsV2Exporter) {
	v2 := &e.cfg.MetricsV2

	ctx := clickhouse.Context(t.Context(), clickhouse.WithSettings(clickhouse.Settings{
		"allow_experimental_time_series_aggregate_functions": 1,
	}))
	row := e.db.QueryRow(ctx, fmt.Sprintf(
		`SELECT s.ServiceName, s.Attributes['code'], r.rate
		 FROM (
		     SELECT SeriesHash,
		            (timeSeriesRateToGrid(toDateTime('2024-01-01 00:10:00', 'UTC'), toDateTime('2024-01-01 00:10:00', 'UTC'), 300, 300)(TimeUnix, Value))[1] AS rate
		     FROM otel_int_test.%q
		     WHERE MetricName = 'test.requests'
		       AND TimeUnix > toDateTime('2024-01-01 00:05:00', 'UTC') AND TimeUnix <= toDateTime('2024-01-01 00:10:00', 'UTC')
		     GROUP BY SeriesHash
		     ORDER BY rate DESC
		     LIMIT 1
		 ) AS r
		 ANY INNER JOIN otel_int_test.%q AS s ON r.SeriesHash = s.SeriesHash`,
		v2.PointsTableName, v2.SeriesTableName))
	require.NoError(t, row.Err())

	var service, code string
	var rate float64
	require.NoError(t, row.Scan(&service, &code, &rate))
	assert.Equal(t, "svc-b", service)
	assert.Equal(t, "200", code)
	assert.InDelta(t, 7.0/15.0, rate, 1e-6)
}

// mathTestWindowedHistogramQuantile is the real dashboard shape:
// histogram_quantile(0.95, rate(test_duration_bucket[5m])). Bucket increases
// over the window come from element-wise argMax-argMin on the cumulative
// bucket arrays; PromQL's rate extrapolation constant cancels inside
// histogram_quantile, so increases give the identical quantile.
func mathTestWindowedHistogramQuantile(t *testing.T, e *metricsV2Exporter) {
	v2 := &e.cfg.MetricsV2

	// Window (00:05, 00:10]: per-series increases are 18x the per-scrape
	// bucket deltas: /api 18x[2 3 4 1 0] -> cum [36 90 162 180 180];
	// /web 18x[5 0 0 0 0] -> cum [90 90 90 90 90]. Summed per le:
	// [126 180 252 270 270], total 270. q95: rank 256.5 -> bucket (1,5]:
	// 1 + 4*(256.5-252)/18 = 2.0. Increases MUST be computed per series
	// (argMax/argMin within one series' arrays) before summing per le —
	// aggregating arrays across series picks arbitrary rows and is wrong.
	q95 := scanFloat(t, e, fmt.Sprintf(
		`WITH per_series AS (
		     SELECT s.ExplicitBounds AS bounds, r.increases
		     FROM (
		         SELECT SeriesHash,
		                arrayMap((l, f) -> l - f, argMax(BucketCounts, TimeUnix), argMin(BucketCounts, TimeUnix)) AS increases
		         FROM otel_int_test.%q
		         WHERE MetricName = 'test.duration'
		           AND TimeUnix > toDateTime64('2024-01-01 00:05:00', 3, 'UTC')
		           AND TimeUnix <= toDateTime64('2024-01-01 00:10:00', 3, 'UTC')
		         GROUP BY SeriesHash
		     ) AS r
		     ANY INNER JOIN otel_int_test.%q AS s ON r.SeriesHash = s.SeriesHash
		 )
		 SELECT quantilePrometheusHistogram(0.95)(le, cum)
		 FROM (
		     SELECT le, sum(cumInc) AS cum
		     FROM (
		         SELECT tup.1 AS le, toFloat64(tup.2) AS cumInc
		         FROM per_series
		         ARRAY JOIN arrayZip(arrayConcat(bounds, [toFloat64(inf)]), arrayCumSum(increases)) AS tup
		     )
		     GROUP BY le
		 )`, v2.HistogramPointsTableName, v2.SeriesTableName))
	assert.InDelta(t, 2.0, q95, 1e-9)
}

// mathTestQuirkFixViews prototypes the ergonomics fixes: a finalize view that
// hides the argMaxMerge/-Merge ceremony on rollups, and a parameterized view
// that hides the histogram_quantile ARRAY JOIN ceremony.
func mathTestQuirkFixViews(t *testing.T, e *metricsV2Exporter) {
	v2 := &e.cfg.MetricsV2

	// Finalize view over the 5m rollup: plain SELECTs, no combinators.
	require.NoError(t, e.db.Exec(t.Context(), fmt.Sprintf(
		`CREATE OR REPLACE VIEW otel_int_test.metrics_points_5m_final AS
		 SELECT MetricName, SeriesHash, TimeBucket,
		        argMaxMerge(Last) AS Last, min(Min) AS Min, max(Max) AS Max,
		        sum(Sum) AS Sum, sum(Count) AS Count
		 FROM otel_int_test.%q
		 GROUP BY MetricName, SeriesHash, TimeBucket`, v2.PointsTableName+"_5m")))

	var last, sum float64
	var count uint64
	row := e.db.QueryRow(t.Context(), fmt.Sprintf(
		`SELECT Last, Sum, Count FROM otel_int_test.metrics_points_5m_final
		 WHERE MetricName = 'test.cpu.utilization' AND SeriesHash IN (
		     SELECT SeriesHash FROM otel_int_test.%q
		     WHERE MetricName = 'test.cpu.utilization' AND ServiceName = 'svc-a' AND Attributes['core'] = '0' AND Date = toDate('2024-01-01'))
		 ORDER BY TimeBucket LIMIT 1`, v2.SeriesTableName))
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&last, &sum, &count))
	assert.InDelta(t, 29, last, 1e-9)
	assert.InDelta(t, 390, sum, 1e-9)
	assert.Equal(t, uint64(20), count)

	// Parameterized view: histogram_quantile as a one-liner for callers.
	// Latest cumulative counts are taken per series, then summed per le
	// (correct for any number of series under the metric).
	require.NoError(t, e.db.Exec(t.Context(), fmt.Sprintf(
		`CREATE OR REPLACE VIEW otel_int_test.otel_histogram_quantile AS
		 SELECT quantilePrometheusHistogram({phi:Float64})(le, cum) AS value
		 FROM (
		     SELECT le, sum(cumCount) AS cum
		     FROM (
		         SELECT tup.1 AS le, toFloat64(tup.2) AS cumCount
		         FROM (
		             SELECT s.ExplicitBounds AS bounds, r.counts
		             FROM (
		                 SELECT SeriesHash, argMax(BucketCounts, TimeUnix) AS counts
		                 FROM otel_int_test.%q
		                 WHERE MetricName = {metric:String}
		                 GROUP BY SeriesHash
		             ) AS r
		             ANY INNER JOIN otel_int_test.%q AS s ON r.SeriesHash = s.SeriesHash
		         )
		         ARRAY JOIN arrayZip(arrayConcat(bounds, [toFloat64(inf)]), arrayCumSum(counts)) AS tup
		     )
		     GROUP BY le
		 )`, v2.HistogramPointsTableName, v2.SeriesTableName)))

	// Combined latest counts: /api 40x[2 3 4 1 0] cum [80 200 360 400 400];
	// /web 40x[5 0 0 0 0] cum [200 200 200 200 200]; per-le sums
	// [280 400 560 600 600], total 600. q99: rank 594 -> bucket (1,5]:
	// 1 + 4*(594-560)/40 = 4.4.
	q99 := scanFloat(t, e,
		`SELECT value FROM otel_int_test.otel_histogram_quantile(phi=0.99, metric='test.duration')`)
	assert.InDelta(t, 4.4, q99, 1e-9)
}

// mathTestDeltaSums documents the Temporality branch: delta sums are NOT
// rate-function material — windowed increase is a plain SQL sum, and the
// rollup Sum column is exact for them. The query layer reads Temporality
// from the series table and branches.
func mathTestDeltaSums(t *testing.T, e *metricsV2Exporter) {
	v2 := &e.cfg.MetricsV2

	// The discriminator lives on the series table.
	var temporality string
	row := e.db.QueryRow(t.Context(), fmt.Sprintf(
		`SELECT Temporality FROM otel_int_test.%q WHERE MetricName = 'test.jobs' LIMIT 1`, v2.SeriesTableName))
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&temporality))
	require.Equal(t, "delta", temporality)

	// increase over (00:05, 00:10] = plain sum: 19 points x 2.
	assert.InDelta(t, 38.0, scanFloat(t, e, fmt.Sprintf(
		`SELECT sum(Value) FROM otel_int_test.%q
		 WHERE MetricName = 'test.jobs'
		   AND TimeUnix > toDateTime('2024-01-01 00:05:00', 'UTC') AND TimeUnix <= toDateTime('2024-01-01 00:10:00', 'UTC')
		   AND SeriesHash IN (
		     SELECT SeriesHash FROM otel_int_test.%q
		     WHERE MetricName = 'test.jobs' AND Attributes['queue'] = 'q1' AND Date = toDate('2024-01-01'))`,
		v2.PointsTableName, v2.SeriesTableName)), 1e-9)

	// The rollup Sum column is the exact long-range increase for delta sums:
	// 40 points x 2 across both 5m buckets.
	assert.InDelta(t, 80.0, scanFloat(t, e, fmt.Sprintf(
		`SELECT sum(Sum) FROM otel_int_test.%q WHERE MetricName = 'test.jobs'`,
		v2.PointsTableName+"_5m")), 1e-9)
}

// mathTestInsertScaleAndParts pushes a larger volume in many batches so the
// points table accumulates multiple parts, measures insert throughput through
// the full exporter path (including rollup MV fan-out), and validates exact
// aggregates at scale.
func mathTestInsertScaleAndParts(t *testing.T, e *metricsV2Exporter) {
	v2 := &e.cfg.MetricsV2

	const (
		seriesCount    = 1000
		scrapesPerPush = 25
		pushes         = 20
	)
	scaleT0 := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	payload := func(push int) pmetric.Metrics {
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service.name", "svc-scale")
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("mathtest")
		m := sm.Metrics().AppendEmpty()
		m.SetName("scale.gauge")
		m.SetUnit("1")
		g := m.SetEmptyGauge()
		for k := 0; k < scrapesPerPush; k++ {
			tIdx := push*scrapesPerPush + k
			ts := pcommon.NewTimestampFromTime(scaleT0.Add(time.Duration(tIdx) * mathScrapeInterval))
			for i := 0; i < seriesCount; i++ {
				dp := g.DataPoints().AppendEmpty()
				dp.SetTimestamp(ts)
				dp.SetStartTimestamp(pcommon.NewTimestampFromTime(scaleT0))
				dp.SetDoubleValue(float64(i + tIdx))
				dp.Attributes().PutStr("idx", fmt.Sprintf("series-%d", i))
			}
		}
		return md
	}

	var firstPush, restTotal time.Duration
	totalStart := time.Now()
	for p := 0; p < pushes; p++ {
		start := time.Now()
		require.NoError(t, e.pushMetricsData(t.Context(), payload(p)))
		if p == 0 {
			firstPush = time.Since(start)
		} else {
			restTotal += time.Since(start)
		}
	}
	total := time.Since(totalStart)

	const totalPoints = seriesCount * scrapesPerPush * pushes
	t.Logf("inserted %d points in %v => %.0f points/s end-to-end (incl. 5m/1h rollup MVs)",
		totalPoints, total, float64(totalPoints)/total.Seconds())
	t.Logf("first push (writes %d series rows): %v; subsequent pushes avg (0 series rows): %v",
		seriesCount, firstPush, restTotal/time.Duration(pushes-1))

	// Multiple inserts must have produced multiple active parts (background
	// merges may have combined some already).
	activeParts := scanUInt(t, e, fmt.Sprintf(
		`SELECT count() FROM system.parts WHERE database = 'otel_int_test' AND table = '%s' AND active`,
		v2.PointsTableName))
	t.Logf("active parts on points table before OPTIMIZE FINAL: %d", activeParts)
	require.GreaterOrEqual(t, activeParts, uint64(2), "expected multiple parts from %d separate inserts", pushes)

	require.NoError(t, e.db.Exec(t.Context(),
		fmt.Sprintf("OPTIMIZE TABLE otel_int_test.%q FINAL", v2.PointsTableName)))

	// Exact math at scale, across parts and after the merge:
	// sum over i in [0,1000) and tIdx in [0,500) of (i+tIdx)
	//   = 500*sum(i) + 1000*sum(tIdx) = 500*499500 + 1000*124750 = 374,500,000.
	require.Equal(t, uint64(totalPoints), scanUInt(t, e, fmt.Sprintf(
		`SELECT count() FROM otel_int_test.%q WHERE MetricName = 'scale.gauge'`, v2.PointsTableName)))
	assert.InDelta(t, 374500000.0, scanFloat(t, e, fmt.Sprintf(
		`SELECT sum(Value) FROM otel_int_test.%q WHERE MetricName = 'scale.gauge'`, v2.PointsTableName)), 1e-3)

	// Exactly one series row per series: the cache prevented re-writes on
	// pushes 2..N even before any merge.
	require.Equal(t, uint64(seriesCount), scanUInt(t, e, fmt.Sprintf(
		`SELECT count() FROM otel_int_test.%q WHERE MetricName = 'scale.gauge'`, v2.SeriesTableName)))

	// Rollup MV cascade stayed consistent at scale: 5m bucket counts cover
	// every point.
	require.Equal(t, uint64(totalPoints), scanUInt(t, e, fmt.Sprintf(
		`SELECT sum(Count) FROM otel_int_test.%q WHERE MetricName = 'scale.gauge'`, v2.PointsTableName+"_5m")))
}

// mathTestAdvancedPromQL covers the "slightly advanced" PromQL tier: vector
// binary operations (the error-ratio shape with on(...) matching semantics),
// subqueries, offset, and absent(). The load-bearing property throughout:
// joins only ever happen AFTER points are aggregated down to per-series (or
// per-group) granularity, so both join sides are series-cardinality, never
// point-cardinality.
func mathTestAdvancedPromQL(t *testing.T, e *metricsV2Exporter) {
	v2 := &e.cfg.MetricsV2

	// PromQL:
	//   sum by (service) (rate(test_requests{code="500"}[5m]))
	// / sum by (service) (rate(test_requests{code="200"}[5m]))
	// Two per-series grid aggregates, each reduced to per-service sums, then
	// joined on the group key. svc-a: (1/3)/(1/3) = 1.0.
	perServiceRate := func(alias, code string) string {
		return fmt.Sprintf(
			`(SELECT s.ServiceName AS service, sum(r.rate_arr[1]) AS rate
			  FROM (
			      SELECT SeriesHash,
			             timeSeriesRateToGrid(toDateTime('2024-01-01 00:10:00', 'UTC'), toDateTime('2024-01-01 00:10:00', 'UTC'), 300, 300)(TimeUnix, Value) AS rate_arr
			      FROM otel_int_test.%q
			      WHERE MetricName = 'test.requests'
			        AND TimeUnix > toDateTime('2024-01-01 00:05:00', 'UTC') AND TimeUnix <= toDateTime('2024-01-01 00:10:00', 'UTC')
			        AND SeriesHash IN (
			          SELECT SeriesHash FROM otel_int_test.%q
			          WHERE MetricName = 'test.requests' AND Attributes['code'] = '%s' AND Date = toDate('2024-01-01'))
			      GROUP BY SeriesHash
			  ) AS r
			  ANY INNER JOIN otel_int_test.%q AS s ON r.SeriesHash = s.SeriesHash
			  GROUP BY service) AS %s`,
			v2.PointsTableName, v2.SeriesTableName, code, v2.SeriesTableName, alias)
	}
	ratio := scanFloat(t, e, fmt.Sprintf(
		`SELECT errors.rate / total.rate
		 FROM %s
		 INNER JOIN %s ON errors.service = total.service
		 WHERE errors.service = 'svc-a'`,
		perServiceRate("errors", "500"), perServiceRate("total", "200")))
	assert.InDelta(t, 1.0, ratio, 1e-6)

	// PromQL subquery: max_over_time(rate(test_requests{code="200",service="svc-b"}[5m])[5m:1m])
	// = the max over a 6-point rate grid. The rate is constant 7/15.
	maxOverTime := scanFloat(t, e, fmt.Sprintf(
		`SELECT arrayMax(arrayMap(x -> assumeNotNull(x),
		        timeSeriesRateToGrid(toDateTime('2024-01-01 00:05:00', 'UTC'), toDateTime('2024-01-01 00:10:00', 'UTC'), 60, 300)(TimeUnix, Value)))
		 FROM otel_int_test.%q
		 WHERE MetricName = 'test.requests'
		   AND TimeUnix > toDateTime('2024-01-01 00:00:00', 'UTC') AND TimeUnix <= toDateTime('2024-01-01 00:10:00', 'UTC')
		   AND SeriesHash IN (
		     SELECT SeriesHash FROM otel_int_test.%q
		     WHERE MetricName = 'test.requests' AND ServiceName = 'svc-b' AND Attributes['code'] = '200' AND Date = toDate('2024-01-01'))`,
		v2.PointsTableName, v2.SeriesTableName))
	assert.InDelta(t, 7.0/15.0, maxOverTime, 1e-6)

	// PromQL offset: last_over_time(test_cpu_utilization[1m] offset 2m)
	// evaluated at 00:10 => evaluation shifts to 00:08; last sample at or
	// before 00:08 is i=32 (t=480s), v = 10+32 = 42.
	offsetLast := scanFloat(t, e, fmt.Sprintf(
		`SELECT (timeSeriesResampleToGridWithStaleness(toDateTime('2024-01-01 00:08:00', 'UTC'), toDateTime('2024-01-01 00:08:00', 'UTC'), 60, 60)(TimeUnix, Value))[1]
		 FROM otel_int_test.%q
		 WHERE MetricName = 'test.cpu.utilization'
		   AND TimeUnix > toDateTime('2024-01-01 00:07:00', 'UTC') AND TimeUnix <= toDateTime('2024-01-01 00:08:00', 'UTC')
		   AND SeriesHash IN (
		     SELECT SeriesHash FROM otel_int_test.%q
		     WHERE MetricName = 'test.cpu.utilization' AND ServiceName = 'svc-a' AND Attributes['core'] = '0' AND Date = toDate('2024-01-01'))`,
		v2.PointsTableName, v2.SeriesTableName))
	assert.InDelta(t, 42.0, offsetLast, 1e-9)

	// PromQL absent(): 1 when no series matched, empty otherwise.
	require.Equal(t, uint64(1), scanUInt(t, e, fmt.Sprintf(
		`SELECT toUInt64(if(count() = 0, 1, 0)) FROM otel_int_test.%q WHERE MetricName = 'no.such.metric'`,
		v2.SeriesTableName)))
	require.Equal(t, uint64(0), scanUInt(t, e, fmt.Sprintf(
		`SELECT toUInt64(if(count() = 0, 1, 0)) FROM otel_int_test.%q WHERE MetricName = 'test.requests'`,
		v2.SeriesTableName)))
}

// mathTestStalenessMarkers validates OTLP NoRecordedValue staleness markers
// (DataPointFlags bit 0 — the OTel form of Prometheus staleness markers; the
// prometheusreceiver emits them routinely when scrape targets disappear). The
// contract has three parts:
//
//  1. Raw points tables store marker rows untouched (Flags=1, zeroed values):
//     full fidelity, no write-path filtering.
//  2. The 5m rollup MVs exclude markers (WHERE bitAnd(Flags, 1) = 0): an
//     aggregated state can never be retro-filtered, so a marker folded into
//     Min/Count/ValueSketch/argMax states would corrupt the tier permanently.
//     The 1h tier cascades from the already-clean 5m TABLES, so it needs no
//     filter of its own.
//  3. The query layer applies bitAnd(Flags, 1) = 0 on every raw-points value
//     aggregation (rule §5.0-6): a marker's Value is a meaningless 0 that
//     otherwise reads as a false gauge zero, or as a counter reset that makes
//     reset correction add the entire previous counter value.
//
// Data (staleT0 = 2024-01-03, 15s scrapes, own metric names — independent of
// the main fixture):
//
//	sum   stale.requests (cumulative, monotonic) svc-a{code=200}
//	      v=5i for i=0..39, except i=25 which is a marker (mid-stream gap)
//	gauge stale.gauge svc-a{core=0}
//	      v=10+i for i=0..9, marker at i=10, nothing after (target vanished)
//	hist  stale.duration (cumulative) svc-a{route=/x}, bounds [0.1 1]
//	      point i: buckets [2n 3n n] (n=i+1), count 6n, sum 3n for i=0..18;
//	      marker at i=19 (trailing marker attacks the argMax Last* states)
//
// The payload is pushed 5 times concurrently, so this doubles as the
// retry-dedup regression for marker-containing batches (deterministic
// insert_deduplication_token + MV-extended dedup must still collapse them).
func mathTestStalenessMarkers(t *testing.T, e *metricsV2Exporter) {
	v2 := &e.cfg.MetricsV2
	staleT0 := time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC)
	ts := func(i int) pcommon.Timestamp {
		return pcommon.NewTimestampFromTime(staleT0.Add(time.Duration(i) * mathScrapeInterval))
	}
	start := pcommon.NewTimestampFromTime(staleT0)
	noValue := pmetric.DefaultDataPointFlags.WithNoRecordedValue(true)

	payload := func() pmetric.Metrics {
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service.name", "svc-a")
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("mathtest")
		sm.Scope().SetVersion("1.0")

		// Cumulative sum with a mid-stream marker at i=25.
		{
			m := sm.Metrics().AppendEmpty()
			m.SetName("stale.requests")
			m.SetUnit("{requests}")
			s := m.SetEmptySum()
			s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			s.SetIsMonotonic(true)
			for i := 0; i < 40; i++ {
				dp := s.DataPoints().AppendEmpty()
				dp.SetTimestamp(ts(i))
				dp.SetStartTimestamp(start)
				dp.Attributes().PutStr("code", "200")
				if i == 25 {
					dp.SetFlags(noValue)
					dp.SetDoubleValue(0)
				} else {
					dp.SetDoubleValue(5 * float64(i))
				}
			}
		}

		// Gauge whose target disappears: marker is the final point.
		{
			m := sm.Metrics().AppendEmpty()
			m.SetName("stale.gauge")
			m.SetUnit("1")
			g := m.SetEmptyGauge()
			for i := 0; i <= 10; i++ {
				dp := g.DataPoints().AppendEmpty()
				dp.SetTimestamp(ts(i))
				dp.SetStartTimestamp(start)
				dp.Attributes().PutStr("core", "0")
				if i == 10 {
					dp.SetFlags(noValue)
					dp.SetDoubleValue(0)
				} else {
					dp.SetDoubleValue(10 + float64(i))
				}
			}
		}

		// Cumulative histogram with a trailing marker. The marker carries the
		// series' ExplicitBounds (bounds are series identity — a boundless
		// marker would hash to a different series) with zeroed Count/Sum and
		// an all-zero BucketCounts array.
		{
			m := sm.Metrics().AppendEmpty()
			m.SetName("stale.duration")
			m.SetUnit("s")
			h := m.SetEmptyHistogram()
			h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			for i := 0; i <= 19; i++ {
				dp := h.DataPoints().AppendEmpty()
				dp.SetTimestamp(ts(i))
				dp.SetStartTimestamp(start)
				dp.ExplicitBounds().FromRaw([]float64{0.1, 1})
				dp.Attributes().PutStr("route", "/x")
				if i == 19 {
					dp.SetFlags(noValue)
					dp.BucketCounts().FromRaw([]uint64{0, 0, 0})
					dp.SetCount(0)
					dp.SetSum(0)
				} else {
					n := uint64(i + 1)
					dp.BucketCounts().FromRaw([]uint64{2 * n, 3 * n, n})
					dp.SetCount(6 * n)
					dp.SetSum(3 * float64(n))
					dp.SetMin(0.05)
					dp.SetMax(2.5)
				}
			}
		}

		return md
	}

	// 5 byte-identical concurrent pushes = the exporterhelper retry shape.
	// Every count below asserts single-insert totals, extending the
	// retry-dedup regression to marker-containing payloads.
	md := payload()
	pushConcurrentlyNoError(t, func() error {
		return e.pushMetricsData(t.Context(), md)
	})

	for _, table := range []string{
		v2.PointsTableName, v2.HistogramPointsTableName,
		v2.PointsTableName + "_5m", v2.PointsTableName + "_1h",
		v2.HistogramPointsTableName + "_5m", v2.HistogramPointsTableName + "_1h",
	} {
		require.NoError(t, e.db.Exec(t.Context(), fmt.Sprintf("OPTIMIZE TABLE otel_int_test.%q FINAL", table)))
	}

	// --- Raw fidelity: marker rows land untouched, exactly once ---
	require.Equal(t, uint64(40), scanUInt(t, e, fmt.Sprintf(
		`SELECT count() FROM otel_int_test.%q WHERE MetricName = 'stale.requests'`, v2.PointsTableName)))
	require.Equal(t, uint64(11), scanUInt(t, e, fmt.Sprintf(
		`SELECT count() FROM otel_int_test.%q WHERE MetricName = 'stale.gauge'`, v2.PointsTableName)))
	require.Equal(t, uint64(20), scanUInt(t, e, fmt.Sprintf(
		`SELECT count() FROM otel_int_test.%q WHERE MetricName = 'stale.duration'`, v2.HistogramPointsTableName)))

	// Exactly one marker row per series, with Flags=1 and Value 0.
	require.Equal(t, uint64(1), scanUInt(t, e, fmt.Sprintf(
		`SELECT count() FROM otel_int_test.%q WHERE MetricName = 'stale.requests' AND bitAnd(Flags, 1) = 1 AND Value = 0
		   AND TimeUnix = toDateTime64('2024-01-03 00:06:15', 3, 'UTC')`, v2.PointsTableName)))
	require.Equal(t, uint64(1), scanUInt(t, e, fmt.Sprintf(
		`SELECT count() FROM otel_int_test.%q WHERE MetricName = 'stale.gauge' AND bitAnd(Flags, 1) = 1 AND Value = 0`,
		v2.PointsTableName)))
	require.Equal(t, uint64(1), scanUInt(t, e, fmt.Sprintf(
		`SELECT count() FROM otel_int_test.%q WHERE MetricName = 'stale.duration' AND bitAnd(Flags, 1) = 1
		   AND Count = 0 AND Sum = 0 AND BucketCounts = [0, 0, 0]`, v2.HistogramPointsTableName)))

	// --- Gauge last-value: the canonical query-layer rule (§5.0-6) ---
	// Without the filter the marker's meaningless 0 wins argMax (false zero);
	// with bitAnd(Flags, 1) = 0 the pre-marker value 19 comes back.
	gaugeSeries := fmt.Sprintf(
		`SeriesHash IN (SELECT SeriesHash FROM otel_int_test.%q
		 WHERE MetricName = 'stale.gauge' AND ServiceName = 'svc-a' AND Attributes['core'] = '0' AND Date = toDate('2024-01-03'))`,
		v2.SeriesTableName)
	assert.InDelta(t, 0.0, scanFloat(t, e, fmt.Sprintf(
		`SELECT argMax(Value, TimeUnix) FROM otel_int_test.%q
		 WHERE MetricName = 'stale.gauge' AND %s`, v2.PointsTableName, gaugeSeries)), 1e-9,
		"unfiltered last-value reads the marker's false zero (this is WHY the rule exists)")
	assert.InDelta(t, 19.0, scanFloat(t, e, fmt.Sprintf(
		`SELECT argMax(Value, TimeUnix) FROM otel_int_test.%q
		 WHERE MetricName = 'stale.gauge' AND bitAnd(Flags, 1) = 0 AND %s`, v2.PointsTableName, gaugeSeries)), 1e-9)

	// --- Counter rate: marker mid-window fakes a reset without the filter ---
	// Window (00:05, 00:10] = i 21..39, marker at i=25. Filtered: clean +5/15s
	// => delta 90 over 270s sampled, extrapolated to 300s => 1/3. Unfiltered:
	// the 0 at i=25 looks like a reset, correction adds v(24)=120 =>
	// (195-105)+120 = 210 over 270s => 7/9 — a large silent overcount.
	rateQ := func(filter string) string {
		return fmt.Sprintf(
			`SELECT (timeSeriesRateToGrid(toDateTime('2024-01-03 00:10:00', 'UTC'), toDateTime('2024-01-03 00:10:00', 'UTC'), 300, 300)(TimeUnix, Value))[1]
			 FROM otel_int_test.%q
			 WHERE MetricName = 'stale.requests' %s
			   AND TimeUnix > toDateTime('2024-01-03 00:05:00', 'UTC') AND TimeUnix <= toDateTime('2024-01-03 00:10:00', 'UTC')
			   AND SeriesHash IN (
			     SELECT SeriesHash FROM otel_int_test.%q
			     WHERE MetricName = 'stale.requests' AND Attributes['code'] = '200' AND Date = toDate('2024-01-03'))`,
			v2.PointsTableName, filter, v2.SeriesTableName)
	}
	assert.InDelta(t, 1.0/3.0, scanFloat(t, e, rateQ("AND bitAnd(Flags, 1) = 0")), 1e-6)
	assert.InDelta(t, 7.0/9.0, scanFloat(t, e, rateQ("")), 1e-6,
		"unfiltered rate reset-corrects the marker's 0 into a +120 overcount")

	// --- Float rollups: the marker bucket aggregates ONLY the real points ---
	// 5m bucket 0 = i 0..19 (clean): First 0, Last 95, Min 0, Max 95,
	// Sum 5*(0+..+19) = 950, Count 20.
	// 5m bucket 1 = i 20..39 minus the marker: 19 points, First 100, Last 195,
	// Min 100 (not the marker's 0), Max 195, Sum 5*(20+..+39 − 25) = 2825,
	// Count 19 (not 20).
	sumSeries := fmt.Sprintf(
		`SeriesHash IN (SELECT SeriesHash FROM otel_int_test.%q
		 WHERE MetricName = 'stale.requests' AND Attributes['code'] = '200' AND Date = toDate('2024-01-03'))`,
		v2.SeriesTableName)
	type bucket struct {
		first, last, min, max, sum float64
		count                      uint64
	}
	rows, err := e.db.Query(t.Context(), fmt.Sprintf(
		`SELECT argMinMerge(First), argMaxMerge(Last), min(Min), max(Max), sum(Sum), sum(Count)
		 FROM otel_int_test.%q WHERE MetricName = 'stale.requests' AND %s
		 GROUP BY TimeBucket ORDER BY TimeBucket`, v2.PointsTableName+"_5m", sumSeries))
	require.NoError(t, err)
	var buckets []bucket
	for rows.Next() {
		var b bucket
		require.NoError(t, rows.Scan(&b.first, &b.last, &b.min, &b.max, &b.sum, &b.count))
		buckets = append(buckets, b)
	}
	require.NoError(t, rows.Close())
	require.Len(t, buckets, 2)
	assert.Equal(t, bucket{first: 0, last: 95, min: 0, max: 95, sum: 950, count: 20}, buckets[0])
	assert.Equal(t, bucket{first: 100, last: 195, min: 100, max: 195, sum: 2825, count: 19}, buckets[1])

	// The ValueSketch state is marker-free too: median of bucket 1's 19 real
	// values (100..195 step 5, without 125) is 150; a folded-in 0 would drag
	// it to 145.
	assert.InDelta(t, 150.0, scanFloat(t, e, fmt.Sprintf(
		`SELECT quantileBFloat16Merge(0.5)(ValueSketch) FROM otel_int_test.%q
		 WHERE MetricName = 'stale.requests' AND %s AND TimeBucket = toDateTime('2024-01-03 00:05:00', 'UTC')`,
		v2.PointsTableName+"_5m", sumSeries)), 1.0)

	// 1h cascade (one bucket) stays clean: Count 39, Sum 3775, Min 0 from the
	// genuine i=0 point, chained Last 195.
	var last1h, min1h, sum1h float64
	var count1h uint64
	row := e.db.QueryRow(t.Context(), fmt.Sprintf(
		`SELECT argMaxMerge(Last), min(Min), sum(Sum), sum(Count) FROM otel_int_test.%q
		 WHERE MetricName = 'stale.requests' AND %s GROUP BY TimeBucket`, v2.PointsTableName+"_1h", sumSeries))
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&last1h, &min1h, &sum1h, &count1h))
	assert.InDelta(t, 195.0, last1h, 1e-9)
	assert.InDelta(t, 0.0, min1h, 1e-9)
	assert.InDelta(t, 3775.0, sum1h, 1e-9)
	assert.Equal(t, uint64(39), count1h)

	// The gauge's 5m bucket keeps the true last value 19, not the marker 0.
	var gLast, gMax, gSum float64
	var gCount uint64
	row = e.db.QueryRow(t.Context(), fmt.Sprintf(
		`SELECT argMaxMerge(Last), max(Max), sum(Sum), sum(Count) FROM otel_int_test.%q
		 WHERE MetricName = 'stale.gauge' AND %s GROUP BY TimeBucket`, v2.PointsTableName+"_5m", gaugeSeries))
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&gLast, &gMax, &gSum, &gCount))
	assert.InDelta(t, 19.0, gLast, 1e-9)
	assert.InDelta(t, 19.0, gMax, 1e-9)
	assert.InDelta(t, 145.0, gSum, 1e-9) // sum of 10..19
	assert.Equal(t, uint64(10), gCount)

	// --- Histogram rollups: trailing marker must not win the Last* states ---
	// Real points i=0..18 (n=1..19, sum n = 190) land in one 5m bucket with
	// the i=19 marker. Expected from ONLY the real points:
	// FirstBuckets [2 3 1], LastBuckets [38 57 19] (i=18; an unfiltered argMax
	// would return the marker's [0 0 0]), SumBuckets [380 570 190],
	// SumCount 6*190 = 1140, SumSum 3*190 = 570, Min 0.05 (not the marker's
	// unset 0), Max 2.5, PointCount 19 (not 20).
	histSeries := fmt.Sprintf(
		`SeriesHash IN (SELECT SeriesHash FROM otel_int_test.%q
		 WHERE MetricName = 'stale.duration' AND Attributes['route'] = '/x' AND Date = toDate('2024-01-03'))`,
		v2.SeriesTableName)
	for _, table := range []string{v2.HistogramPointsTableName + "_5m", v2.HistogramPointsTableName + "_1h"} {
		var firstBuckets, lastBuckets, sumBuckets []uint64
		var firstCount, lastCount, sumCount, pointCount uint64
		var firstSum, lastSum, sumSum, minVal, maxVal float64
		row = e.db.QueryRow(t.Context(), fmt.Sprintf(
			`SELECT argMinMerge(FirstBuckets), argMaxMerge(LastBuckets), sumForEachMerge(SumBuckets),
			        argMinMerge(FirstCount), argMaxMerge(LastCount), sum(SumCount),
			        argMinMerge(FirstSum), argMaxMerge(LastSum), sum(SumSum),
			        min(Min), max(Max), sum(PointCount)
			 FROM otel_int_test.%q WHERE MetricName = 'stale.duration' AND %s GROUP BY TimeBucket`, table, histSeries))
		require.NoError(t, row.Err())
		require.NoError(t, row.Scan(&firstBuckets, &lastBuckets, &sumBuckets,
			&firstCount, &lastCount, &sumCount, &firstSum, &lastSum, &sumSum,
			&minVal, &maxVal, &pointCount))
		assert.Equal(t, []uint64{2, 3, 1}, firstBuckets, "%s", table)
		assert.Equal(t, []uint64{38, 57, 19}, lastBuckets, "%s", table)
		assert.Equal(t, []uint64{380, 570, 190}, sumBuckets, "%s", table)
		assert.Equal(t, uint64(6), firstCount, "%s", table)
		assert.Equal(t, uint64(114), lastCount, "%s", table)
		assert.Equal(t, uint64(1140), sumCount, "%s", table)
		assert.InDelta(t, 3.0, firstSum, 1e-9, "%s", table)
		assert.InDelta(t, 57.0, lastSum, 1e-9, "%s", table)
		assert.InDelta(t, 570.0, sumSum, 1e-9, "%s", table)
		assert.InDelta(t, 0.05, minVal, 1e-9, "%s", table)
		assert.InDelta(t, 2.5, maxVal, 1e-9, "%s", table)
		assert.Equal(t, uint64(19), pointCount, "%s", table)
	}

	// Avg latency from the marker-free rollup scalars:
	// (57-3)/(114-6) = 54/108 = 0.5.
	assert.InDelta(t, 0.5, scanFloat(t, e, fmt.Sprintf(
		`SELECT (argMaxMerge(LastSum) - argMinMerge(FirstSum)) / (argMaxMerge(LastCount) - argMinMerge(FirstCount))
		 FROM otel_int_test.%q WHERE MetricName = 'stale.duration' AND %s`,
		v2.HistogramPointsTableName+"_5m", histSeries)), 1e-9)
}
