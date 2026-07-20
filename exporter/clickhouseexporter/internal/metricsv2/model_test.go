// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metricsv2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

var testTime = time.Unix(1703498029, 0).UTC()

func newTestBatch() *Batch {
	return NewBatch(NewSeriesCache(0, 0, nil), zap.NewNop())
}

func addAllTypes(t *testing.T, b *Batch, dpAttrOrder []string) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-svc")
	rm.Resource().Attributes().PutStr("host.name", "host-1")
	rm.SetSchemaUrl("res-url")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.SetSchemaUrl("scope-url")
	sm.Scope().SetName("scope")
	sm.Scope().SetVersion("1.0")

	ts := pcommon.NewTimestampFromTime(testTime)

	putAttrs := func(m pcommon.Map) {
		for _, k := range dpAttrOrder {
			m.PutStr(k, k+"-value")
		}
	}

	gauge := sm.Metrics().AppendEmpty()
	gauge.SetName("test.gauge")
	gauge.SetUnit("1")
	gauge.SetDescription("a gauge")
	g := gauge.SetEmptyGauge()
	for i := 0; i < 2; i++ {
		dp := g.DataPoints().AppendEmpty()
		dp.SetTimestamp(ts)
		dp.SetStartTimestamp(ts)
		dp.SetDoubleValue(float64(i))
		putAttrs(dp.Attributes())
		ex := dp.Exemplars().AppendEmpty()
		ex.SetTimestamp(ts)
		ex.SetDoubleValue(1.5)
		ex.FilteredAttributes().PutStr("exk", "exv")
	}

	sum := sm.Metrics().AppendEmpty()
	sum.SetName("test.sum")
	sum.SetUnit("1")
	s := sum.SetEmptySum()
	s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	s.SetIsMonotonic(true)
	for i := 0; i < 2; i++ {
		dp := s.DataPoints().AppendEmpty()
		dp.SetTimestamp(ts)
		dp.SetStartTimestamp(ts)
		dp.SetIntValue(int64(100 + i))
		putAttrs(dp.Attributes())
	}

	hist := sm.Metrics().AppendEmpty()
	hist.SetName("test.histogram")
	hist.SetUnit("s")
	h := hist.SetEmptyHistogram()
	h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	for i := 0; i < 2; i++ {
		dp := h.DataPoints().AppendEmpty()
		dp.SetTimestamp(ts)
		dp.SetStartTimestamp(ts)
		dp.SetCount(10)
		dp.SetSum(5.5)
		dp.ExplicitBounds().FromRaw([]float64{0.1, 0.5, 1})
		dp.BucketCounts().FromRaw([]uint64{1, 2, 3, 4})
		putAttrs(dp.Attributes())
	}

	expHist := sm.Metrics().AppendEmpty()
	expHist.SetName("test.exp_histogram")
	expHist.SetUnit("s")
	eh := expHist.SetEmptyExponentialHistogram()
	eh.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	for i := 0; i < 2; i++ {
		dp := eh.DataPoints().AppendEmpty()
		dp.SetTimestamp(ts)
		dp.SetStartTimestamp(ts)
		dp.SetCount(20)
		dp.SetSum(9.9)
		dp.SetScale(3)
		dp.SetZeroCount(1)
		dp.SetZeroThreshold(1e-12)
		dp.Positive().SetOffset(-2)
		dp.Positive().BucketCounts().FromRaw([]uint64{5, 6})
		dp.Negative().SetOffset(1)
		dp.Negative().BucketCounts().FromRaw([]uint64{7})
		putAttrs(dp.Attributes())
	}

	summary := sm.Metrics().AppendEmpty()
	summary.SetName("test.summary")
	summary.SetUnit("ms")
	sy := summary.SetEmptySummary()
	for i := 0; i < 2; i++ {
		dp := sy.DataPoints().AppendEmpty()
		dp.SetTimestamp(ts)
		dp.SetStartTimestamp(ts)
		dp.SetCount(4)
		dp.SetSum(2.2)
		// Quantiles deliberately out of order; conversion must sort them.
		q99 := dp.QuantileValues().AppendEmpty()
		q99.SetQuantile(0.99)
		q99.SetValue(3)
		q50 := dp.QuantileValues().AppendEmpty()
		q50.SetQuantile(0.5)
		q50.SetValue(1)
		putAttrs(dp.Attributes())
	}

	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		res := rms.At(i)
		b.SetResource(res.Resource().Attributes(), res.SchemaUrl())
		sms := res.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			scope := sms.At(j)
			b.SetScope(scope.Scope(), scope.SchemaUrl())
			ms := scope.Metrics()
			for k := 0; k < ms.Len(); k++ {
				require.NoError(t, b.AddMetric(ms.At(k)))
			}
		}
	}
}

func TestBatchConversionCounts(t *testing.T) {
	b := newTestBatch()
	addAllTypes(t, b, []string{"label_a", "label_b"})

	// Two data points per metric on the same series: one series row per metric.
	assert.Len(t, b.series, 5)
	assert.Len(t, b.floatPoints, 4, "gauge + sum points share the float points table")
	assert.Len(t, b.histPoints, 2)
	assert.Len(t, b.expHistPoints, 2)
	assert.Len(t, b.summaryPoints, 2)
	assert.Len(t, b.exemplars, 2)
	assert.Len(t, b.families, 5)

	day := int32(testTime.Unix() / secondsPerDay)
	assert.Len(t, b.newSeries[day], 5)
}

func TestBatchSeriesMetadata(t *testing.T) {
	b := newTestBatch()
	addAllTypes(t, b, []string{"label_a", "label_b"})

	byName := map[string]seriesRow{}
	for _, s := range b.series {
		byName[s.metricName] = s
	}

	assert.Equal(t, metricTypeGauge, byName["test.gauge"].metricType)
	assert.Equal(t, temporalityUnspecified, byName["test.gauge"].temporality)

	assert.Equal(t, metricTypeSum, byName["test.sum"].metricType)
	assert.Equal(t, temporalityCumulative, byName["test.sum"].temporality)
	assert.True(t, byName["test.sum"].isMonotonic)

	assert.Equal(t, []float64{0.1, 0.5, 1}, byName["test.histogram"].bounds)
	assert.Equal(t, temporalityDelta, byName["test.exp_histogram"].temporality)
	assert.Equal(t, []float64{0.5, 0.99}, byName["test.summary"].quantiles, "quantile levels must be sorted")

	for _, s := range b.series {
		assert.Equal(t, "test-svc", s.serviceName)
		assert.Equal(t, "scope", s.scopeName)
		assert.Equal(t, testTime.Truncate(24*time.Hour), s.date)
	}
}

func TestBatchHashStableAcrossAttrOrder(t *testing.T) {
	b1 := newTestBatch()
	addAllTypes(t, b1, []string{"label_a", "label_b"})
	b2 := newTestBatch()
	addAllTypes(t, b2, []string{"label_b", "label_a"})

	require.Len(t, b2.series, len(b1.series))
	hashes1 := map[string]uint64{}
	for _, s := range b1.series {
		hashes1[s.metricName] = s.hash
	}
	for _, s := range b2.series {
		assert.Equal(t, hashes1[s.metricName], s.hash,
			"series hash must not depend on attribute insertion order (metric %s)", s.metricName)
	}
}

func TestBatchSeriesCacheSkipsKnownSeries(t *testing.T) {
	cache := NewSeriesCache(0, 0, nil)
	b1 := NewBatch(cache, zap.NewNop())
	addAllTypes(t, b1, []string{"label_a", "label_b"})
	require.Len(t, b1.series, 5)

	// Simulate a successful series insert.
	for day, hashes := range b1.newSeries {
		cache.Add(day, hashes)
	}

	b2 := NewBatch(cache, zap.NewNop())
	addAllTypes(t, b2, []string{"label_a", "label_b"})
	assert.Empty(t, b2.series, "cached series must not produce new series rows")
	assert.Len(t, b2.floatPoints, 4, "points are always written")
}

func TestBatchSummaryValuesFollowSortedQuantiles(t *testing.T) {
	b := newTestBatch()
	addAllTypes(t, b, []string{"label_a"})

	require.Len(t, b.summaryPoints, 2)
	for _, p := range b.summaryPoints {
		assert.Equal(t, []float64{1, 3}, p.quantileValues,
			"values must be reordered to match sorted quantile levels")
	}
}
