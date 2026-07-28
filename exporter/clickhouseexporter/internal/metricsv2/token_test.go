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
)

// tokenTestBatch converts a payload built by fn (under a fixed resource and
// scope) into a fresh Batch, mirroring the exporter's push loop.
func tokenTestBatch(t *testing.T, fn func(sm pmetric.ScopeMetrics)) *Batch {
	t.Helper()
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "token-svc")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("scope")
	sm.Scope().SetVersion("1.0")
	fn(sm)

	b := newTestBatch()
	b.SetResource(rm.Resource().Attributes(), rm.SchemaUrl())
	b.SetScope(sm.Scope(), sm.SchemaUrl())
	ms := sm.Metrics()
	for k := 0; k < ms.Len(); k++ {
		require.NoError(t, b.AddMetric(ms.At(k)))
	}
	return b
}

func allTokens(b *Batch) map[string]string {
	return map[string]string{
		"points":         b.floatPointsToken(),
		"histogram":      b.histPointsToken(),
		"exp histogram":  b.expHistPointsToken(),
		"summary points": b.summaryPointsToken(),
		"exemplars":      b.exemplarsToken(),
	}
}

// TestDedupTokensDeterministicAcrossBatches: the same pdata content converted
// by two independent Batch instances (with attributes inserted in different
// orders) must produce identical tokens for every table.
func TestDedupTokensDeterministicAcrossBatches(t *testing.T) {
	b1 := newTestBatch()
	addAllTypes(t, b1, []string{"label_a", "label_b"})
	b2 := newTestBatch()
	addAllTypes(t, b2, []string{"label_b", "label_a"})

	tokens1 := allTokens(b1)
	tokens2 := allTokens(b2)
	for table, token := range tokens1 {
		require.NotEmpty(t, token, "table %s", table)
		assert.Equal(t, token, tokens2[table],
			"token for %s must not depend on the converting Batch instance or attribute insertion order", table)
	}
}

func TestDedupTokensEmptyBatch(t *testing.T) {
	b := newTestBatch()
	for table, token := range allTokens(b) {
		assert.Empty(t, token, "empty batch must produce an empty %s token (no dedup settings applied)", table)
	}
}

// TestDedupTokenGolden pins the exact token strings for a fixed batch. The
// token serialization must be stable across processes and releases: a retried
// insert after a collector restart (or from another replica built at another
// version) must still produce the identical token. If this test fails, the
// token derivation changed — treat it as a new token version and consider the
// dedup consequences for in-flight rollouts.
func TestDedupTokenGolden(t *testing.T) {
	b := newTestBatch()
	addAllTypes(t, b, []string{"label_a", "label_b"})

	assert.Equal(t, map[string]string{
		"points":         "otelv2-781eebe6464f7c5b",
		"histogram":      "otelv2-c46e6f19aec713bd",
		"exp histogram":  "otelv2-c5dd47c05615f0fc",
		"summary points": "otelv2-56c224dcab6762c0",
		"exemplars":      "otelv2-c34ba6388d39ea1f",
	}, allTokens(b))
}

// TestDedupTokenMiddleRowChangesToken is the regression test for the false
// positive of the old boundary-row token: two blocks sharing row count and
// identical first/last rows but differing in a middle row MUST get different
// tokens — a shared token would make ClickHouse silently drop the second
// block. Covered for every v2 table that carries a token.
func TestDedupTokenMiddleRowChangesToken(t *testing.T) {
	ts := func(i int) pcommon.Timestamp {
		return pcommon.NewTimestampFromTime(testTime.Add(time.Duration(i) * time.Second))
	}
	start := pcommon.NewTimestampFromTime(testTime)

	cases := []struct {
		name  string
		token func(*Batch) string
		build func(mutateMiddle bool) *Batch
	}{
		{
			name:  "points middle value",
			token: (*Batch).floatPointsToken,
			build: func(mutateMiddle bool) *Batch {
				return tokenTestBatch(t, func(sm pmetric.ScopeMetrics) {
					m := sm.Metrics().AppendEmpty()
					m.SetName("test.gauge")
					g := m.SetEmptyGauge()
					for i, v := range []float64{10, 20, 30} {
						if i == 1 && mutateMiddle {
							v = 99
						}
						dp := g.DataPoints().AppendEmpty()
						dp.SetTimestamp(ts(i))
						dp.SetStartTimestamp(start)
						dp.SetDoubleValue(v)
						dp.Attributes().PutStr("k", "v")
					}
				})
			},
		},
		{
			name:  "histogram middle sum",
			token: (*Batch).histPointsToken,
			build: func(mutateMiddle bool) *Batch {
				return tokenTestBatch(t, func(sm pmetric.ScopeMetrics) {
					m := sm.Metrics().AppendEmpty()
					m.SetName("test.hist")
					h := m.SetEmptyHistogram()
					h.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
					for i := 0; i < 3; i++ {
						dp := h.DataPoints().AppendEmpty()
						dp.SetTimestamp(ts(i))
						dp.SetStartTimestamp(start)
						dp.SetCount(10)
						dp.SetSum(5.5)
						if i == 1 && mutateMiddle {
							dp.SetSum(6.5)
						}
						dp.ExplicitBounds().FromRaw([]float64{1, 2})
						dp.BucketCounts().FromRaw([]uint64{3, 4, 3})
						dp.Attributes().PutStr("k", "v")
					}
				})
			},
		},
		{
			name:  "exp histogram middle zero count",
			token: (*Batch).expHistPointsToken,
			build: func(mutateMiddle bool) *Batch {
				return tokenTestBatch(t, func(sm pmetric.ScopeMetrics) {
					m := sm.Metrics().AppendEmpty()
					m.SetName("test.exphist")
					h := m.SetEmptyExponentialHistogram()
					h.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
					for i := 0; i < 3; i++ {
						dp := h.DataPoints().AppendEmpty()
						dp.SetTimestamp(ts(i))
						dp.SetStartTimestamp(start)
						dp.SetScale(2)
						dp.SetCount(6)
						dp.SetSum(9)
						dp.SetZeroCount(1)
						if i == 1 && mutateMiddle {
							dp.SetZeroCount(2)
						}
						dp.Positive().SetOffset(0)
						dp.Positive().BucketCounts().FromRaw([]uint64{2, 3})
						dp.Attributes().PutStr("k", "v")
					}
				})
			},
		},
		{
			name:  "summary middle sum",
			token: (*Batch).summaryPointsToken,
			build: func(mutateMiddle bool) *Batch {
				return tokenTestBatch(t, func(sm pmetric.ScopeMetrics) {
					m := sm.Metrics().AppendEmpty()
					m.SetName("test.summary")
					s := m.SetEmptySummary()
					for i := 0; i < 3; i++ {
						dp := s.DataPoints().AppendEmpty()
						dp.SetTimestamp(ts(i))
						dp.SetStartTimestamp(start)
						dp.SetCount(4)
						dp.SetSum(2.5)
						if i == 1 && mutateMiddle {
							dp.SetSum(3.5)
						}
						qv := dp.QuantileValues().AppendEmpty()
						qv.SetQuantile(0.5)
						qv.SetValue(1)
						dp.Attributes().PutStr("k", "v")
					}
				})
			},
		},
		{
			name:  "exemplars middle trace id",
			token: (*Batch).exemplarsToken,
			build: func(mutateMiddle bool) *Batch {
				return tokenTestBatch(t, func(sm pmetric.ScopeMetrics) {
					m := sm.Metrics().AppendEmpty()
					m.SetName("test.gauge")
					g := m.SetEmptyGauge()
					for i := 0; i < 3; i++ {
						dp := g.DataPoints().AppendEmpty()
						dp.SetTimestamp(ts(i))
						dp.SetStartTimestamp(start)
						dp.SetDoubleValue(1)
						dp.Attributes().PutStr("k", "v")
						ex := dp.Exemplars().AppendEmpty()
						ex.SetTimestamp(ts(i))
						ex.SetDoubleValue(1.5)
						traceID := pcommon.TraceID{1, 2, 3, 4}
						if i == 1 && mutateMiddle {
							traceID = pcommon.TraceID{9, 9, 9, 9}
						}
						ex.SetTraceID(traceID)
						ex.SetSpanID(pcommon.SpanID{5, 6})
						ex.FilteredAttributes().PutStr("exk", "exv")
					}
				})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := tc.token(tc.build(false))
			rebuilt := tc.token(tc.build(false))
			mutated := tc.token(tc.build(true))
			require.NotEmpty(t, base)
			assert.Equal(t, base, rebuilt, "identical content must produce identical tokens")
			assert.NotEqual(t, base, mutated,
				"a middle-row difference must change the token even when row count and boundary rows are identical")
		})
	}
}

// TestDedupTokenFieldSensitivity mutates individual row fields (most of which
// the old boundary token ignored even on the first/last rows) and requires a
// token change for each: the token must cover every identity-and-value
// relevant column that reaches ClickHouse.
func TestDedupTokenFieldSensitivity(t *testing.T) {
	ts := pcommon.NewTimestampFromTime(testTime)
	start := pcommon.NewTimestampFromTime(testTime.Add(-time.Minute))

	t.Run("points", func(t *testing.T) {
		build := func(mutate func(pmetric.NumberDataPoint)) string {
			b := tokenTestBatch(t, func(sm pmetric.ScopeMetrics) {
				m := sm.Metrics().AppendEmpty()
				m.SetName("test.gauge")
				dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
				dp.SetTimestamp(ts)
				dp.SetStartTimestamp(start)
				dp.SetDoubleValue(10)
				dp.Attributes().PutStr("k", "v")
				if mutate != nil {
					mutate(dp)
				}
			})
			return b.floatPointsToken()
		}
		base := build(nil)
		for name, mutate := range map[string]func(pmetric.NumberDataPoint){
			"value":      func(dp pmetric.NumberDataPoint) { dp.SetDoubleValue(11) },
			"timestamp":  func(dp pmetric.NumberDataPoint) { dp.SetTimestamp(ts + 1) },
			"start time": func(dp pmetric.NumberDataPoint) { dp.SetStartTimestamp(start + 1) },
			"flags":      func(dp pmetric.NumberDataPoint) { dp.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true)) },
			"series":     func(dp pmetric.NumberDataPoint) { dp.Attributes().PutStr("k", "other") },
		} {
			assert.NotEqual(t, base, build(mutate), "%s must affect the points token", name)
		}
	})

	t.Run("histogram", func(t *testing.T) {
		build := func(mutate func(pmetric.HistogramDataPoint)) string {
			b := tokenTestBatch(t, func(sm pmetric.ScopeMetrics) {
				m := sm.Metrics().AppendEmpty()
				m.SetName("test.hist")
				h := m.SetEmptyHistogram()
				h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				dp := h.DataPoints().AppendEmpty()
				dp.SetTimestamp(ts)
				dp.SetStartTimestamp(start)
				dp.SetCount(10)
				dp.SetSum(5.5)
				dp.SetMin(0.5)
				dp.SetMax(4)
				dp.ExplicitBounds().FromRaw([]float64{1, 2})
				dp.BucketCounts().FromRaw([]uint64{3, 4, 3})
				dp.Attributes().PutStr("k", "v")
				if mutate != nil {
					mutate(dp)
				}
			})
			return b.histPointsToken()
		}
		base := build(nil)
		for name, mutate := range map[string]func(pmetric.HistogramDataPoint){
			"count":        func(dp pmetric.HistogramDataPoint) { dp.SetCount(11) },
			"sum":          func(dp pmetric.HistogramDataPoint) { dp.SetSum(6.5) },
			"min":          func(dp pmetric.HistogramDataPoint) { dp.SetMin(0.25) },
			"max":          func(dp pmetric.HistogramDataPoint) { dp.SetMax(5) },
			"bucket count": func(dp pmetric.HistogramDataPoint) { dp.BucketCounts().SetAt(1, 5) },
			"start time":   func(dp pmetric.HistogramDataPoint) { dp.SetStartTimestamp(start + 1) },
			"flags": func(dp pmetric.HistogramDataPoint) {
				dp.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))
			},
		} {
			assert.NotEqual(t, base, build(mutate), "%s must affect the histogram token", name)
		}
	})

	t.Run("exp histogram", func(t *testing.T) {
		build := func(mutate func(pmetric.ExponentialHistogramDataPoint)) string {
			b := tokenTestBatch(t, func(sm pmetric.ScopeMetrics) {
				m := sm.Metrics().AppendEmpty()
				m.SetName("test.exphist")
				h := m.SetEmptyExponentialHistogram()
				h.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				dp := h.DataPoints().AppendEmpty()
				dp.SetTimestamp(ts)
				dp.SetStartTimestamp(start)
				dp.SetScale(2)
				dp.SetCount(13)
				dp.SetSum(9)
				dp.SetMin(0.5)
				dp.SetMax(4)
				dp.SetZeroCount(1)
				dp.SetZeroThreshold(1e-12)
				dp.Positive().SetOffset(0)
				dp.Positive().BucketCounts().FromRaw([]uint64{2, 3})
				dp.Negative().SetOffset(1)
				dp.Negative().BucketCounts().FromRaw([]uint64{7})
				dp.Attributes().PutStr("k", "v")
				if mutate != nil {
					mutate(dp)
				}
			})
			return b.expHistPointsToken()
		}
		base := build(nil)
		for name, mutate := range map[string]func(pmetric.ExponentialHistogramDataPoint){
			"sum":             func(dp pmetric.ExponentialHistogramDataPoint) { dp.SetSum(10) },
			"scale":           func(dp pmetric.ExponentialHistogramDataPoint) { dp.SetScale(3) },
			"zero count":      func(dp pmetric.ExponentialHistogramDataPoint) { dp.SetZeroCount(2) },
			"zero threshold":  func(dp pmetric.ExponentialHistogramDataPoint) { dp.SetZeroThreshold(1e-9) },
			"positive offset": func(dp pmetric.ExponentialHistogramDataPoint) { dp.Positive().SetOffset(-1) },
			"positive bucket": func(dp pmetric.ExponentialHistogramDataPoint) { dp.Positive().BucketCounts().SetAt(0, 9) },
			"negative offset": func(dp pmetric.ExponentialHistogramDataPoint) { dp.Negative().SetOffset(2) },
			"negative bucket": func(dp pmetric.ExponentialHistogramDataPoint) { dp.Negative().BucketCounts().SetAt(0, 9) },
			"flags": func(dp pmetric.ExponentialHistogramDataPoint) {
				dp.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))
			},
		} {
			assert.NotEqual(t, base, build(mutate), "%s must affect the exp histogram token", name)
		}
	})

	t.Run("summary", func(t *testing.T) {
		build := func(mutate func(pmetric.SummaryDataPoint)) string {
			b := tokenTestBatch(t, func(sm pmetric.ScopeMetrics) {
				m := sm.Metrics().AppendEmpty()
				m.SetName("test.summary")
				dp := m.SetEmptySummary().DataPoints().AppendEmpty()
				dp.SetTimestamp(ts)
				dp.SetStartTimestamp(start)
				dp.SetCount(4)
				dp.SetSum(2.5)
				qv := dp.QuantileValues().AppendEmpty()
				qv.SetQuantile(0.5)
				qv.SetValue(1)
				dp.Attributes().PutStr("k", "v")
				if mutate != nil {
					mutate(dp)
				}
			})
			return b.summaryPointsToken()
		}
		base := build(nil)
		for name, mutate := range map[string]func(pmetric.SummaryDataPoint){
			"count":          func(dp pmetric.SummaryDataPoint) { dp.SetCount(5) },
			"sum":            func(dp pmetric.SummaryDataPoint) { dp.SetSum(3.5) },
			"quantile value": func(dp pmetric.SummaryDataPoint) { dp.QuantileValues().At(0).SetValue(2) },
			"flags": func(dp pmetric.SummaryDataPoint) {
				dp.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))
			},
		} {
			assert.NotEqual(t, base, build(mutate), "%s must affect the summary token", name)
		}
	})

	t.Run("exemplars", func(t *testing.T) {
		build := func(mutate func(pmetric.Exemplar)) string {
			b := tokenTestBatch(t, func(sm pmetric.ScopeMetrics) {
				m := sm.Metrics().AppendEmpty()
				m.SetName("test.gauge")
				dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
				dp.SetTimestamp(ts)
				dp.SetDoubleValue(10)
				dp.Attributes().PutStr("k", "v")
				ex := dp.Exemplars().AppendEmpty()
				ex.SetTimestamp(ts)
				ex.SetDoubleValue(1.5)
				ex.SetTraceID(pcommon.TraceID{1, 2, 3, 4})
				ex.SetSpanID(pcommon.SpanID{5, 6})
				ex.FilteredAttributes().PutStr("exk", "exv")
				if mutate != nil {
					mutate(ex)
				}
			})
			return b.exemplarsToken()
		}
		base := build(nil)
		for name, mutate := range map[string]func(pmetric.Exemplar){
			"value":      func(ex pmetric.Exemplar) { ex.SetDoubleValue(2.5) },
			"timestamp":  func(ex pmetric.Exemplar) { ex.SetTimestamp(ts + 1) },
			"trace id":   func(ex pmetric.Exemplar) { ex.SetTraceID(pcommon.TraceID{9}) },
			"span id":    func(ex pmetric.Exemplar) { ex.SetSpanID(pcommon.SpanID{9}) },
			"attr value": func(ex pmetric.Exemplar) { ex.FilteredAttributes().PutStr("exk", "other") },
			"attr added": func(ex pmetric.Exemplar) { ex.FilteredAttributes().PutStr("exk2", "v2") },
		} {
			assert.NotEqual(t, base, build(mutate), "%s must affect the exemplars token", name)
		}
	})
}
