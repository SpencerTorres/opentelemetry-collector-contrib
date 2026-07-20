// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metricsv2

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestSortedPairsCanonicalOrder(t *testing.T) {
	a := pcommon.NewMap()
	a.PutStr("zebra", "1")
	a.PutStr("alpha", "2")
	a.PutInt("mid", 3)

	b := pcommon.NewMap()
	b.PutInt("mid", 3)
	b.PutStr("alpha", "2")
	b.PutStr("zebra", "1")

	pairsA := sortedPairs(a, nil)
	pairsB := sortedPairs(b, nil)

	require.Equal(t, pairsA, pairsB)
	require.Equal(t, []attrPair{{"alpha", "2"}, {"mid", "3"}, {"zebra", "1"}}, pairsA)
}

func TestHasherMapOrderIndependent(t *testing.T) {
	a := pcommon.NewMap()
	a.PutStr("k1", "v1")
	a.PutStr("k2", "v2")

	b := pcommon.NewMap()
	b.PutStr("k2", "v2")
	b.PutStr("k1", "v1")

	var h1, h2 hasher
	h1.writeAttrs(sectionDataPointAttrs, sortedPairs(a, nil))
	h2.writeAttrs(sectionDataPointAttrs, sortedPairs(b, nil))

	assert.Equal(t, h1.sum(), h2.sum())
}

func TestHasherSectionNamespacing(t *testing.T) {
	m := pcommon.NewMap()
	m.PutStr("k", "v")
	pairs := sortedPairs(m, nil)

	var asResource, asPoint hasher
	asResource.writeAttrs(sectionResourceAttrs, pairs)
	asPoint.writeAttrs(sectionDataPointAttrs, pairs)

	assert.NotEqual(t, asResource.sum(), asPoint.sum(),
		"the same key/value must hash differently under different attribute namespaces")
}

func TestHasherKeyValueBoundary(t *testing.T) {
	// Length prefixing must prevent ambiguous concatenations: ("ab","c") vs ("a","bc").
	var h1, h2 hasher
	h1.writeAttrs(sectionDataPointAttrs, []attrPair{{"ab", "c"}})
	h2.writeAttrs(sectionDataPointAttrs, []attrPair{{"a", "bc"}})
	assert.NotEqual(t, h1.sum(), h2.sum())
}

func TestHasherBoundsChangeIdentity(t *testing.T) {
	var h1, h2 hasher
	h1.writeString("metric")
	h1.writeFloats(sectionBounds, []float64{0.1, 0.5, 1})
	h2.writeString("metric")
	h2.writeFloats(sectionBounds, []float64{0.1, 0.5, 2.5})
	assert.NotEqual(t, h1.sum(), h2.sum(),
		"histogram bucket bounds are part of the series identity")
}

func TestHasherTruncateReuse(t *testing.T) {
	var h hasher
	h.writeAttrs(sectionResourceAttrs, []attrPair{{"res", "1"}})
	mark := h.len()

	h.writeByte(sectionMetricName)
	h.writeString("metric_a")
	first := h.sum()

	h.truncate(mark)
	h.writeByte(sectionMetricName)
	h.writeString("metric_b")
	second := h.sum()

	h.truncate(mark)
	h.writeByte(sectionMetricName)
	h.writeString("metric_a")
	assert.Equal(t, first, h.sum(), "rewinding and rewriting the same tail must reproduce the hash")
	assert.NotEqual(t, first, second)
}

// TestSeriesHashGolden pins the canonical serialization. If this test breaks,
// the on-disk SeriesHash definition changed: bump the schema version rather
// than updating the constant casually.
func TestSeriesHashGolden(t *testing.T) {
	res := pcommon.NewMap()
	res.PutStr("service.name", "svc")
	scopeAttrs := pcommon.NewMap()
	scopeAttrs.PutStr("scope.key", "sv")
	dp := pcommon.NewMap()
	dp.PutStr("label_a", "1")
	dp.PutStr("label_b", "2")

	var h hasher
	h.writeAttrs(sectionResourceAttrs, sortedPairs(res, nil))
	h.writeByte(sectionScope)
	h.writeString("scope-name")
	h.writeString("1.2.3")
	h.writeAttrs(sectionScope, sortedPairs(scopeAttrs, nil))
	h.writeByte(sectionMetricName)
	h.writeString("http.server.request.duration")
	h.writeAttrs(sectionDataPointAttrs, sortedPairs(dp, nil))
	h.writeFloats(sectionBounds, []float64{0.005, 0.01, 0.025})

	assert.Equal(t, uint64(0x5e339cb4a7a309a9), h.sum())
}

func TestOrderedMapFromPairsIteratesSorted(t *testing.T) {
	m := pcommon.NewMap()
	m.PutStr("c", "3")
	m.PutStr("a", "1")
	m.PutStr("b", "2")

	om := orderedMapFromPairs(sortedPairs(m, nil))
	iter := om.Iterator()
	var keys []string
	for iter.Next() {
		keys = append(keys, iter.Key().(string))
	}
	assert.Equal(t, []string{"a", "b", "c"}, keys)
}
