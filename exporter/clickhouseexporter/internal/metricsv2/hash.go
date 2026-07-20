// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package metricsv2 implements the experimental "v2" metrics schema for the
// ClickHouse exporter: a series/points split where label sets are written once
// per series per day into a series table, and data points carry only a
// 64-bit series fingerprint (SeriesHash).
package metricsv2 // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal/metricsv2"

import (
	"encoding/binary"
	"math"
	"sort"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column/orderedmap"
	"github.com/go-faster/city"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// SeriesHash canonical serialization, version 1.
//
// SeriesHash = cityHash64(S) where S is the concatenation of the sections
// below, in order. All strings are encoded as uvarint(len) followed by raw
// bytes; attribute maps are encoded as uvarint(count) followed by key/value
// pairs sorted by key (byte order); float64 values are encoded as their
// IEEE-754 bits in little-endian order.
//
//	0x01 <resource attributes>
//	0x02 <scope name> <scope version> 0x02 <scope attributes>
//	0x00 <metric name>
//	0x03 <data point attributes>
//	0x04 uvarint(count) <explicit bounds>    (histogram series only)
//	0x05 uvarint(count) <quantile levels>    (summary series only)
//
// Note the 0x02 section byte appears twice in the scope section: once before
// the name/version strings and once as the attribute-map prefix (writeAttrs
// always emits its section byte). The golden test pins this exact layout.
//
// Design notes:
//   - Attribute keys are namespaced by section, so a resource attribute and a
//     data point attribute with the same key/value produce different hashes.
//   - Histogram bounds and summary quantile levels are part of the series
//     identity. This mirrors the Prometheus model, where each bound is its own
//     `le`-labeled series: changing the bucket layout produces a new series.
//   - Attribute values are hashed exactly as stored (pcommon.Value.AsString),
//     so the hash of a stored row can be reproduced from the row itself.
//   - Metric type, unit, and temporality are metadata, not identity, and are
//     deliberately excluded.
//
// This serialization must remain stable; treat any change as a new hash
// version requiring a new schema revision.
const (
	sectionMetricName     = 0x00
	sectionResourceAttrs  = 0x01
	sectionScope          = 0x02
	sectionDataPointAttrs = 0x03
	sectionBounds         = 0x04
	sectionQuantiles      = 0x05
)

// attrPair is a single key/value attribute with the value already rendered
// the same way it is stored in ClickHouse Map columns.
type attrPair struct {
	k, v string
}

// sortedPairs extracts the attributes of m into reuse (reset to length zero),
// sorted by key. The returned slice aliases reuse's backing array.
func sortedPairs(m pcommon.Map, reuse []attrPair) []attrPair {
	pairs := reuse[:0]
	m.Range(func(k string, v pcommon.Value) bool {
		pairs = append(pairs, attrPair{k: k, v: v.AsString()})
		return true
	})
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].k < pairs[j].k })
	return pairs
}

// orderedMapFromPairs builds a clickhouse-go map column value that iterates in
// the given (sorted) order. Writing key-sorted maps improves Map column
// compression and keeps stored maps consistent with the hash serialization.
func orderedMapFromPairs(pairs []attrPair) column.IterableOrderedMap {
	return orderedmap.CollectN(func(yield func(string, string) bool) {
		for i := range pairs {
			if !yield(pairs[i].k, pairs[i].v) {
				return
			}
		}
	}, len(pairs))
}

// hasher incrementally builds the canonical serialization buffer. The buffer
// is reusable across data points: sections shared by many points (resource,
// scope) are written once and the per-point tail is rewound via truncate.
type hasher struct {
	buf []byte
}

func (h *hasher) reset() {
	h.buf = h.buf[:0]
}

// len returns the current buffer length, for use with truncate.
func (h *hasher) len() int {
	return len(h.buf)
}

// truncate rewinds the buffer to a length previously returned by len.
func (h *hasher) truncate(n int) {
	h.buf = h.buf[:n]
}

func (h *hasher) writeByte(b byte) {
	h.buf = append(h.buf, b)
}

func (h *hasher) writeString(s string) {
	h.buf = binary.AppendUvarint(h.buf, uint64(len(s)))
	h.buf = append(h.buf, s...)
}

func (h *hasher) writeAttrs(section byte, pairs []attrPair) {
	h.writeByte(section)
	h.buf = binary.AppendUvarint(h.buf, uint64(len(pairs)))
	for i := range pairs {
		h.writeString(pairs[i].k)
		h.writeString(pairs[i].v)
	}
}

func (h *hasher) writeFloats(section byte, vals []float64) {
	h.writeByte(section)
	h.buf = binary.AppendUvarint(h.buf, uint64(len(vals)))
	for _, v := range vals {
		h.buf = binary.LittleEndian.AppendUint64(h.buf, math.Float64bits(v))
	}
}

// sum returns the series hash of the current buffer contents.
func (h *hasher) sum() uint64 {
	return city.CH64(h.buf)
}
