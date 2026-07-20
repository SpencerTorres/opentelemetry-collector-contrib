// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package clickhouseexporter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap/zaptest"
)

func testMetricsV2Exporter(t *testing.T, endpoint string) {
	// The v2 series table always uses text indexes (ClickHouse 26.2+); skip
	// on older images in the integration matrix, same as profiles.
	requireFullTextSearch(t, endpoint)
	exporter := newTestMetricsV2Exporter(t, endpoint)
	verifyExporterMetricsV2(t, exporter)
}

func newTestMetricsV2Exporter(t *testing.T, dsn string, fns ...func(*Config)) *metricsV2Exporter {
	fns = append(fns, func(cfg *Config) {
		cfg.MetricsSchema = metricsSchemaV2
	})
	exporter := newMetricsV2Exporter(zaptest.NewLogger(t), withTestExporterConfig(fns...)(dsn))

	require.NoError(t, exporter.start(t.Context(), nil))

	t.Cleanup(func() { _ = exporter.shutdown(t.Context()) })
	return exporter
}

func verifyExporterMetricsV2(t *testing.T, exporter *metricsV2Exporter) {
	metric := pmetric.NewMetrics()
	rm := metric.ResourceMetrics().AppendEmpty()
	simpleMetrics(1000).ResourceMetrics().At(0).CopyTo(rm)

	pushConcurrentlyNoError(t, func() error {
		return exporter.pushMetricsData(t.Context(), metric)
	})

	// The 5 concurrent pushes carry byte-identical payloads, which is exactly
	// what an exporterhelper retry after a partial failure looks like. The
	// deterministic insert_deduplication_token therefore collapses them to a
	// single insert per table — this assertion is the regression test for
	// retry-induced duplication of raw points and rollup double-counting.
	const pointsPerType = 1000

	count := func(query string) uint64 {
		var result uint64
		row := exporter.db.QueryRow(t.Context(), query)
		require.NoError(t, row.Err())
		require.NoError(t, row.Scan(&result))
		return result
	}

	v2 := &exporter.cfg.MetricsV2
	table := func(name string) string { return fmt.Sprintf("otel_int_test.%q", name) }

	// Gauge and sum share the float points table.
	require.Equal(t, uint64(2*pointsPerType), count("SELECT count() FROM "+table(v2.PointsTableName)))
	require.Equal(t, uint64(pointsPerType), count("SELECT count() FROM "+table(v2.HistogramPointsTableName)))
	require.Equal(t, uint64(pointsPerType), count("SELECT count() FROM "+table(v2.ExpHistogramPointsTableName)))
	require.Equal(t, uint64(pointsPerType), count("SELECT count() FROM "+table(v2.SummaryPointsTableName)))

	// The fixture attaches one exemplar per gauge/sum/histogram/exp histogram point.
	require.Equal(t, uint64(4*pointsPerType), count("SELECT count() FROM "+table(v2.ExemplarsTableName)))

	// Every data point of a metric in the fixture shares one attribute set, so
	// there is exactly one series per metric type regardless of duplicate rows
	// written by the concurrent pushes before merges collapse them.
	require.Equal(t, uint64(5), count("SELECT count(DISTINCT SeriesHash) FROM "+table(v2.SeriesTableName)))
	require.Equal(t, uint64(5), count("SELECT count(DISTINCT MetricName) FROM "+table(v2.FamiliesTableName)))

	// Referential integrity: every point row must resolve to a series row.
	require.Equal(t, uint64(0), count(fmt.Sprintf(
		"SELECT count() FROM %s AS p LEFT ANTI JOIN %s AS s ON p.SeriesHash = s.SeriesHash",
		table(v2.PointsTableName), table(v2.SeriesTableName))))

	// Rollup materialized views fire per insert: bucketed counts must cover
	// every float point.
	require.Equal(t, uint64(2*pointsPerType), count("SELECT sum(Count) FROM "+table(v2.PointsTableName+"_5m")))
	require.Equal(t, uint64(2*pointsPerType), count("SELECT sum(Count) FROM "+table(v2.PointsTableName+"_1h")))

	// Same for the histogram rollups: every histogram point counted exactly
	// once despite the 5 identical concurrent pushes (retry dedup must cover
	// the histogram MV chain too).
	require.Equal(t, uint64(pointsPerType), count("SELECT sum(PointCount) FROM "+table(v2.HistogramPointsTableName+"_5m")))
	require.Equal(t, uint64(pointsPerType), count("SELECT sum(PointCount) FROM "+table(v2.HistogramPointsTableName+"_1h")))

	verifyMetricsV2SeriesRow(t, exporter)
}

func verifyMetricsV2SeriesRow(t *testing.T, exporter *metricsV2Exporter) {
	type series struct {
		MetricName         string            `ch:"MetricName"`
		ServiceName        string            `ch:"ServiceName"`
		MetricType         string            `ch:"MetricType"`
		Temporality        string            `ch:"Temporality"`
		IsMonotonic        bool              `ch:"IsMonotonic"`
		Unit               string            `ch:"Unit"`
		ScopeName          string            `ch:"ScopeName"`
		ScopeVersion       string            `ch:"ScopeVersion"`
		ResourceAttributes map[string]string `ch:"ResourceAttributes"`
		Attributes         map[string]string `ch:"Attributes"`
		ExplicitBounds     []float64         `ch:"ExplicitBounds"`
	}

	v2 := &exporter.cfg.MetricsV2
	row := exporter.db.QueryRow(t.Context(), fmt.Sprintf(
		"SELECT MetricName, ServiceName, MetricType, Temporality, IsMonotonic, Unit, ScopeName, ScopeVersion, ResourceAttributes, Attributes, ExplicitBounds "+
			"FROM otel_int_test.%q WHERE MetricName = 'histogram metrics' LIMIT 1", v2.SeriesTableName))
	require.NoError(t, row.Err())

	var actual series
	require.NoError(t, row.ScanStruct(&actual))

	require.Equal(t, "histogram metrics", actual.MetricName)
	require.Equal(t, "demo 1", actual.ServiceName)
	require.Equal(t, "histogram", actual.MetricType)
	require.Equal(t, "Scope name 1", actual.ScopeName)
	require.Equal(t, "Scope version 1", actual.ScopeVersion)
	require.Equal(t, map[string]string{
		"service.name":          "demo 1",
		"Resource Attributes 1": "value1",
	}, actual.ResourceAttributes)
	require.Equal(t, []float64{0, 0, 0, 0, 0}, actual.ExplicitBounds)
}
