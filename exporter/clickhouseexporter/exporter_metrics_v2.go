// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter"

import (
	"bytes"
	"context"
	"fmt"
	"text/template"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal/metricsv2"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal/sqltemplates"
)

// metricsV2Exporter implements the experimental series/points split metrics
// schema (`metrics_schema: v2`). Data points carry only a 64-bit series
// fingerprint; label sets are written once per series per day to a dedicated
// series table, deduplicated by an in-memory cache and collapsed by
// AggregatingMergeTree merges.
type metricsV2Exporter struct {
	db driver.Conn

	logger     *zap.Logger
	cfg        *Config
	cache      *metricsv2.SeriesCache
	insertSQLs metricsv2.InsertSQLs
}

func newMetricsV2Exporter(logger *zap.Logger, cfg *Config) *metricsV2Exporter {
	return &metricsV2Exporter{
		logger: logger,
		cfg:    cfg,
		cache:  metricsv2.NewSeriesCache(cfg.MetricsV2.SeriesCacheSize, 0, logger),
	}
}

func (e *metricsV2Exporter) start(ctx context.Context, _ component.Host) error {
	opt, err := e.cfg.buildClickHouseOptions()
	if err != nil {
		return err
	}

	e.db, err = internal.NewClickhouseClientFromOptions(opt)
	if err != nil {
		return err
	}

	if err := e.renderInsertSQLs(); err != nil {
		return err
	}

	if e.cfg.shouldCreateSchema() {
		if err := internal.CreateDatabase(ctx, e.db, e.cfg.database(), e.cfg.clusterString()); err != nil {
			return err
		}
		if err := e.createSchema(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (e *metricsV2Exporter) shutdown(_ context.Context) error {
	if e.db != nil {
		return e.db.Close()
	}

	return nil
}

func (e *metricsV2Exporter) pushMetricsData(ctx context.Context, md pmetric.Metrics) error {
	batch := metricsv2.NewBatch(e.cache, e.logger)

	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		batch.SetResource(rm.Resource().Attributes(), rm.SchemaUrl())
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			batch.SetScope(sm.Scope(), sm.SchemaUrl())
			ms := sm.Metrics()
			for k := 0; k < ms.Len(); k++ {
				if err := batch.AddMetric(ms.At(k)); err != nil {
					return err
				}
			}
		}
	}

	return batch.Insert(ctx, e.db, e.insertSQLs)
}

func (e *metricsV2Exporter) renderInsertSQLs() error {
	render := func(tmpl *template.Template, tableName string) (string, error) {
		var buf bytes.Buffer
		err := tmpl.Execute(&buf, sqltemplates.InsertData{
			Database:  e.cfg.database(),
			TableName: tableName,
		})
		if err != nil {
			return "", fmt.Errorf("execute %s: %w", tmpl.Name(), err)
		}
		return buf.String(), nil
	}

	v2 := &e.cfg.MetricsV2
	var err error
	if e.insertSQLs.Series, err = render(sqltemplates.MetricsV2SeriesInsertTmpl, v2.SeriesTableName); err != nil {
		return err
	}
	if e.insertSQLs.Points, err = render(sqltemplates.MetricsV2PointsInsertTmpl, v2.PointsTableName); err != nil {
		return err
	}
	if e.insertSQLs.HistPoints, err = render(sqltemplates.MetricsV2HistogramPointsInsertTmpl, v2.HistogramPointsTableName); err != nil {
		return err
	}
	if e.insertSQLs.ExpHistPoints, err = render(sqltemplates.MetricsV2ExpHistogramPointsInsertTmpl, v2.ExpHistogramPointsTableName); err != nil {
		return err
	}
	if e.insertSQLs.SummaryPoints, err = render(sqltemplates.MetricsV2SummaryPointsInsertTmpl, v2.SummaryPointsTableName); err != nil {
		return err
	}
	if e.insertSQLs.Exemplars, err = render(sqltemplates.MetricsV2ExemplarsInsertTmpl, v2.ExemplarsTableName); err != nil {
		return err
	}
	if e.insertSQLs.Families, err = render(sqltemplates.MetricsV2FamiliesInsertTmpl, v2.FamiliesTableName); err != nil {
		return err
	}

	return nil
}

// createSchema creates every v2 table (and, when enabled, the rollup tables
// and materialized views). Table engines are currently fixed (MergeTree,
// AggregatingMergeTree, ReplacingMergeTree); the table_engine config option
// does not apply to the v2 schema yet.
//
// The series table always uses text (full-text-search) indexes and therefore
// requires ClickHouse 26.2 or newer, same as the profiles schema.
func (e *metricsV2Exporter) createSchema(ctx context.Context) error {
	database := e.cfg.database()
	clusterStr := e.cfg.clusterString()
	v2 := &e.cfg.MetricsV2

	pointsTTL := internal.GenerateTTLExpr(e.cfg.TTL, "toDateTime(TimeUnix)")
	seriesTTL := internal.GenerateTTLExpr(e.cfg.TTL, "toDateTime(Date)")

	execTable := func(tmpl *template.Template, tableName, ttl string) error {
		var buf bytes.Buffer
		err := tmpl.Execute(&buf, sqltemplates.CreateTableData{
			Database:      database,
			TableName:     tableName,
			ClusterString: clusterStr,
			TTL:           ttl,
		})
		if err != nil {
			return fmt.Errorf("execute %s: %w", tmpl.Name(), err)
		}
		if execErr := e.db.Exec(ctx, buf.String()); execErr != nil {
			return fmt.Errorf("exec %s: %w", tmpl.Name(), execErr)
		}
		return nil
	}

	tables := []struct {
		tmpl      *template.Template
		tableName string
		ttl       string
	}{
		{sqltemplates.MetricsV2FamiliesCreateTableTmpl, v2.FamiliesTableName, ""},
		{sqltemplates.MetricsV2SeriesCreateTableTmpl, v2.SeriesTableName, seriesTTL},
		{sqltemplates.MetricsV2PointsCreateTableTmpl, v2.PointsTableName, pointsTTL},
		{sqltemplates.MetricsV2HistogramPointsCreateTableTmpl, v2.HistogramPointsTableName, pointsTTL},
		{sqltemplates.MetricsV2ExpHistogramPointsCreateTableTmpl, v2.ExpHistogramPointsTableName, pointsTTL},
		{sqltemplates.MetricsV2SummaryPointsCreateTableTmpl, v2.SummaryPointsTableName, pointsTTL},
		{sqltemplates.MetricsV2ExemplarsCreateTableTmpl, v2.ExemplarsTableName, pointsTTL},
	}
	for _, t := range tables {
		if err := execTable(t.tmpl, t.tableName, t.ttl); err != nil {
			return err
		}
	}

	if !v2.RollupsEnabled {
		return nil
	}

	execView := func(tmpl *template.Template, viewName, sourceTable, destTable string) error {
		var buf bytes.Buffer
		err := tmpl.Execute(&buf, sqltemplates.MaterializedViewData{
			Database:        database,
			ViewName:        viewName,
			SourceTableName: sourceTable,
			DestTableName:   destTable,
			ClusterString:   clusterStr,
		})
		if err != nil {
			return fmt.Errorf("execute %s: %w", tmpl.Name(), err)
		}
		if execErr := e.db.Exec(ctx, buf.String()); execErr != nil {
			return fmt.Errorf("exec %s: %w", tmpl.Name(), execErr)
		}
		return nil
	}

	rollupTTL := internal.GenerateTTLExpr(v2.RollupTTL, "TimeBucket")
	points5m := v2.PointsTableName + "_5m"
	points1h := v2.PointsTableName + "_1h"
	if err := execTable(sqltemplates.MetricsV2Points5mCreateTableTmpl, points5m, rollupTTL); err != nil {
		return err
	}
	if err := execView(sqltemplates.MetricsV2Points5mCreateViewTmpl, points5m+"_mv", v2.PointsTableName, points5m); err != nil {
		return err
	}
	if err := execTable(sqltemplates.MetricsV2Points1hCreateTableTmpl, points1h, rollupTTL); err != nil {
		return err
	}
	if err := execView(sqltemplates.MetricsV2Points1hCreateViewTmpl, points1h+"_mv", points5m, points1h); err != nil {
		return err
	}

	// Explicit-bounds histogram rollups: First/Last/Sum of the scalar columns
	// plus per-bucket-array states (argMin/argMax for cumulative chaining,
	// sumForEach for exact delta window increases). Exponential histograms are
	// deliberately NOT rolled up: their per-point Scale/Offset can vary, so
	// naive element-wise bucket aggregation is unsafe without downscale-merge
	// logic — long-range exp-histogram queries stay on the raw tier.
	hist5m := v2.HistogramPointsTableName + "_5m"
	hist1h := v2.HistogramPointsTableName + "_1h"
	if err := execTable(sqltemplates.MetricsV2HistogramPoints5mCreateTableTmpl, hist5m, rollupTTL); err != nil {
		return err
	}
	if err := execView(sqltemplates.MetricsV2HistogramPoints5mCreateViewTmpl, hist5m+"_mv", v2.HistogramPointsTableName, hist5m); err != nil {
		return err
	}
	if err := execTable(sqltemplates.MetricsV2HistogramPoints1hCreateTableTmpl, hist1h, rollupTTL); err != nil {
		return err
	}
	if err := execView(sqltemplates.MetricsV2HistogramPoints1hCreateViewTmpl, hist1h+"_mv", hist5m, hist1h); err != nil {
		return err
	}

	return nil
}
