// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
package sqltemplates // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal/sqltemplates"

import (
	_ "embed"
	"fmt"
	"strings"
	"text/template"
)

// templateFuncs provides helper functions available in all SQL templates.
var templateFuncs = template.FuncMap{
	// ident wraps a ClickHouse identifier in backticks, escaping any embedded backticks.
	"ident": func(s string) string {
		return fmt.Sprintf("`%s`", strings.ReplaceAll(s, "`", "\\`"))
	},
}

// newTemplate creates a named template with the shared function map.
func newTemplate(name, text string) *template.Template {
	return template.Must(template.New(name).Funcs(templateFuncs).Parse(text))
}

// LOGS

//go:embed logs_table.sql
var LogsCreateTable string

//go:embed logs_insert.sql
var LogsInsert string

//go:embed logs_json_table.sql
var LogsJSONCreateTable string

//go:embed logs_json_insert.sql
var LogsJSONInsert string

// Parsed templates for logs (text/template).
var (
	LogsCreateTableTmpl     = newTemplate("logs_table", LogsCreateTable)
	LogsInsertTmpl          = newTemplate("logs_insert", LogsInsert)
	LogsJSONCreateTableTmpl = newTemplate("logs_json_table", LogsJSONCreateTable)
	LogsJSONInsertTmpl      = newTemplate("logs_json_insert", LogsJSONInsert)
)

// CreateTableData contains the template parameters for creating a logs table.
type CreateTableData struct {
	Database          string
	TableName         string
	ClusterString     string
	Engine            string
	TTL               string
	HasFullTextSearch bool
}

// InsertData contains the template parameters for a logs INSERT statement.
type InsertData struct {
	Database               string
	TableName              string
	FeatureColumnNames     string
	FeatureColumnPositions string
}

// TRACES

//go:embed traces_table.sql
var TracesCreateTable string

//go:embed traces_json_table.sql
var TracesJSONCreateTable string

//go:embed traces_id_ts_lookup_table.sql
var TracesCreateTsTable string

//go:embed traces_id_ts_lookup_mv.sql
var TracesCreateTsView string

//go:embed traces_insert.sql
var TracesInsert string

//go:embed traces_json_insert.sql
var TracesJSONInsert string

// PROFILES

//go:embed profiles_table.sql
var ProfilesCreateTable string

//go:embed profiles_insert.sql
var ProfilesInsert string

// METRICS

//go:embed metrics_gauge_table.sql
var MetricsGaugeCreateTable string

//go:embed metrics_gauge_insert.sql
var MetricsGaugeInsert string

//go:embed metrics_exp_histogram_table.sql
var MetricsExpHistogramCreateTable string

//go:embed metrics_exp_histogram_insert.sql
var MetricsExpHistogramInsert string

//go:embed metrics_histogram_table.sql
var MetricsHistogramCreateTable string

//go:embed metrics_histogram_insert.sql
var MetricsHistogramInsert string

//go:embed metrics_sum_table.sql
var MetricsSumCreateTable string

//go:embed metrics_sum_insert.sql
var MetricsSumInsert string

//go:embed metrics_summary_table.sql
var MetricsSummaryCreateTable string

//go:embed metrics_summary_insert.sql
var MetricsSummaryInsert string

// METRICS V2 (series/points split schema)

//go:embed metrics_v2_series_table.sql
var MetricsV2SeriesCreateTable string

//go:embed metrics_v2_series_insert.sql
var MetricsV2SeriesInsert string

//go:embed metrics_v2_points_table.sql
var MetricsV2PointsCreateTable string

//go:embed metrics_v2_points_insert.sql
var MetricsV2PointsInsert string

//go:embed metrics_v2_histogram_points_table.sql
var MetricsV2HistogramPointsCreateTable string

//go:embed metrics_v2_histogram_points_insert.sql
var MetricsV2HistogramPointsInsert string

//go:embed metrics_v2_exp_histogram_points_table.sql
var MetricsV2ExpHistogramPointsCreateTable string

//go:embed metrics_v2_exp_histogram_points_insert.sql
var MetricsV2ExpHistogramPointsInsert string

//go:embed metrics_v2_summary_points_table.sql
var MetricsV2SummaryPointsCreateTable string

//go:embed metrics_v2_summary_points_insert.sql
var MetricsV2SummaryPointsInsert string

//go:embed metrics_v2_exemplars_table.sql
var MetricsV2ExemplarsCreateTable string

//go:embed metrics_v2_exemplars_insert.sql
var MetricsV2ExemplarsInsert string

//go:embed metrics_v2_families_table.sql
var MetricsV2FamiliesCreateTable string

//go:embed metrics_v2_families_insert.sql
var MetricsV2FamiliesInsert string

//go:embed metrics_v2_points_5m_table.sql
var MetricsV2Points5mCreateTable string

//go:embed metrics_v2_points_5m_mv.sql
var MetricsV2Points5mCreateView string

//go:embed metrics_v2_points_1h_table.sql
var MetricsV2Points1hCreateTable string

//go:embed metrics_v2_points_1h_mv.sql
var MetricsV2Points1hCreateView string

//go:embed metrics_v2_histogram_points_5m_table.sql
var MetricsV2HistogramPoints5mCreateTable string

//go:embed metrics_v2_histogram_points_5m_mv.sql
var MetricsV2HistogramPoints5mCreateView string

//go:embed metrics_v2_histogram_points_1h_table.sql
var MetricsV2HistogramPoints1hCreateTable string

//go:embed metrics_v2_histogram_points_1h_mv.sql
var MetricsV2HistogramPoints1hCreateView string

// Parsed templates for the metrics v2 schema (text/template).
var (
	MetricsV2SeriesCreateTableTmpl             = newTemplate("metrics_v2_series_table", MetricsV2SeriesCreateTable)
	MetricsV2SeriesInsertTmpl                  = newTemplate("metrics_v2_series_insert", MetricsV2SeriesInsert)
	MetricsV2PointsCreateTableTmpl             = newTemplate("metrics_v2_points_table", MetricsV2PointsCreateTable)
	MetricsV2PointsInsertTmpl                  = newTemplate("metrics_v2_points_insert", MetricsV2PointsInsert)
	MetricsV2HistogramPointsCreateTableTmpl    = newTemplate("metrics_v2_histogram_points_table", MetricsV2HistogramPointsCreateTable)
	MetricsV2HistogramPointsInsertTmpl         = newTemplate("metrics_v2_histogram_points_insert", MetricsV2HistogramPointsInsert)
	MetricsV2ExpHistogramPointsCreateTableTmpl = newTemplate("metrics_v2_exp_histogram_points_table", MetricsV2ExpHistogramPointsCreateTable)
	MetricsV2ExpHistogramPointsInsertTmpl      = newTemplate("metrics_v2_exp_histogram_points_insert", MetricsV2ExpHistogramPointsInsert)
	MetricsV2SummaryPointsCreateTableTmpl      = newTemplate("metrics_v2_summary_points_table", MetricsV2SummaryPointsCreateTable)
	MetricsV2SummaryPointsInsertTmpl           = newTemplate("metrics_v2_summary_points_insert", MetricsV2SummaryPointsInsert)
	MetricsV2ExemplarsCreateTableTmpl          = newTemplate("metrics_v2_exemplars_table", MetricsV2ExemplarsCreateTable)
	MetricsV2ExemplarsInsertTmpl               = newTemplate("metrics_v2_exemplars_insert", MetricsV2ExemplarsInsert)
	MetricsV2FamiliesCreateTableTmpl           = newTemplate("metrics_v2_families_table", MetricsV2FamiliesCreateTable)
	MetricsV2FamiliesInsertTmpl                = newTemplate("metrics_v2_families_insert", MetricsV2FamiliesInsert)
	MetricsV2Points5mCreateTableTmpl           = newTemplate("metrics_v2_points_5m_table", MetricsV2Points5mCreateTable)
	MetricsV2Points5mCreateViewTmpl            = newTemplate("metrics_v2_points_5m_mv", MetricsV2Points5mCreateView)
	MetricsV2Points1hCreateTableTmpl           = newTemplate("metrics_v2_points_1h_table", MetricsV2Points1hCreateTable)
	MetricsV2Points1hCreateViewTmpl            = newTemplate("metrics_v2_points_1h_mv", MetricsV2Points1hCreateView)
	MetricsV2HistogramPoints5mCreateTableTmpl  = newTemplate("metrics_v2_histogram_points_5m_table", MetricsV2HistogramPoints5mCreateTable)
	MetricsV2HistogramPoints5mCreateViewTmpl   = newTemplate("metrics_v2_histogram_points_5m_mv", MetricsV2HistogramPoints5mCreateView)
	MetricsV2HistogramPoints1hCreateTableTmpl  = newTemplate("metrics_v2_histogram_points_1h_table", MetricsV2HistogramPoints1hCreateTable)
	MetricsV2HistogramPoints1hCreateViewTmpl   = newTemplate("metrics_v2_histogram_points_1h_mv", MetricsV2HistogramPoints1hCreateView)
)

// MaterializedViewData contains the template parameters for creating a
// materialized view that reads from SourceTableName and writes to DestTableName.
type MaterializedViewData struct {
	Database        string
	ViewName        string
	SourceTableName string
	DestTableName   string
	ClusterString   string
}
