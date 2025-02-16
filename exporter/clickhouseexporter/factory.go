// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

package clickhouseexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter"

import (
	"context"
	"fmt"
	chgo "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal/chgo"
	"net/url"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal/metadata"
)

// NewFactory creates a factory for ClickHouse exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		exporter.WithMetrics(createMetricExporter, metadata.MetricsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		TimeoutSettings:  exporterhelper.NewDefaultTimeoutConfig(),
		QueueSettings:    exporterhelper.NewDefaultQueueConfig(),
		BackOffConfig:    configretry.NewDefaultBackOffConfig(),
		ConnectionParams: map[string]string{},
		Database:         defaultDatabase,
		LogsTableName:    "otel_logs",
		TracesTableName:  "otel_traces",
		TTL:              0,
		CreateSchema:     true,
		AsyncInsert:      true,
		MetricsTables: MetricTablesConfig{
			Gauge:                internal.MetricTypeConfig{Name: defaultMetricTableName + defaultGaugeSuffix},
			Sum:                  internal.MetricTypeConfig{Name: defaultMetricTableName + defaultSumSuffix},
			Summary:              internal.MetricTypeConfig{Name: defaultMetricTableName + defaultSummarySuffix},
			Histogram:            internal.MetricTypeConfig{Name: defaultMetricTableName + defaultHistogramSuffix},
			ExponentialHistogram: internal.MetricTypeConfig{Name: defaultMetricTableName + defaultExpHistogramSuffix},
		},
	}
}

func chConfigFromComponentConfig(cfg *Config, traces bool) (*chgo.ChConfig, error) {
	dsnURL, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", errConfigInvalidEndpoint, err.Error())
	}

	queryParams := dsnURL.Query()

	secureStr := queryParams.Get("secure")
	secure, err := strconv.ParseBool(secureStr)
	if secureStr != "" && err != nil {
		return nil, fmt.Errorf("fail parse secure param: %w", err)
	}
	queryParams.Del("secure")

	compression := queryParams.Get("compress")
	queryParams.Del("compress")
	compressionLevelStr := queryParams.Get("compress_level")
	compressionLevel, err := strconv.Atoi(compressionLevelStr)
	if compressionLevelStr != "" && err != nil {
		return nil, fmt.Errorf("fail parse compress_level param: %w", err)
	}
	queryParams.Del("compress_level")

	settings := make(map[string]string, len(queryParams))
	for key := range queryParams {
		value := queryParams.Get(key)
		settings[key] = value
	}

	table := cfg.LogsTableName
	if traces {
		table = cfg.TracesTableName
	}

	chCfg := chgo.ChConfig{
		Address:          dsnURL.Host,
		User:             cfg.Username,
		Password:         string(cfg.Password),
		Database:         cfg.Database,
		Table:            table,
		Compression:      compression,
		CompressionLevel: compressionLevel,
		TLS:              secure,
		ClientName:       "otel-chgo",
		Settings:         settings,
	}

	return &chCfg, nil
}

// createLogsExporter creates a new exporter for logs.
// Logs are directly inserted into ClickHouse.
func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	c := cfg.(*Config)

	chCfg, err := chConfigFromComponentConfig(c, false)
	if err != nil {
		return nil, fmt.Errorf("cannot create clickhouse logs exporter config: %w", err)
	}

	exporter, err := chgo.NewLogsExporter(chCfg, set.Logger)
	if err != nil {
		return nil, fmt.Errorf("cannot configure clickhouse logs exporter: %w", err)
	}

	return exporterhelper.NewLogs(
		ctx,
		set,
		cfg,
		exporter.PushLogsData,
		exporterhelper.WithStart(exporter.Start),
		exporterhelper.WithShutdown(exporter.Shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.BackOffConfig),
	)

	//exporter, err := newLogsExporter(set.Logger, c)
	//if err != nil {
	//	return nil, fmt.Errorf("cannot configure clickhouse logs exporter: %w", err)
	//}
	//
	//return exporterhelper.NewLogs(
	//	ctx,
	//	set,
	//	cfg,
	//	exporter.pushLogsData,
	//	exporterhelper.WithStart(exporter.start),
	//	exporterhelper.WithShutdown(exporter.shutdown),
	//	exporterhelper.WithTimeout(c.TimeoutSettings),
	//	exporterhelper.WithQueue(c.QueueSettings),
	//	exporterhelper.WithRetry(c.BackOffConfig),
	//)
}

// createTracesExporter creates a new exporter for traces.
// Traces are directly inserted into ClickHouse.
func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	c := cfg.(*Config)
	exporter, err := newTracesExporter(set.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure clickhouse traces exporter: %w", err)
	}

	return exporterhelper.NewTraces(
		ctx,
		set,
		cfg,
		exporter.pushTraceData,
		exporterhelper.WithStart(exporter.start),
		exporterhelper.WithShutdown(exporter.shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}

func createMetricExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	c := cfg.(*Config)
	exporter, err := newMetricsExporter(set.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure clickhouse metrics exporter: %w", err)
	}

	return exporterhelper.NewMetrics(
		ctx,
		set,
		cfg,
		exporter.pushMetricsData,
		exporterhelper.WithStart(exporter.start),
		exporterhelper.WithShutdown(exporter.shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}

func generateTTLExpr(ttl time.Duration, timeField string) string {
	if ttl > 0 {
		switch {
		case ttl%(24*time.Hour) == 0:
			return fmt.Sprintf(`TTL %s + toIntervalDay(%d)`, timeField, ttl/(24*time.Hour))
		case ttl%(time.Hour) == 0:
			return fmt.Sprintf(`TTL %s + toIntervalHour(%d)`, timeField, ttl/time.Hour)
		case ttl%(time.Minute) == 0:
			return fmt.Sprintf(`TTL %s + toIntervalMinute(%d)`, timeField, ttl/time.Minute)
		default:
			return fmt.Sprintf(`TTL %s + toIntervalSecond(%d)`, timeField, ttl/time.Second)
		}
	}
	return ""
}
