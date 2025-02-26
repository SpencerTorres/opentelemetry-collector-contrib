// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter"

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal/json"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal"
)

type logsExporter struct {
	db        driver.Conn
	insertSQL string

	logger *zap.Logger
	cfg    *Config

	resourceAttributesBufferPool *internal.ExporterStructPool[*json.JSONBuffer]
	scopeAttributesBufferPool    *internal.ExporterStructPool[*json.JSONBuffer]
	logAttributesBufferPool      *internal.ExporterStructPool[*json.JSONBuffer]
	traceHexBufferPool           *internal.ExporterStructPool[[]byte]
	spanHexBufferPool            *internal.ExporterStructPool[[]byte]
}

func newLogsExporter(logger *zap.Logger, cfg *Config, numConsumers int) (*logsExporter, error) {
	db, err := newClickhouseNativeClient(cfg)
	if err != nil {
		return nil, err
	}

	newJSONBuffer := func() (*json.JSONBuffer, error) {
		return json.NewJSONBuffer(2048, 256), nil
	}
	newHexBuffer := func() ([]byte, error) {
		return make([]byte, 0, 128), nil
	}

	resourceAttributesBufferPool, _ := internal.NewExporterStructPool[*json.JSONBuffer](numConsumers, newJSONBuffer)
	scopeAttributesBufferPool, _ := internal.NewExporterStructPool[*json.JSONBuffer](numConsumers, newJSONBuffer)
	logAttributesBufferPool, _ := internal.NewExporterStructPool[*json.JSONBuffer](numConsumers, newJSONBuffer)
	traceHexBufferPool, _ := internal.NewExporterStructPool[[]byte](numConsumers, newHexBuffer)
	spanHexBufferPool, _ := internal.NewExporterStructPool[[]byte](numConsumers, newHexBuffer)

	return &logsExporter{
		db:                           db,
		insertSQL:                    renderInsertLogsSQL(cfg),
		logger:                       logger,
		cfg:                          cfg,
		resourceAttributesBufferPool: resourceAttributesBufferPool,
		scopeAttributesBufferPool:    scopeAttributesBufferPool,
		logAttributesBufferPool:      logAttributesBufferPool,
		traceHexBufferPool:           traceHexBufferPool,
		spanHexBufferPool:            spanHexBufferPool,
	}, nil
}

func (e *logsExporter) start(ctx context.Context, _ component.Host) error {
	if !e.cfg.shouldCreateSchema() {
		return nil
	}

	if err := createDatabaseNative(ctx, e.cfg, e.db); err != nil {
		return err
	}

	return createLogsTable(ctx, e.cfg, e.db)
}

// shutdown will shut down the exporter.
func (e *logsExporter) shutdown(_ context.Context) error {
	if e.db == nil {
		return nil
	}

	e.resourceAttributesBufferPool.Destroy()
	e.scopeAttributesBufferPool.Destroy()
	e.logAttributesBufferPool.Destroy()
	e.traceHexBufferPool.Destroy()
	e.spanHexBufferPool.Destroy()

	return e.db.Close()
}

func (e *logsExporter) pushLogsData(ctx context.Context, ld plog.Logs) error {
	batch, err := e.db.PrepareBatch(ctx, e.insertSQL)
	if err != nil {
		return err
	}

	resourceAttributesBuffer := e.resourceAttributesBufferPool.Acquire()
	defer e.resourceAttributesBufferPool.Release(resourceAttributesBuffer)
	scopeAttributesBuffer := e.scopeAttributesBufferPool.Acquire()
	defer e.scopeAttributesBufferPool.Release(scopeAttributesBuffer)
	logAttributesBuffer := e.logAttributesBufferPool.Acquire()
	defer e.logAttributesBufferPool.Release(logAttributesBuffer)
	traceHexBuffer := e.traceHexBufferPool.Acquire()
	defer e.traceHexBufferPool.Release(traceHexBuffer)
	spanHexBuffer := e.spanHexBufferPool.Acquire()
	defer e.spanHexBufferPool.Release(spanHexBuffer)

	processStart := time.Now()

	var logCount int
	rsLogs := ld.ResourceLogs()
	rsLen := rsLogs.Len()
	for i := 0; i < rsLen; i++ {
		logs := rsLogs.At(i)
		res := logs.Resource()
		resURL := logs.SchemaUrl()
		resAttr := res.Attributes()
		serviceName := internal.GetServiceName(resAttr)
		resourceAttributesBuffer.Reset()
		json.AttributesToJSON(resourceAttributesBuffer, resAttr)

		slLen := logs.ScopeLogs().Len()
		for j := 0; j < slLen; j++ {
			scopeLog := logs.ScopeLogs().At(j)
			scopeURL := scopeLog.SchemaUrl()
			scopeLogScope := scopeLog.Scope()
			scopeName := scopeLogScope.Name()
			scopeVersion := scopeLogScope.Version()
			scopeLogRecords := scopeLog.LogRecords()
			scopeAttributesBuffer.Reset()
			json.AttributesToJSON(scopeAttributesBuffer, scopeLogScope.Attributes())

			slrLen := scopeLogRecords.Len()
			for k := 0; k < slrLen; k++ {
				r := scopeLogRecords.At(k)
				logAttributesBuffer.Reset()
				json.AttributesToJSON(logAttributesBuffer, r.Attributes())

				timestamp := r.Timestamp()
				if timestamp == 0 {
					timestamp = r.ObservedTimestamp()
				}

				traceHexBuffer = json.AppendTraceIDToHex(traceHexBuffer[:0], r.TraceID())
				spanHexBuffer = json.AppendSpanIDToHex(spanHexBuffer[:0], r.SpanID())
				batch.Append(
					timestamp.AsTime(),
					traceHexBuffer,
					spanHexBuffer,
					uint8(r.Flags()),
					r.SeverityText(),
					uint8(r.SeverityNumber()),
					serviceName,
					r.Body().Str(),
					resURL,
					resourceAttributesBuffer.Bytes(),
					scopeURL,
					scopeName,
					scopeVersion,
					scopeAttributesBuffer.Bytes(),
					logAttributesBuffer.Bytes(),
				)

				logCount++
			}
		}
	}

	processDuration := time.Since(processStart)
	networkStart := time.Now()
	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse logs insert failed: %w", err)
	}

	networkDuration := time.Since(networkStart)
	totalDuration := time.Since(processStart)
	e.logger.Debug("insert logs", zap.Int("records", logCount),
		zap.String("process_cost", processDuration.String()),
		zap.String("network_cost", networkDuration.String()),
		zap.String("total_cost", totalDuration.String()))

	return nil
}

const (
	// language=ClickHouse SQL
	createLogsTableSQL = `
CREATE TABLE IF NOT EXISTS %s %s (
	Timestamp DateTime64(9) CODEC(Delta(8), ZSTD(1)),
	TimestampTime DateTime DEFAULT toDateTime(Timestamp),
	TraceId String CODEC(ZSTD(1)),
	SpanId String CODEC(ZSTD(1)),
	TraceFlags UInt8,
	SeverityText LowCardinality(String) CODEC(ZSTD(1)),
	SeverityNumber UInt8,
	ServiceName LowCardinality(String) CODEC(ZSTD(1)),
	Body String CODEC(ZSTD(1)),
	ResourceSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
	ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	ScopeSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
	ScopeName String CODEC(ZSTD(1)),
	ScopeVersion LowCardinality(String) CODEC(ZSTD(1)),
	ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	LogAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),

	INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
	INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_log_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_log_attr_value mapValues(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_body Body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 8
) ENGINE = %s
PARTITION BY toDate(TimestampTime)
PRIMARY KEY (ServiceName, TimestampTime)
ORDER BY (ServiceName, TimestampTime, Timestamp)
%s
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`
	// language=ClickHouse SQL
	insertLogsSQLTemplate = `INSERT INTO %s (
                        Timestamp,
                        TraceId,
                        SpanId,
                        TraceFlags,
                        SeverityText,
                        SeverityNumber,
                        ServiceName,
                        Body,
                        ResourceSchemaUrl,
                        ResourceAttributes,
                        ScopeSchemaUrl,
                        ScopeName,
                        ScopeVersion,
                        ScopeAttributes,
                        LogAttributes
                        ) VALUES (
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?
                                  )`
)

var driverName = "clickhouse" // for testing

// newClickhouseClient create a clickhouse client.
func newClickhouseClient(cfg *Config) (*sql.DB, error) {
	db, err := cfg.buildDB()
	if err != nil {
		return nil, err
	}
	return db, nil
}

// newClickhouseNativeClient create a clickhouse client using the native interface.
func newClickhouseNativeClient(cfg *Config) (driver.Conn, error) {
	db, err := cfg.buildNativeDB()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func createDatabase(ctx context.Context, cfg *Config) error {
	// use default database to create new database
	if cfg.Database == defaultDatabase {
		return nil
	}

	db, err := cfg.buildDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s %s", cfg.Database, cfg.clusterString())
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	return nil
}

func createDatabaseNative(ctx context.Context, cfg *Config, db driver.Conn) error {
	// use default database to create new database
	if cfg.Database == defaultDatabase {
		return nil
	}

	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s %s", cfg.Database, cfg.clusterString())
	err := db.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	return nil
}

func createLogsTable(ctx context.Context, cfg *Config, db driver.Conn) error {
	if err := db.Exec(ctx, renderCreateLogsTableSQL(cfg)); err != nil {
		return fmt.Errorf("exec create logs table sql: %w", err)
	}
	return nil
}

func renderCreateLogsTableSQL(cfg *Config) string {
	ttlExpr := generateTTLExpr(cfg.TTL, "TimestampTime")
	return fmt.Sprintf(createLogsTableSQL, cfg.LogsTableName, cfg.clusterString(), cfg.tableEngineString(), ttlExpr)
}

func renderInsertLogsSQL(cfg *Config) string {
	return fmt.Sprintf(insertLogsSQLTemplate, cfg.LogsTableName)
}

func doWithTx(_ context.Context, db *sql.DB, fn func(tx *sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("db.Begin: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
