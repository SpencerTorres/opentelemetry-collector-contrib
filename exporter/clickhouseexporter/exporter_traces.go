// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter"

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal/json"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/traceutil"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type tracesExporter struct {
	db        driver.Conn
	insertSQL string

	logger *zap.Logger
	cfg    *Config

	resourceAttributesBufferPool *internal.ExporterStructPool[*json.JSONBuffer]
	spanAttributesBufferPool     *internal.ExporterStructPool[*json.JSONBuffer]
	eventsAttributesBufferPool   *internal.ExporterStructPool[*json.JSONBuffer]
	linksAttributesBufferPool    *internal.ExporterStructPool[*json.JSONBuffer]
}

func newTracesExporter(logger *zap.Logger, cfg *Config, numConsumers int) (*tracesExporter, error) {
	db, err := newClickhouseNativeClient(cfg)
	if err != nil {
		return nil, err
	}

	newJSONBuffer := func() (*json.JSONBuffer, error) {
		return json.NewJSONBuffer(2048, 256), nil
	}

	resourceAttributesBufferPool, _ := internal.NewExporterStructPool[*json.JSONBuffer](numConsumers, newJSONBuffer)
	spanAttributesBufferPool, _ := internal.NewExporterStructPool[*json.JSONBuffer](numConsumers, newJSONBuffer)
	eventsAttributesBufferPool, _ := internal.NewExporterStructPool[*json.JSONBuffer](numConsumers, newJSONBuffer)
	linksAttributesBufferPool, _ := internal.NewExporterStructPool[*json.JSONBuffer](numConsumers, newJSONBuffer)

	return &tracesExporter{
		db:        db,
		insertSQL: renderInsertTracesSQL(cfg),
		logger:    logger,
		cfg:       cfg,

		resourceAttributesBufferPool: resourceAttributesBufferPool,
		spanAttributesBufferPool:     spanAttributesBufferPool,
		eventsAttributesBufferPool:   eventsAttributesBufferPool,
		linksAttributesBufferPool:    linksAttributesBufferPool,
	}, nil
}

func (e *tracesExporter) start(ctx context.Context, _ component.Host) error {
	if !e.cfg.shouldCreateSchema() {
		return nil
	}

	if err := createDatabaseNative(ctx, e.cfg, e.db); err != nil {
		return err
	}

	return createTracesTable(ctx, e.cfg, e.db)
}

// shutdown will shut down the exporter.
func (e *tracesExporter) shutdown(_ context.Context) error {
	if e.db == nil {
		return nil
	}

	e.resourceAttributesBufferPool.Destroy()
	e.spanAttributesBufferPool.Destroy()
	e.eventsAttributesBufferPool.Destroy()
	e.linksAttributesBufferPool.Destroy()

	return e.db.Close()
}

func (e *tracesExporter) pushTraceData(ctx context.Context, td ptrace.Traces) error {
	batch, err := e.db.PrepareBatch(ctx, e.insertSQL)
	if err != nil {
		return err
	}

	resourceAttributesBuffer := e.resourceAttributesBufferPool.Acquire()
	defer e.resourceAttributesBufferPool.Release(resourceAttributesBuffer)
	spanAttributesBuffer := e.spanAttributesBufferPool.Acquire()
	defer e.spanAttributesBufferPool.Release(spanAttributesBuffer)
	eventsAttributesBuffer := e.eventsAttributesBufferPool.Acquire()
	defer e.eventsAttributesBufferPool.Release(eventsAttributesBuffer)
	linksAttributesBuffer := e.linksAttributesBufferPool.Acquire()
	defer e.linksAttributesBufferPool.Release(linksAttributesBuffer)

	processStart := time.Now()

	var spanCount int
	rsSpans := td.ResourceSpans()
	rsLen := rsSpans.Len()
	for i := 0; i < rsLen; i++ {
		spans := rsSpans.At(i)
		res := spans.Resource()
		resAttr := res.Attributes()
		serviceName := internal.GetServiceName(resAttr)
		resourceAttributesBuffer.Reset()
		json.AttributesToJSON(resourceAttributesBuffer, resAttr)

		ssRootLen := spans.ScopeSpans().Len()
		for j := 0; j < ssRootLen; j++ {
			scopeSpanRoot := spans.ScopeSpans().At(j)
			scopeSpanScope := scopeSpanRoot.Scope()
			scopeName := scopeSpanScope.Name()
			scopeVersion := scopeSpanScope.Version()
			scopeSpans := scopeSpanRoot.Spans()

			ssLen := scopeSpans.Len()
			for k := 0; k < ssLen; k++ {
				span := scopeSpans.At(k)
				spanStatus := span.Status()
				spanDurationNanos := span.EndTimestamp() - span.StartTimestamp()

				spanAttributesBuffer.Reset()
				json.AttributesToJSON(spanAttributesBuffer, span.Attributes())

				eventTimes, eventNames, eventAttrs := convertEvents(span.Events(), eventsAttributesBuffer)
				linksTraceIDs, linksSpanIDs, linksTraceStates, linksAttrs := convertLinks(span.Links(), linksAttributesBuffer)

				batch.Append(
					span.StartTimestamp().AsTime(),
					traceutil.TraceIDToHexOrEmptyString(span.TraceID()),
					traceutil.SpanIDToHexOrEmptyString(span.SpanID()),
					traceutil.SpanIDToHexOrEmptyString(span.ParentSpanID()),
					span.TraceState().AsRaw(),
					span.Name(),
					span.Kind().String(),
					serviceName,
					resourceAttributesBuffer.Bytes(),
					scopeName,
					scopeVersion,
					spanAttributesBuffer.Bytes(),
					spanDurationNanos,
					spanStatus.Code().String(),
					spanStatus.Message(),
					eventTimes,
					eventNames,
					eventAttrs,
					linksTraceIDs,
					linksSpanIDs,
					linksTraceStates,
					linksAttrs,
				)

				spanCount++
			}
		}
	}

	processDuration := time.Since(processStart)
	networkStart := time.Now()
	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse traces insert failed: %w", err)
	}

	networkDuration := time.Since(networkStart)
	totalDuration := time.Since(processStart)
	e.logger.Debug("insert traces", zap.Int("records", spanCount),
		zap.String("process_cost", processDuration.String()),
		zap.String("network_cost", networkDuration.String()),
		zap.String("total_cost", totalDuration.String()))

	return err
}

func convertEvents(events ptrace.SpanEventSlice, attrBuffer *json.JSONBuffer) (times []time.Time, names []string, attrs []string) {
	for i := 0; i < events.Len(); i++ {
		event := events.At(i)
		times = append(times, event.Timestamp().AsTime())
		names = append(names, event.Name())

		attrBuffer.Reset()
		json.AttributesToJSON(attrBuffer, event.Attributes())
		attrs = append(attrs, string(attrBuffer.Bytes()))
	}

	return
}

func convertLinks(links ptrace.SpanLinkSlice, attrBuffer *json.JSONBuffer) (traceIDs []string, spanIDs []string, states []string, attrs []string) {
	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		traceIDs = append(traceIDs, traceutil.TraceIDToHexOrEmptyString(link.TraceID()))
		spanIDs = append(spanIDs, traceutil.SpanIDToHexOrEmptyString(link.SpanID()))
		states = append(states, link.TraceState().AsRaw())

		attrBuffer.Reset()
		json.AttributesToJSON(attrBuffer, link.Attributes())
		attrs = append(attrs, string(attrBuffer.Bytes()))
	}

	return
}

const (
	// language=ClickHouse SQL
	createTracesTableSQL = `
CREATE TABLE IF NOT EXISTS %s %s (
	Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
	TraceId String CODEC(ZSTD(1)),
	SpanId String CODEC(ZSTD(1)),
	ParentSpanId String CODEC(ZSTD(1)),
	TraceState String CODEC(ZSTD(1)),
	SpanName LowCardinality(String) CODEC(ZSTD(1)),
	SpanKind LowCardinality(String) CODEC(ZSTD(1)),
	ServiceName LowCardinality(String) CODEC(ZSTD(1)),
	ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	ScopeName String CODEC(ZSTD(1)),
	ScopeVersion String CODEC(ZSTD(1)),
	SpanAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	Duration UInt64 CODEC(ZSTD(1)),
	StatusCode LowCardinality(String) CODEC(ZSTD(1)),
	StatusMessage String CODEC(ZSTD(1)),
	Events Nested (
		Timestamp DateTime64(9),
		Name LowCardinality(String),
		Attributes Map(LowCardinality(String), String)
	) CODEC(ZSTD(1)),
	Links Nested (
		TraceId String,
		SpanId String,
		TraceState String,
		Attributes Map(LowCardinality(String), String)
	) CODEC(ZSTD(1)),
	INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
	INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_span_attr_value mapValues(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_duration Duration TYPE minmax GRANULARITY 1
) ENGINE = %s
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
%s
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
`
	// language=ClickHouse SQL
	insertTracesSQLTemplate = `INSERT INTO %s (
                        Timestamp,
                        TraceId,
                        SpanId,
                        ParentSpanId,
                        TraceState,
                        SpanName,
                        SpanKind,
                        ServiceName,
					    ResourceAttributes,
						ScopeName,
						ScopeVersion,
                        SpanAttributes,
                        Duration,
                        StatusCode,
                        StatusMessage,
                        Events.Timestamp,
                        Events.Name,
                        Events.Attributes,
                        Links.TraceId,
                        Links.SpanId,
                        Links.TraceState,
                        Links.Attributes
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

const (
	createTraceIDTsTableSQL = `
CREATE TABLE IF NOT EXISTS %s_trace_id_ts %s (
     TraceId String CODEC(ZSTD(1)),
     Start DateTime CODEC(Delta, ZSTD(1)),
     End DateTime CODEC(Delta, ZSTD(1)),
     INDEX idx_trace_id TraceId TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = %s
PARTITION BY toDate(Start)
ORDER BY (TraceId, Start)
%s
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
`
	createTraceIDTsMaterializedViewSQL = `
CREATE MATERIALIZED VIEW IF NOT EXISTS %s_trace_id_ts_mv %s
TO %s.%s_trace_id_ts
AS SELECT
	TraceId,
	min(Timestamp) as Start,
	max(Timestamp) as End
FROM
%s.%s
WHERE TraceId != ''
GROUP BY TraceId;
`
)

func createTracesTable(ctx context.Context, cfg *Config, db driver.Conn) error {
	if err := db.Exec(ctx, renderCreateTracesTableSQL(cfg)); err != nil {
		return fmt.Errorf("exec create traces table sql: %w", err)
	}
	if err := db.Exec(ctx, renderCreateTraceIDTsTableSQL(cfg)); err != nil {
		return fmt.Errorf("exec create traceID timestamp table sql: %w", err)
	}
	if err := db.Exec(ctx, renderTraceIDTsMaterializedViewSQL(cfg)); err != nil {
		return fmt.Errorf("exec create traceID timestamp view sql: %w", err)
	}

	return nil
}

func renderInsertTracesSQL(cfg *Config) string {
	return fmt.Sprintf(strings.ReplaceAll(insertTracesSQLTemplate, "'", "`"), cfg.TracesTableName)
}

func renderCreateTracesTableSQL(cfg *Config) string {
	ttlExpr := generateTTLExpr(cfg.TTL, "toDate(Timestamp)")
	return fmt.Sprintf(createTracesTableSQL, cfg.TracesTableName, cfg.clusterString(), cfg.tableEngineString(), ttlExpr)
}

func renderCreateTraceIDTsTableSQL(cfg *Config) string {
	ttlExpr := generateTTLExpr(cfg.TTL, "toDate(Start)")
	return fmt.Sprintf(createTraceIDTsTableSQL, cfg.TracesTableName, cfg.clusterString(), cfg.tableEngineString(), ttlExpr)
}

func renderTraceIDTsMaterializedViewSQL(cfg *Config) string {
	return fmt.Sprintf(createTraceIDTsMaterializedViewSQL, cfg.TracesTableName,
		cfg.clusterString(), cfg.Database, cfg.TracesTableName, cfg.Database, cfg.TracesTableName)
}
