-- Default Trace table DDL

CREATE TABLE IF NOT EXISTS otel_traces (
    Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
    TraceId String CODEC(ZSTD(1)),
    SpanId String CODEC(ZSTD(1)),
    ParentSpanId String CODEC(ZSTD(1)),
    TraceState String CODEC(ZSTD(1)),
    SpanName LowCardinality(String) CODEC(ZSTD(1)),
    SpanKind LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes JSON,
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    SpanAttributes JSON,
    Duration UInt64 CODEC(ZSTD(1)),
    StatusCode LowCardinality(String) CODEC(ZSTD(1)),
    StatusMessage String CODEC(ZSTD(1)),
    Events Nested (
        Timestamp DateTime64(9),
        Name LowCardinality(String),
        Attributes JSON
    ) CODEC(ZSTD(1)),
    Links Nested (
        TraceId String,
        SpanId String,
        TraceState String,
        Attributes JSON
    ) CODEC(ZSTD(1)),
    ) ENGINE = MergeTree()
    PARTITION BY toMonth(Timestamp)
    PRIMARY KEY (ServiceName, SpanName, toDateTime(Timestamp))
    ORDER BY (ServiceName, SpanName, toDateTime(Timestamp), Timestamp)
    SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;



CREATE TABLE IF NOT EXISTS otel_traces_trace_id_ts (
     TraceId String CODEC(ZSTD(1)),
     Start DateTime CODEC(Delta, ZSTD(1)),
     End DateTime CODEC(Delta, ZSTD(1)),
     INDEX idx_trace_id TraceId TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY toDate(Start)
ORDER BY (TraceId, Start)
TTL toDate(Start) + toIntervalDay(180)
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;


CREATE MATERIALIZED VIEW IF NOT EXISTS otel_traces_trace_id_ts_mv
TO otel_traces_trace_id_ts
AS SELECT
    TraceId,
    min(Timestamp) as Start,
    max(Timestamp) as End
FROM otel_traces
WHERE TraceId != ''
GROUP BY TraceId;
