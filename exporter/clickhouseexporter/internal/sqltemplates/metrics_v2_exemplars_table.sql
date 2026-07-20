CREATE TABLE IF NOT EXISTS {{ident .Database}}.{{ident .TableName}} {{.ClusterString}} (
    `MetricName` LowCardinality(String) COMMENT 'OTel metric name' CODEC(ZSTD(1)),
    `SeriesHash` UInt64 COMMENT 'Canonical series fingerprint; join key to the series table' CODEC(ZSTD(1)),
    `TimeUnix` DateTime64(3) COMMENT 'Exemplar timestamp (millisecond precision)' CODEC(Delta(8), ZSTD(1)),
    `Value` Float64 COMMENT 'Exemplar value' CODEC(ZSTD(1)),
    `TraceId` String COMMENT 'Hex-encoded trace ID of the exemplar' CODEC(ZSTD(1)),
    `SpanId` String COMMENT 'Hex-encoded span ID of the exemplar' CODEC(ZSTD(1)),
    `FilteredAttributes` Map(LowCardinality(String), String) COMMENT 'Attributes removed from the data point by aggregation, key-sorted' CODEC(ZSTD(1)),
    INDEX idx_time_minmax TimeUnix TYPE minmax GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY toDate(TimeUnix)
ORDER BY (MetricName, SeriesHash, TimeUnix)
{{.TTL}}
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, non_replicated_deduplication_window = 512
