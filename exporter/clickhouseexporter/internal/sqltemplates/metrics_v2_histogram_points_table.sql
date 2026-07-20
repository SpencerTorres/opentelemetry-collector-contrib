CREATE TABLE IF NOT EXISTS {{ident .Database}}.{{ident .TableName}} {{.ClusterString}} (
    `MetricName` LowCardinality(String) COMMENT 'OTel metric name' CODEC(ZSTD(1)),
    `SeriesHash` UInt64 COMMENT 'Canonical series fingerprint; join key to the series table' CODEC(ZSTD(1)),
    `StartTimeUnix` DateTime64(3) COMMENT 'Start timestamp of the aggregation window' CODEC(Delta(8), ZSTD(1)),
    `TimeUnix` DateTime64(3) COMMENT 'Data point timestamp (millisecond precision)' CODEC(DoubleDelta, ZSTD(1)),
    `Count` UInt64 COMMENT 'Total observation count' CODEC(Delta(8), ZSTD(1)),
    `Sum` Float64 COMMENT 'Sum of observed values' CODEC(ZSTD(1)),
    `Min` Float64 COMMENT 'Minimum observed value (0 when not set)' CODEC(ZSTD(1)),
    `Max` Float64 COMMENT 'Maximum observed value (0 when not set)' CODEC(ZSTD(1)),
    `BucketCounts` Array(UInt64) COMMENT 'Per-bucket observation counts; bucket bounds are stored once on the series table (ExplicitBounds)' CODEC(ZSTD(1)),
    `Flags` UInt8 COMMENT 'OTel data point flags' CODEC(ZSTD(1)),
    INDEX idx_time_minmax TimeUnix TYPE minmax GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY toDate(TimeUnix)
ORDER BY (MetricName, SeriesHash, TimeUnix)
{{.TTL}}
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, non_replicated_deduplication_window = 512
