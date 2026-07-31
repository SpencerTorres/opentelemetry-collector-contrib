CREATE TABLE IF NOT EXISTS {{ident .Database}}.{{ident .TableName}} {{.ClusterString}} (
    `MetricName` LowCardinality(String) COMMENT 'OTel metric name' CODEC(ZSTD(1)),
    `SeriesHash` UInt64 COMMENT 'Canonical series fingerprint; join key to the series table' CODEC(ZSTD(1)),
    `StartTimeUnix` DateTime64(3) COMMENT 'Start timestamp of the aggregation window' CODEC(Delta(8), ZSTD(1)),
    `TimeUnix` DateTime64(3) COMMENT 'Data point timestamp (millisecond precision)' CODEC(DoubleDelta, ZSTD(1)),
    `Count` UInt64 COMMENT 'Total observation count' CODEC(Delta(8), ZSTD(1)),
    `Sum` Float64 COMMENT 'Sum of observed values' CODEC(ZSTD(1)),
    `Min` Float64 COMMENT 'Minimum observed value (0 when not set)' CODEC(ZSTD(1)),
    `Max` Float64 COMMENT 'Maximum observed value (0 when not set)' CODEC(ZSTD(1)),
    `Scale` Int8 COMMENT 'Exponential histogram scale; base = 2^(2^-scale). The OTel spec bounds scale to [-10, 20]' CODEC(ZSTD(1)),
    `ZeroCount` UInt64 COMMENT 'Count of values in the zero bucket' CODEC(ZSTD(1)),
    `ZeroThreshold` Float64 COMMENT 'Width of the zero bucket' CODEC(ZSTD(1)),
    `PositiveOffset` Int32 COMMENT 'Bucket index offset of the first positive bucket' CODEC(ZSTD(1)),
    `PositiveBucketCounts` Array(UInt64) COMMENT 'Positive bucket counts' CODEC(ZSTD(1)),
    `NegativeOffset` Int32 COMMENT 'Bucket index offset of the first negative bucket' CODEC(ZSTD(1)),
    `NegativeBucketCounts` Array(UInt64) COMMENT 'Negative bucket counts' CODEC(ZSTD(1)),
    `Flags` UInt8 COMMENT 'OTel data point flags' CODEC(ZSTD(1)),
    INDEX idx_time_minmax TimeUnix TYPE minmax GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY toDate(TimeUnix)
ORDER BY (MetricName, SeriesHash, TimeUnix)
{{.TTL}}
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, non_replicated_deduplication_window = 8192, replicated_deduplication_window = 8192
