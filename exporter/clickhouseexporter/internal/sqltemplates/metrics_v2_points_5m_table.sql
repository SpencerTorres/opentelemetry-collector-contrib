CREATE TABLE IF NOT EXISTS {{ident .Database}}.{{ident .TableName}} {{.ClusterString}} (
    `MetricName` LowCardinality(String) COMMENT 'OTel metric name' CODEC(ZSTD(1)),
    `SeriesHash` UInt64 COMMENT 'Canonical series fingerprint; join key to the series table' CODEC(ZSTD(1)),
    `TimeBucket` DateTime COMMENT '5-minute bucket start' CODEC(DoubleDelta, ZSTD(1)),
    `First` AggregateFunction(argMin, Float64, DateTime64(3)) COMMENT 'Earliest value in the bucket; with Last, enables reset-aware counter increase across buckets' CODEC(ZSTD(1)),
    `Last` AggregateFunction(argMax, Float64, DateTime64(3)) COMMENT 'Latest value in the bucket' CODEC(ZSTD(1)),
    `Min` SimpleAggregateFunction(min, Float64) COMMENT 'Minimum value in the bucket' CODEC(ZSTD(1)),
    `Max` SimpleAggregateFunction(max, Float64) COMMENT 'Maximum value in the bucket' CODEC(ZSTD(1)),
    `Sum` SimpleAggregateFunction(sum, Float64) COMMENT 'Sum of values in the bucket' CODEC(ZSTD(1)),
    `Count` SimpleAggregateFunction(sum, UInt64) COMMENT 'Number of points in the bucket' CODEC(ZSTD(1)),
    `ValueSketch` AggregateFunction(quantileBFloat16, Float64) COMMENT 'Mergeable value distribution sketch; query any level via quantileBFloat16Merge(level)' CODEC(ZSTD(1))
) ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(TimeBucket)
ORDER BY (MetricName, SeriesHash, TimeBucket)
{{.TTL}}
SETTINGS index_granularity = 8192, non_replicated_deduplication_window = 8192, replicated_deduplication_window = 8192
