CREATE TABLE IF NOT EXISTS {{ident .Database}}.{{ident .TableName}} {{.ClusterString}} (
    `MetricName` LowCardinality(String) COMMENT 'OTel metric name' CODEC(ZSTD(1)),
    `SeriesHash` UInt64 COMMENT 'Canonical series fingerprint; join key to the series table' CODEC(ZSTD(1)),
    `TimeBucket` DateTime COMMENT '1-hour bucket start' CODEC(DoubleDelta, ZSTD(1)),
    `FirstCount` AggregateFunction(argMin, UInt64, DateTime64(3)) COMMENT 'Earliest observation count in the bucket; with LastCount, enables reset-aware counter increase across buckets for cumulative temporality' CODEC(ZSTD(1)),
    `LastCount` AggregateFunction(argMax, UInt64, DateTime64(3)) COMMENT 'Latest observation count in the bucket' CODEC(ZSTD(1)),
    `FirstSum` AggregateFunction(argMin, Float64, DateTime64(3)) COMMENT 'Earliest sum of observed values in the bucket' CODEC(ZSTD(1)),
    `LastSum` AggregateFunction(argMax, Float64, DateTime64(3)) COMMENT 'Latest sum of observed values in the bucket' CODEC(ZSTD(1)),
    `SumCount` SimpleAggregateFunction(sum, UInt64) COMMENT 'Sum of per-point Count; the exact window observation count for delta temporality' CODEC(ZSTD(1)),
    `SumSum` SimpleAggregateFunction(sum, Float64) COMMENT 'Sum of per-point Sum; the exact window observation sum for delta temporality' CODEC(ZSTD(1)),
    `Min` SimpleAggregateFunction(min, Float64) COMMENT 'Minimum observed value in the bucket (0 when not set on raw points)' CODEC(ZSTD(1)),
    `Max` SimpleAggregateFunction(max, Float64) COMMENT 'Maximum observed value in the bucket (0 when not set on raw points)' CODEC(ZSTD(1)),
    `FirstBuckets` AggregateFunction(argMin, Array(UInt64), DateTime64(3)) COMMENT 'Earliest per-bucket counts in the bucket; with LastBuckets, enables per-le counter chaining for cumulative temporality. Bucket bounds live on the series table (ExplicitBounds), positionally aligned' CODEC(ZSTD(1)),
    `LastBuckets` AggregateFunction(argMax, Array(UInt64), DateTime64(3)) COMMENT 'Latest per-bucket counts in the bucket' CODEC(ZSTD(1)),
    `SumBuckets` AggregateFunction(sumForEach, Array(UInt64)) COMMENT 'Element-wise sum of per-bucket counts; the exact per-bucket window increase for delta temporality. Query via sumForEachMerge' CODEC(ZSTD(1)),
    `PointCount` SimpleAggregateFunction(sum, UInt64) COMMENT 'Number of raw histogram points in the bucket' CODEC(ZSTD(1))
) ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(TimeBucket)
ORDER BY (MetricName, SeriesHash, TimeBucket)
{{.TTL}}
SETTINGS index_granularity = 8192, non_replicated_deduplication_window = 8192, replicated_deduplication_window = 8192
