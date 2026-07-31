CREATE TABLE IF NOT EXISTS {{ident .Database}}.{{ident .TableName}} {{.ClusterString}} (
    `MetricName` LowCardinality(String) COMMENT 'OTel metric name' CODEC(ZSTD(1)),
    `SeriesHash` UInt64 COMMENT 'Canonical series fingerprint; join key to the series table' CODEC(ZSTD(1)),
    `TimeBucket` DateTime COMMENT '1-hour bucket start' CODEC(DoubleDelta, ZSTD(1)),
    `Scale` Int8 COMMENT 'Exponential histogram scale; part of the aggregation key so states only ever merge within one scale (exact by construction). A series that changes scale inside a bucket yields one row per scale; the query layer downscale-merges those rows to the minimum scale' CODEC(ZSTD(1)),
    `First` AggregateFunction(argMin, Tuple(Time DateTime64(3), Count UInt64, Sum Float64, ZeroCount UInt64, PositiveBuckets Map(Int32, UInt64), NegativeBuckets Map(Int32, UInt64)), DateTime64(3)) COMMENT 'Earliest sample in the bucket as one consistent tuple (timestamp, count, sum, zero count, bucket maps); with Last, enables reset-aware chaining for cumulative temporality. Bucket maps are keyed by absolute bucket index (offset + i), so they stay comparable under offset drift' CODEC(ZSTD(1)),
    `Last` AggregateFunction(argMax, Tuple(Time DateTime64(3), Count UInt64, Sum Float64, ZeroCount UInt64, PositiveBuckets Map(Int32, UInt64), NegativeBuckets Map(Int32, UInt64)), DateTime64(3)) COMMENT 'Latest sample in the bucket, same tuple shape as First' CODEC(ZSTD(1)),
    `SumCount` SimpleAggregateFunction(sum, UInt64) COMMENT 'Sum of per-point Count; the exact window observation count for delta temporality' CODEC(ZSTD(1)),
    `SumSum` SimpleAggregateFunction(sum, Float64) COMMENT 'Sum of per-point Sum; the exact window observation sum for delta temporality' CODEC(ZSTD(1)),
    `SumZeroCount` SimpleAggregateFunction(sum, UInt64) COMMENT 'Sum of per-point ZeroCount; the exact zero-bucket window increase for delta temporality' CODEC(ZSTD(1)),
    `SumPositive` SimpleAggregateFunction(sumMap, Map(Int32, UInt64)) COMMENT 'Additive per-bucket sums keyed by absolute positive bucket index (PositiveOffset + i); the exact per-bucket window increase for delta temporality, immune to per-point offset drift. Query via sumMap()' CODEC(ZSTD(1)),
    `SumNegative` SimpleAggregateFunction(sumMap, Map(Int32, UInt64)) COMMENT 'Additive per-bucket sums keyed by absolute negative bucket index (NegativeOffset + i)' CODEC(ZSTD(1)),
    `Min` SimpleAggregateFunction(min, Float64) COMMENT 'Minimum observed value in the bucket (0 when not set on raw points)' CODEC(ZSTD(1)),
    `Max` SimpleAggregateFunction(max, Float64) COMMENT 'Maximum observed value in the bucket (0 when not set on raw points)' CODEC(ZSTD(1)),
    `ZeroThreshold` SimpleAggregateFunction(max, Float64) COMMENT 'Widest zero-bucket threshold seen in the bucket (effectively constant per series; max is the conservative merge, matching OTel zero-bucket widening semantics)' CODEC(ZSTD(1)),
    `PointCount` SimpleAggregateFunction(sum, UInt64) COMMENT 'Number of raw exponential histogram points in the bucket' CODEC(ZSTD(1))
) ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(TimeBucket)
ORDER BY (MetricName, SeriesHash, TimeBucket, Scale)
{{.TTL}}
SETTINGS index_granularity = 8192, non_replicated_deduplication_window = 8192, replicated_deduplication_window = 8192
