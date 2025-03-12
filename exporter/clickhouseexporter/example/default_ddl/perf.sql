CREATE TABLE batch_perf
(
    Version String,
    Timestamp DateTime64(3),
    Count UInt64,
    Process UInt64,
    Network UInt64
)
ENGINE = MergeTree
ORDER BY (Version, Timestamp)
SETTINGS index_granularity = 8192