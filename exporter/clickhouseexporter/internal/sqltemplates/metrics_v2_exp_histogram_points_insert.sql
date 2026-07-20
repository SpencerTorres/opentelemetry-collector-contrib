INSERT INTO {{ident .Database}}.{{ident .TableName}} (
    MetricName,
    SeriesHash,
    StartTimeUnix,
    TimeUnix,
    Count,
    Sum,
    Min,
    Max,
    Scale,
    ZeroCount,
    ZeroThreshold,
    PositiveOffset,
    PositiveBucketCounts,
    NegativeOffset,
    NegativeBucketCounts,
    Flags
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
