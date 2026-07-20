INSERT INTO {{ident .Database}}.{{ident .TableName}} (
    MetricName,
    SeriesHash,
    StartTimeUnix,
    TimeUnix,
    Count,
    Sum,
    Min,
    Max,
    BucketCounts,
    Flags
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
