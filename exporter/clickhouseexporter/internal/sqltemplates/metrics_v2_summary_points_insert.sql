INSERT INTO {{ident .Database}}.{{ident .TableName}} (
    MetricName,
    SeriesHash,
    StartTimeUnix,
    TimeUnix,
    Count,
    Sum,
    QuantileValues,
    Flags
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?
)
