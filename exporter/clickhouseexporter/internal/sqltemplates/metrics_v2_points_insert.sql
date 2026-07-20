INSERT INTO {{ident .Database}}.{{ident .TableName}} (
    MetricName,
    SeriesHash,
    StartTimeUnix,
    TimeUnix,
    Value,
    Flags
) VALUES (
    ?, ?, ?, ?, ?, ?
)
