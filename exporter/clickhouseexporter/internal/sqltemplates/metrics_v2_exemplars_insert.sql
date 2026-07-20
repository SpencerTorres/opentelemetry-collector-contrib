INSERT INTO {{ident .Database}}.{{ident .TableName}} (
    MetricName,
    SeriesHash,
    TimeUnix,
    Value,
    TraceId,
    SpanId,
    FilteredAttributes
) VALUES (
    ?, ?, ?, ?, ?, ?, ?
)
