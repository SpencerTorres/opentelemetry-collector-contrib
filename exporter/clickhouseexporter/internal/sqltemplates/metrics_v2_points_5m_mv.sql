CREATE MATERIALIZED VIEW IF NOT EXISTS {{ident .Database}}.{{ident .ViewName}} {{.ClusterString}}
TO {{ident .Database}}.{{ident .DestTableName}} AS
SELECT
    MetricName,
    SeriesHash,
    toStartOfFiveMinutes(TimeUnix) AS TimeBucket,
    argMinState(Value, TimeUnix) AS First,
    argMaxState(Value, TimeUnix) AS Last,
    min(Value) AS Min,
    max(Value) AS Max,
    sum(Value) AS Sum,
    toUInt64(count()) AS Count,
    quantileBFloat16State(Value) AS ValueSketch
FROM {{ident .Database}}.{{ident .SourceTableName}}
WHERE bitAnd(Flags, 1) = 0
GROUP BY MetricName, SeriesHash, TimeBucket
