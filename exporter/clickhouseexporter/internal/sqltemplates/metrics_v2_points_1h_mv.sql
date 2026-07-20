CREATE MATERIALIZED VIEW IF NOT EXISTS {{ident .Database}}.{{ident .ViewName}} {{.ClusterString}}
TO {{ident .Database}}.{{ident .DestTableName}} AS
SELECT
    MetricName,
    SeriesHash,
    toStartOfHour(TimeBucket) AS TimeBucket,
    argMinMergeState(First) AS First,
    argMaxMergeState(Last) AS Last,
    min(Min) AS Min,
    max(Max) AS Max,
    sum(Sum) AS Sum,
    sum(Count) AS Count,
    quantileBFloat16MergeState(ValueSketch) AS ValueSketch
FROM {{ident .Database}}.{{ident .SourceTableName}}
GROUP BY MetricName, SeriesHash, TimeBucket
