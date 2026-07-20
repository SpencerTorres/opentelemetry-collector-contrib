CREATE MATERIALIZED VIEW IF NOT EXISTS {{ident .Database}}.{{ident .ViewName}} {{.ClusterString}}
TO {{ident .Database}}.{{ident .DestTableName}} AS
SELECT
    MetricName,
    SeriesHash,
    toStartOfHour(TimeBucket) AS TimeBucket,
    Scale,
    argMinMergeState(First) AS First,
    argMaxMergeState(Last) AS Last,
    sum(SumCount) AS SumCount,
    sum(SumSum) AS SumSum,
    sum(SumZeroCount) AS SumZeroCount,
    sumMap(SumPositive) AS SumPositive,
    sumMap(SumNegative) AS SumNegative,
    min(Min) AS Min,
    max(Max) AS Max,
    max(ZeroThreshold) AS ZeroThreshold,
    sum(PointCount) AS PointCount
FROM {{ident .Database}}.{{ident .SourceTableName}}
GROUP BY MetricName, SeriesHash, TimeBucket, Scale
