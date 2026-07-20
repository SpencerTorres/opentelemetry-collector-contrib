CREATE MATERIALIZED VIEW IF NOT EXISTS {{ident .Database}}.{{ident .ViewName}} {{.ClusterString}}
TO {{ident .Database}}.{{ident .DestTableName}} AS
SELECT
    MetricName,
    SeriesHash,
    toStartOfHour(TimeBucket) AS TimeBucket,
    argMinMergeState(FirstCount) AS FirstCount,
    argMaxMergeState(LastCount) AS LastCount,
    argMinMergeState(FirstSum) AS FirstSum,
    argMaxMergeState(LastSum) AS LastSum,
    sum(SumCount) AS SumCount,
    sum(SumSum) AS SumSum,
    min(Min) AS Min,
    max(Max) AS Max,
    argMinMergeState(FirstBuckets) AS FirstBuckets,
    argMaxMergeState(LastBuckets) AS LastBuckets,
    sumForEachMergeState(SumBuckets) AS SumBuckets,
    sum(PointCount) AS PointCount
FROM {{ident .Database}}.{{ident .SourceTableName}}
GROUP BY MetricName, SeriesHash, TimeBucket
