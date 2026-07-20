CREATE MATERIALIZED VIEW IF NOT EXISTS {{ident .Database}}.{{ident .ViewName}} {{.ClusterString}}
TO {{ident .Database}}.{{ident .DestTableName}} AS
SELECT
    MetricName,
    SeriesHash,
    toStartOfFiveMinutes(TimeUnix) AS TimeBucket,
    argMinState(Count, TimeUnix) AS FirstCount,
    argMaxState(Count, TimeUnix) AS LastCount,
    argMinState(Sum, TimeUnix) AS FirstSum,
    argMaxState(Sum, TimeUnix) AS LastSum,
    sum(Count) AS SumCount,
    sum(Sum) AS SumSum,
    min(Min) AS Min,
    max(Max) AS Max,
    argMinState(BucketCounts, TimeUnix) AS FirstBuckets,
    argMaxState(BucketCounts, TimeUnix) AS LastBuckets,
    sumForEachState(BucketCounts) AS SumBuckets,
    toUInt64(count()) AS PointCount
FROM {{ident .Database}}.{{ident .SourceTableName}}
WHERE bitAnd(Flags, 1) = 0
GROUP BY MetricName, SeriesHash, TimeBucket
