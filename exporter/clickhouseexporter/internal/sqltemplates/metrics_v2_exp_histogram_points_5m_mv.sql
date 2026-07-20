CREATE MATERIALIZED VIEW IF NOT EXISTS {{ident .Database}}.{{ident .ViewName}} {{.ClusterString}}
TO {{ident .Database}}.{{ident .DestTableName}} AS
WITH
    mapFilter((k, v) -> v != 0, mapFromArrays(
        arrayMap(i -> toInt32(PositiveOffset + toInt32(i) - 1), arrayEnumerate(PositiveBucketCounts)),
        PositiveBucketCounts)) AS PositiveMap,
    mapFilter((k, v) -> v != 0, mapFromArrays(
        arrayMap(i -> toInt32(NegativeOffset + toInt32(i) - 1), arrayEnumerate(NegativeBucketCounts)),
        NegativeBucketCounts)) AS NegativeMap
SELECT
    MetricName,
    SeriesHash,
    toStartOfFiveMinutes(TimeUnix) AS TimeBucket,
    Scale,
    argMinState(CAST((TimeUnix, Count, Sum, ZeroCount, PositiveMap, NegativeMap),
        'Tuple(Time DateTime64(3), Count UInt64, Sum Float64, ZeroCount UInt64, PositiveBuckets Map(Int32, UInt64), NegativeBuckets Map(Int32, UInt64))'), TimeUnix) AS First,
    argMaxState(CAST((TimeUnix, Count, Sum, ZeroCount, PositiveMap, NegativeMap),
        'Tuple(Time DateTime64(3), Count UInt64, Sum Float64, ZeroCount UInt64, PositiveBuckets Map(Int32, UInt64), NegativeBuckets Map(Int32, UInt64))'), TimeUnix) AS Last,
    sum(Count) AS SumCount,
    sum(Sum) AS SumSum,
    sum(ZeroCount) AS SumZeroCount,
    sumMap(PositiveMap) AS SumPositive,
    sumMap(NegativeMap) AS SumNegative,
    min(Min) AS Min,
    max(Max) AS Max,
    max(ZeroThreshold) AS ZeroThreshold,
    toUInt64(count()) AS PointCount
FROM {{ident .Database}}.{{ident .SourceTableName}}
WHERE bitAnd(Flags, 1) = 0
GROUP BY MetricName, SeriesHash, TimeBucket, Scale
