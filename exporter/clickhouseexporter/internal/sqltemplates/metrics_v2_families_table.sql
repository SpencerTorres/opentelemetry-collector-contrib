CREATE TABLE IF NOT EXISTS {{ident .Database}}.{{ident .TableName}} {{.ClusterString}} (
    `MetricName` LowCardinality(String) COMMENT 'OTel metric name' CODEC(ZSTD(1)),
    `MetricType` LowCardinality(String) COMMENT 'OTel metric type: gauge, sum, histogram, exponential_histogram, summary' CODEC(ZSTD(1)),
    `Unit` LowCardinality(String) COMMENT 'Metric unit' CODEC(ZSTD(1)),
    `Description` String COMMENT 'Metric description' CODEC(ZSTD(1))
) ENGINE = ReplacingMergeTree
ORDER BY (MetricName, MetricType, Unit)
SETTINGS index_granularity = 8192
