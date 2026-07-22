CREATE TABLE IF NOT EXISTS {{ident .Database}}.{{ident .TableName}} {{.ClusterString}} (
    `Date` Date COMMENT 'Day bucket for series liveness; a series row exists for each day the series reported' CODEC(Delta(2), ZSTD(1)),
    `MetricName` LowCardinality(String) COMMENT 'OTel metric name' CODEC(ZSTD(1)),
    `SeriesHash` UInt64 COMMENT 'Canonical series fingerprint: cityHash64 over the sorted label serialization (see internal/metricsv2/hash.go)' CODEC(ZSTD(1)),
    `ServiceName` LowCardinality(String) COMMENT 'Value of the service.name resource attribute' CODEC(ZSTD(1)),
    `MetricType` LowCardinality(String) COMMENT 'OTel metric type: gauge, sum, histogram, exponential_histogram, summary' CODEC(ZSTD(1)),
    `Temporality` LowCardinality(String) COMMENT 'Aggregation temporality: unspecified, delta, cumulative' CODEC(ZSTD(1)),
    `IsMonotonic` Bool COMMENT 'True for monotonic sums',
    `Unit` LowCardinality(String) COMMENT 'Metric unit' CODEC(ZSTD(1)),
    `ScopeName` String COMMENT 'Instrumentation scope name' CODEC(ZSTD(1)),
    `ScopeVersion` LowCardinality(String) COMMENT 'Instrumentation scope version' CODEC(ZSTD(1)),
    `ResourceSchemaUrl` LowCardinality(String) COMMENT 'Schema URL for the resource' CODEC(ZSTD(1)),
    `ScopeSchemaUrl` LowCardinality(String) COMMENT 'Schema URL for the instrumentation scope' CODEC(ZSTD(1)),
    `ResourceAttributes` Map(LowCardinality(String), String) COMMENT 'Resource attributes, key-sorted' CODEC(ZSTD(1)),
    `ScopeAttributes` Map(LowCardinality(String), String) COMMENT 'Instrumentation scope attributes, key-sorted' CODEC(ZSTD(1)),
    `Attributes` Map(LowCardinality(String), String) COMMENT 'Data point attributes, key-sorted' CODEC(ZSTD(1)),
    `ExplicitBounds` Array(Float64) COMMENT 'Histogram bucket bounds (histogram series only); bounds are part of the series identity' CODEC(ZSTD(1)),
    `Quantiles` Array(Float64) COMMENT 'Summary quantile levels (summary series only); quantiles are part of the series identity' CODEC(ZSTD(1)),
    `FirstSeen` SimpleAggregateFunction(min, DateTime64(3)) COMMENT 'Earliest data point timestamp observed for this series on this day' CODEC(ZSTD(1)),
    `LastSeen` SimpleAggregateFunction(max, DateTime64(3)) COMMENT 'Latest data point timestamp observed when a series row was written' CODEC(ZSTD(1)),
    `ResourceAttributeItems` Array(String) ALIAS arrayMap((arr) -> concat(arr.1, '=', arr.2), ResourceAttributes::Array(Tuple(String, String))),
    `ScopeAttributeItems` Array(String) ALIAS arrayMap((arr) -> concat(arr.1, '=', arr.2), ScopeAttributes::Array(Tuple(String, String))),
    `AttributeItems` Array(String) ALIAS arrayMap((arr) -> concat(arr.1, '=', arr.2), Attributes::Array(Tuple(String, String))),
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE text(tokenizer = 'array'),
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE text(tokenizer = 'array'),
    INDEX idx_res_attr_items ResourceAttributeItems TYPE text(tokenizer = 'array'),
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE text(tokenizer = 'array'),
    INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE text(tokenizer = 'array'),
    INDEX idx_scope_attr_items ScopeAttributeItems TYPE text(tokenizer = 'array'),
    INDEX idx_attr_key mapKeys(Attributes) TYPE text(tokenizer = 'array'),
    INDEX idx_attr_value mapValues(Attributes) TYPE text(tokenizer = 'array'),
    INDEX idx_attr_items AttributeItems TYPE text(tokenizer = 'array')
) ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(Date)
ORDER BY (Date, MetricName, SeriesHash)
{{.TTL}}
SETTINGS index_granularity = 8192
