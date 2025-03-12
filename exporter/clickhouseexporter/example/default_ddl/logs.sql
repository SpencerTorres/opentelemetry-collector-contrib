CREATE TABLE IF NOT EXISTS otel_logs (
	Timestamp DateTime64(9) CODEC(Delta(8), ZSTD(1)),
	TraceId String CODEC(ZSTD(1)),
	SpanId String CODEC(ZSTD(1)),
	TraceFlags UInt8,
	SeverityText LowCardinality(String) CODEC(ZSTD(1)),
	SeverityNumber UInt8,
	ServiceName LowCardinality(String) CODEC(ZSTD(1)),
	Body String CODEC(ZSTD(1)),
	ResourceSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
	ResourceAttributes JSON,
	ScopeSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
	ScopeName String CODEC(ZSTD(1)),
	ScopeVersion LowCardinality(String) CODEC(ZSTD(1)),
	ScopeAttributes JSON,
	LogAttributes JSON,
) ENGINE = MergeTree()
PARTITION BY toMonth(Timestamp)
PRIMARY KEY (ServiceName, toDateTime(Timestamp))
ORDER BY (ServiceName, toDateTime(Timestamp), Timestamp)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;