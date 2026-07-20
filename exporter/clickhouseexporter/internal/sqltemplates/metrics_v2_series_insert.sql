INSERT INTO {{ident .Database}}.{{ident .TableName}} (
    Date,
    MetricName,
    SeriesHash,
    ServiceName,
    MetricType,
    Temporality,
    IsMonotonic,
    Unit,
    ScopeName,
    ScopeVersion,
    ResourceSchemaUrl,
    ScopeSchemaUrl,
    ResourceAttributes,
    ScopeAttributes,
    Attributes,
    ExplicitBounds,
    Quantiles,
    FirstSeen,
    LastSeen
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
