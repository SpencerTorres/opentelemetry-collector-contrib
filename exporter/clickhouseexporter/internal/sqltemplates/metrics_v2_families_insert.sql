INSERT INTO {{ident .Database}}.{{ident .TableName}} (
    MetricName,
    MetricType,
    Unit,
    Description
) VALUES (
    ?, ?, ?, ?
)
