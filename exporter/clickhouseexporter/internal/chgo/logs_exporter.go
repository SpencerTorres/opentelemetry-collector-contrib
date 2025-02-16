package chgo

import (
	"context"
	"fmt"
	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/traceutil"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"time"
)

type LogsExporter struct {
	cfg    *ChConfig
	logger *zap.Logger

	db *ch.Client

	maxBatchSize int
	columns      *logColumns
	insertSQL    string
	insertInput  proto.Input

	resourceAttributesJSONBuffer *JSONBuffer
	scopeAttributesJSONBuffer    *JSONBuffer
	logAttributesJSONBuffer      *JSONBuffer
}

type logColumns struct {
	timestamp          proto.ColDateTime64Raw
	traceID            proto.ColStr
	spanID             proto.ColStr
	traceFlags         proto.ColUInt8
	severityText       *proto.ColLowCardinality[string]
	severityNumber     proto.ColUInt8
	serviceName        *proto.ColLowCardinality[string]
	body               proto.ColStr
	resourceSchemaUrl  *proto.ColLowCardinality[string]
	resourceAttributes proto.ColJSONBytes
	scopeSchemaUrl     *proto.ColLowCardinality[string]
	scopeName          proto.ColStr
	scopeVersion       *proto.ColLowCardinality[string]
	scopeAttributes    proto.ColJSONBytes
	logAttributes      proto.ColJSONBytes
}

func NewLogsExporter(cfg *ChConfig, logger *zap.Logger) (*LogsExporter, error) {
	return &LogsExporter{
		cfg:          cfg,
		logger:       logger.Named("clickhouse"),
		maxBatchSize: 8192,
		insertSQL:    fmt.Sprintf(`INSERT INTO "%s"."%s" VALUES`, cfg.Database, cfg.Table),
	}, nil
}

func (e *LogsExporter) Start(ctx context.Context, _ component.Host) error {
	err := connectDB(ctx, &e.db, e.cfg)
	if err != nil {
		_ = closeDB(&e.db)
		e.logger.Error(fmt.Sprintf("initial connection failed: %s", err))
	}

	jsonSize := 512
	strSize := 64
	bSize := e.maxBatchSize

	cols := &logColumns{
		timestamp:          newColDateTime64Raw(bSize),
		traceID:            newColString(strSize, bSize),
		spanID:             newColString(strSize, bSize),
		traceFlags:         make(proto.ColUInt8, 0, bSize),
		severityText:       newColLowCardinalityString(strSize, bSize),
		severityNumber:     make(proto.ColUInt8, 0, bSize),
		serviceName:        newColLowCardinalityString(strSize, bSize),
		body:               newColString(strSize, bSize),
		resourceSchemaUrl:  newColLowCardinalityString(strSize, bSize),
		resourceAttributes: newColJSONBytes(jsonSize, strSize),
		scopeSchemaUrl:     newColLowCardinalityString(strSize, bSize),
		scopeName:          newColString(strSize, bSize),
		scopeVersion:       newColLowCardinalityString(strSize, bSize),
		scopeAttributes:    newColJSONBytes(jsonSize, strSize),
		logAttributes:      newColJSONBytes(jsonSize, strSize),
	}
	e.columns = cols

	e.insertInput = proto.Input{
		{Name: "Timestamp", Data: &cols.timestamp},
		{Name: "TraceId", Data: &cols.traceID},
		{Name: "SpanId", Data: &cols.spanID},
		{Name: "TraceFlags", Data: &cols.traceFlags},
		{Name: "SeverityText", Data: cols.severityText},
		{Name: "SeverityNumber", Data: &cols.severityNumber},
		{Name: "ServiceName", Data: cols.serviceName},
		{Name: "Body", Data: &cols.body},
		{Name: "ResourceSchemaUrl", Data: cols.resourceSchemaUrl},
		{Name: "ResourceAttributes", Data: &cols.resourceAttributes},
		{Name: "ScopeSchemaUrl", Data: cols.scopeSchemaUrl},
		{Name: "ScopeName", Data: &cols.scopeName},
		{Name: "ScopeVersion", Data: cols.scopeVersion},
		{Name: "ScopeAttributes", Data: &cols.scopeAttributes},
		{Name: "LogAttributes", Data: &cols.logAttributes},
	}

	e.resourceAttributesJSONBuffer = newJSONBuffer(jsonSize, strSize)
	e.scopeAttributesJSONBuffer = newJSONBuffer(jsonSize, strSize)
	e.logAttributesJSONBuffer = newJSONBuffer(jsonSize, strSize)

	return nil
}

func (e *LogsExporter) Shutdown(_ context.Context) error {
	return closeDB(&e.db)
}

func (e *LogsExporter) PushLogsData(ctx context.Context, ld plog.Logs) error {
	if e.db == nil {
		if err := connectDB(ctx, &e.db, e.cfg); err != nil {
			return err
		}
	}

	cols := e.columns
	e.insertInput.Reset()

	start := time.Now()

	var logCount int
	rsLogs := ld.ResourceLogs()
	rsLen := rsLogs.Len()
	for i := 0; i < rsLen; i++ {
		logs := rsLogs.At(i)
		res := logs.Resource()
		resURL := logs.SchemaUrl()
		resAttr := res.Attributes()
		serviceName := internal.GetServiceName(resAttr)
		e.resourceAttributesJSONBuffer.Reset()
		attributesToJSON(e.resourceAttributesJSONBuffer, resAttr)

		slLen := logs.ScopeLogs().Len()
		for j := 0; j < slLen; j++ {
			scopeLog := logs.ScopeLogs().At(j)
			scopeURL := scopeLog.SchemaUrl()
			scopeLogScope := scopeLog.Scope()
			scopeName := scopeLogScope.Name()
			scopeVersion := scopeLogScope.Version()
			scopeLogRecords := scopeLog.LogRecords()
			e.scopeAttributesJSONBuffer.Reset()
			attributesToJSON(e.scopeAttributesJSONBuffer, scopeLogScope.Attributes())

			slrLen := scopeLogRecords.Len()
			for k := 0; k < slrLen; k++ {
				r := scopeLogRecords.At(k)
				e.logAttributesJSONBuffer.Reset()
				attributesToJSON(e.logAttributesJSONBuffer, r.Attributes())

				timestamp := r.Timestamp()
				if timestamp == 0 {
					timestamp = r.ObservedTimestamp()
				}

				cols.timestamp.Append(proto.DateTime64(timestamp))
				cols.scopeName.Append(scopeName)
				cols.body.Append(r.Body().Str())
				cols.traceID.Append(traceutil.TraceIDToHexOrEmptyString(r.TraceID()))
				cols.spanID.Append(traceutil.SpanIDToHexOrEmptyString(r.SpanID()))
				cols.traceFlags.Append(uint8(r.Flags()))
				cols.severityNumber.Append(uint8(r.SeverityNumber()))
				cols.serviceName.Append(serviceName)
				cols.resourceSchemaUrl.Append(resURL)
				cols.scopeSchemaUrl.Append(scopeURL)
				cols.scopeVersion.Append(scopeVersion)
				cols.severityText.Append(r.SeverityText())
				cols.resourceAttributes.Append(e.resourceAttributesJSONBuffer.Bytes())
				cols.scopeAttributes.Append(e.scopeAttributesJSONBuffer.Bytes())
				cols.logAttributes.Append(e.logAttributesJSONBuffer.Bytes())

				logCount++
			}
		}
	}

	if err := e.db.Do(ctx, ch.Query{
		Body:  e.insertSQL,
		Input: e.insertInput,
	}); err != nil {
		_ = closeDB(&e.db)

		return fmt.Errorf("chgo logs insert: %w", err)
	}

	duration := time.Since(start)
	e.logger.Debug("insert logs", zap.Int("records", logCount),
		zap.String("cost", duration.String()))

	return nil
}
