package logs

import (
	"context"
	"fmt"
	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/traceutil"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"time"
)

type LogsExporter struct {
	logger       *zap.Logger
	db           *ch.Client
	maxBatchSize int
	columns      *logColumns
	insertInput  proto.Input
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
	resourceAttributes proto.ColJSONStr
	scopeSchemaUrl     *proto.ColLowCardinality[string]
	scopeName          proto.ColStr
	scopeVersion       *proto.ColLowCardinality[string]
	scopeAttributes    proto.ColJSONStr
	logAttributes      proto.ColJSONStr
}

func NewLogsExporter(logger *zap.Logger) (*LogsExporter, error) {
	return &LogsExporter{
		logger:       logger,
		maxBatchSize: 10_000,
	}, nil
}

func (e *LogsExporter) Start(ctx context.Context, _ component.Host) error {
	opts := ch.Options{
		Address:     "localhost:9000",
		Database:    "otel_chgo",
		Compression: ch.CompressionLZ4,
		Settings: []ch.Setting{
			{Key: "allow_json_type", Value: "1"},
		},
	}

	c, err := ch.Dial(ctx, opts)
	if err != nil {
		panic(err)
	}
	e.db = c

	strSize := 64
	bSize := e.maxBatchSize

	cols := &logColumns{
		timestamp: proto.ColDateTime64Raw{
			ColDateTime64: proto.ColDateTime64{
				Data:         make([]proto.DateTime64, 0, bSize),
				Location:     time.UTC,
				Precision:    proto.PrecisionNano,
				PrecisionSet: true,
			},
		},
		traceID: proto.ColStr{
			Buf: make([]byte, 0, strSize*bSize),
			Pos: make([]proto.Position, 0, bSize),
		},
		spanID: proto.ColStr{
			Buf: make([]byte, 0, strSize*bSize),
			Pos: make([]proto.Position, 0, bSize),
		},
		traceFlags: make(proto.ColUInt8, 0, bSize),
		severityText: proto.NewLowCardinality[string](&proto.ColStr{
			Buf: make([]byte, 0, strSize*bSize),
			Pos: make([]proto.Position, 0, bSize),
		}),
		severityNumber: make(proto.ColUInt8, 0, bSize),
		serviceName: proto.NewLowCardinality[string](&proto.ColStr{
			Buf: make([]byte, 0, strSize*bSize),
			Pos: make([]proto.Position, 0, bSize),
		}),
		body: proto.ColStr{
			Buf: make([]byte, 0, strSize*bSize),
			Pos: make([]proto.Position, 0, bSize),
		},
		resourceSchemaUrl: proto.NewLowCardinality[string](&proto.ColStr{
			Buf: make([]byte, 0, strSize*bSize),
			Pos: make([]proto.Position, 0, bSize),
		}),
		resourceAttributes: proto.ColJSONStr{},
		scopeSchemaUrl: proto.NewLowCardinality[string](&proto.ColStr{
			Buf: make([]byte, 0, strSize*bSize),
			Pos: make([]proto.Position, 0, bSize),
		}),
		scopeName: proto.ColStr{
			Buf: make([]byte, 0, strSize*bSize),
			Pos: make([]proto.Position, 0, bSize),
		},
		scopeVersion: proto.NewLowCardinality[string](&proto.ColStr{
			Buf: make([]byte, 0, strSize*bSize),
			Pos: make([]proto.Position, 0, bSize),
		}),
		scopeAttributes: proto.ColJSONStr{},
		logAttributes:   proto.ColJSONStr{},
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

	return nil
}

func (e *LogsExporter) Shutdown(_ context.Context) error {
	return e.db.Close()
}

func (e *LogsExporter) PushLogsData(ctx context.Context, ld plog.Logs) error {
	cols := e.columns
	e.insertInput.Reset()

	start := time.Now()

	rsLogs := ld.ResourceLogs()
	rsLen := rsLogs.Len()
	for i := 0; i < rsLen; i++ {
		logs := rsLogs.At(i)
		res := logs.Resource()
		resURL := logs.SchemaUrl()
		resAttr := res.Attributes()
		serviceName := internal.GetServiceName(resAttr)
		resAttrStr := attributesToJSONString(resAttr)

		slLen := logs.ScopeLogs().Len()
		for j := 0; j < slLen; j++ {
			scopeLog := logs.ScopeLogs().At(j)
			scopeURL := scopeLog.SchemaUrl()
			scopeLogScope := scopeLog.Scope()
			scopeName := scopeLogScope.Name()
			scopeVersion := scopeLogScope.Version()
			scopeAttr := scopeLogScope.Attributes()
			scopeLogRecords := scopeLog.LogRecords()
			scopeAttrStr := attributesToJSONString(scopeAttr)

			for k := 0; k < scopeLogRecords.Len(); k++ {
				r := scopeLogRecords.At(k)
				logAttr := r.Attributes()
				logAttrStr := attributesToJSONString(logAttr)

				timestamp := r.Timestamp()
				if timestamp == 0 {
					timestamp = r.ObservedTimestamp()
				}

				cols.timestamp.Append(proto.DateTime64(timestamp))
				cols.traceID.Append(traceutil.TraceIDToHexOrEmptyString(r.TraceID()))
				cols.spanID.Append(traceutil.SpanIDToHexOrEmptyString(r.SpanID()))
				cols.traceFlags.Append(uint8(r.Flags()))
				cols.severityText.Append(r.SeverityText())
				cols.severityNumber.Append(uint8(r.SeverityNumber()))
				cols.serviceName.Append(serviceName)
				cols.body.Append(r.Body().AsString())
				cols.resourceSchemaUrl.Append(resURL)
				cols.resourceAttributes.Append(resAttrStr)
				cols.scopeSchemaUrl.Append(scopeURL)
				cols.scopeName.Append(scopeName)
				cols.scopeVersion.Append(scopeVersion)
				cols.scopeAttributes.Append(scopeAttrStr)
				cols.logAttributes.Append(logAttrStr)
			}
		}
	}

	if err := e.db.Do(ctx, ch.Query{
		Body:  "INSERT INTO otel_chgo.otel_logs VALUES",
		Input: e.insertInput,
	}); err != nil {
		return fmt.Errorf("chgo Do: %w", err)
	}

	duration := time.Since(start)
	e.logger.Info("insert logs", zap.Int("records", ld.LogRecordCount()),
		zap.String("cost", duration.String()))

	return nil
}

func attributesToJSONString(m pcommon.Map) string {
	if m.Len() == 0 {
		return "{}"
	}

	var sb strings.Builder
	sb.WriteRune('{')
	var first = true
	m.Range(func(k string, v pcommon.Value) bool {
		if first {
			first = false
		} else {
			sb.WriteRune(',')
		}

		sb.WriteString(strconv.Quote(k))
		sb.WriteRune(':')
		sb.WriteString(valueToString(v))

		return true
	})
	sb.WriteRune('}')

	return sb.String()
}

func valueToString(v pcommon.Value) string {
	switch v.Type() {
	case pcommon.ValueTypeEmpty:
		return "null"
	case pcommon.ValueTypeStr:
		return strconv.Quote(v.Str())
	case pcommon.ValueTypeBool:
		return strconv.FormatBool(v.Bool())
	case pcommon.ValueTypeDouble:
		return strconv.FormatFloat(v.Double(), 'g', -1, 64)
	case pcommon.ValueTypeInt:
		return strconv.FormatInt(v.Int(), 10)
	case pcommon.ValueTypeBytes:
		return string(v.Bytes().AsRaw())
	case pcommon.ValueTypeMap:
		return attributesToJSONString(v.Map())
	case pcommon.ValueTypeSlice:
		//return v.Slice().AsRaw()
		return "[]"
	default:
		return ""
	}
}
