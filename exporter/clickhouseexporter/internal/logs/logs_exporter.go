package logs

import (
	"context"
	"encoding/base64"
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
	"time"
)

type LogsExporter struct {
	logger       *zap.Logger
	db           *ch.Client
	maxBatchSize int
	columns      *logColumns
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

func NewLogsExporter(logger *zap.Logger) (*LogsExporter, error) {
	return &LogsExporter{
		logger:       logger,
		maxBatchSize: 8192,
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

	jsonSize := 512
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
		traceFlags:     make(proto.ColUInt8, 0, bSize),
		severityText:   newLowCardinalityString(strSize, bSize),
		severityNumber: make(proto.ColUInt8, 0, bSize),
		serviceName:    newLowCardinalityString(strSize, bSize),
		body: proto.ColStr{
			Buf: make([]byte, 0, strSize*bSize),
			Pos: make([]proto.Position, 0, bSize),
		},
		resourceSchemaUrl: newLowCardinalityString(strSize, bSize),
		resourceAttributes: proto.ColJSONBytes{
			ColJSONStr: proto.ColJSONStr{
				Str: proto.ColStr{
					Buf: make([]byte, 0, jsonSize*bSize),
					Pos: make([]proto.Position, 0, bSize),
				},
			},
		},
		scopeSchemaUrl: newLowCardinalityString(strSize, bSize),
		scopeName: proto.ColStr{
			Buf: make([]byte, 0, strSize*bSize),
			Pos: make([]proto.Position, 0, bSize),
		},
		scopeVersion: newLowCardinalityString(strSize, bSize),
		scopeAttributes: proto.ColJSONBytes{
			ColJSONStr: proto.ColJSONStr{
				Str: proto.ColStr{
					Buf: make([]byte, 0, jsonSize*bSize),
					Pos: make([]proto.Position, 0, bSize),
				},
			},
		},
		logAttributes: proto.ColJSONBytes{
			ColJSONStr: proto.ColJSONStr{
				Str: proto.ColStr{
					Buf: make([]byte, 0, jsonSize*bSize),
					Pos: make([]proto.Position, 0, bSize),
				},
			},
		},
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

	e.resourceAttributesJSONBuffer = &JSONBuffer{buf: make([]byte, 0, 8192)}
	e.scopeAttributesJSONBuffer = &JSONBuffer{buf: make([]byte, 0, 8192)}
	e.logAttributesJSONBuffer = &JSONBuffer{buf: make([]byte, 0, 8192)}

	return nil
}

func newLowCardinalityString(strSize, bufSize int) *proto.ColLowCardinality[string] {
	lc := proto.NewLowCardinality[string](&proto.ColStr{
		Buf: make([]byte, 0, strSize*bufSize),
		Pos: make([]proto.Position, 0, bufSize),
	})
	lc.Values = make([]string, 0, bufSize)

	return lc
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

			for k := 0; k < scopeLogRecords.Len(); k++ {
				r := scopeLogRecords.At(k)
				e.logAttributesJSONBuffer.Reset()
				attributesToJSON(e.logAttributesJSONBuffer, r.Attributes())

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
				cols.resourceAttributes.Append(e.resourceAttributesJSONBuffer.Bytes())
				cols.scopeSchemaUrl.Append(scopeURL)
				cols.scopeName.Append(scopeName)
				cols.scopeVersion.Append(scopeVersion)
				cols.scopeAttributes.Append(e.scopeAttributesJSONBuffer.Bytes())
				cols.logAttributes.Append(e.logAttributesJSONBuffer.Bytes())
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

// attributesToJSON serializes attributes to JSON using a reusable buffer
func attributesToJSON(b *JSONBuffer, m pcommon.Map) {
	if m.Len() == 0 {
		b.WriteString("{}")
		return
	}

	b.grow(2)
	b.buf = append(b.buf, '{')
	first := true
	m.Range(func(k string, v pcommon.Value) bool {
		if first {
			first = false
		} else {
			b.WriteByte(',')
		}
		b.WriteQuote(k)
		b.WriteByte(':')
		valueToJSON(b, v)
		return true
	})
	b.buf = append(b.buf, '}')
}

func valueToJSON(b *JSONBuffer, v pcommon.Value) {
	switch v.Type() {
	case pcommon.ValueTypeEmpty:
		b.WriteString("null")
	case pcommon.ValueTypeStr:
		b.WriteQuote(v.Str())
	case pcommon.ValueTypeBool:
		if v.Bool() {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case pcommon.ValueTypeDouble:
		b.buf = strconv.AppendFloat(b.buf, v.Double(), 'g', -1, 64)
	case pcommon.ValueTypeInt:
		b.buf = strconv.AppendInt(b.buf, v.Int(), 10)
	case pcommon.ValueTypeBytes:
		serializeBytesBase64(b, v.Bytes())
	case pcommon.ValueTypeMap:
		attributesToJSON(b, v.Map())
	case pcommon.ValueTypeSlice:
		serializeSlice(b, v.Slice())
	default:
		b.WriteString("null")
	}
}

func serializeSlice(b *JSONBuffer, s pcommon.Slice) {
	if s.Len() == 0 {
		b.WriteString("[]")
		return
	}

	b.grow(2)
	b.buf = append(b.buf, '[')
	for i := 0; i < s.Len(); i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		valueToJSON(b, s.At(i))
	}
	b.buf = append(b.buf, ']')
}

func serializeBytesBase64(b *JSONBuffer, bs pcommon.ByteSlice) {
	raw := bs.AsRaw()
	n := base64.StdEncoding.EncodedLen(len(raw))

	start := len(b.buf)
	b.grow(n + 2)

	b.buf = append(b.buf, '"')

	dst := b.buf[start+1 : start+1+n]
	base64.StdEncoding.Encode(dst, raw)
	b.buf = b.buf[:start+1+n]

	b.buf = append(b.buf, '"')
}

// JSONBuffer is a reusable buffer for faster JSON serialization
type JSONBuffer struct {
	buf []byte
}

func (b *JSONBuffer) Reset() {
	b.buf = b.buf[:0]
}

func (b *JSONBuffer) Bytes() []byte {
	return b.buf
}

func (b *JSONBuffer) grow(n int) {
	if cap(b.buf)-len(b.buf) < n {
		buf := make([]byte, len(b.buf), 2*cap(b.buf)+n)
		copy(buf, b.buf)
		b.buf = buf
	}
}

func (b *JSONBuffer) WriteString(s string) {
	b.grow(len(s))
	b.buf = append(b.buf, s...)
}

func (b *JSONBuffer) WriteByte(c byte) {
	b.grow(1)
	b.buf = append(b.buf, c)
}

// WriteQuote writes a quoted string to the buffer
func (b *JSONBuffer) WriteQuote(s string) {
	b.grow(len(s) + 2)
	b.buf = strconv.AppendQuote(b.buf, s)
}
