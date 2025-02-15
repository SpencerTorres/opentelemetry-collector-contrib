package logs

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/compress"
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
	cfg    *LogsConfig
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

type LogsConfig struct {
	Address          string
	User             string
	Password         string
	Database         string
	Table            string
	Compression      string
	CompressionLevel int
	TLS              bool
	ClientName       string
	Settings         map[string]string
}

func NewLogsExporter(cfg *LogsConfig, logger *zap.Logger) (*LogsExporter, error) {
	return &LogsExporter{
		cfg:          cfg,
		logger:       logger.Named("clickhouse"),
		maxBatchSize: 8192,
		insertSQL:    fmt.Sprintf(`INSERT INTO "%s"."%s" VALUES`, cfg.Database, cfg.Table),
	}, nil
}

func (e *LogsExporter) connectDB(ctx context.Context) error {
	_ = e.closeDB()

	compressMethod, _ := compress.MethodString(e.cfg.Compression)

	var tlsCfg *tls.Config
	if e.cfg.TLS {
		tlsCfg = &tls.Config{}
	}

	opts := ch.Options{
		Address:          e.cfg.Address,
		User:             e.cfg.User,
		Password:         e.cfg.Password,
		Database:         e.cfg.Database,
		Compression:      ch.Compression(compressMethod),
		CompressionLevel: ch.CompressionLevel(e.cfg.CompressionLevel),
		ClientName:       e.cfg.ClientName,
		TLS:              tlsCfg,
		Settings:         make([]ch.Setting, 1, 1+len(e.cfg.Settings)),
	}

	opts.Settings[0] = ch.Setting{Key: "allow_json_type", Value: "1"}
	for name, value := range e.cfg.Settings {
		opts.Settings = append(opts.Settings, ch.Setting{Key: name, Value: value})
	}

	c, err := ch.Dial(ctx, opts)
	if err != nil {
		return fmt.Errorf("chgo dial: %w", err)
	}
	e.db = c

	e.logger.Info("ClickHouse connected")

	return nil
}

func (e *LogsExporter) closeDB() error {
	if e.db == nil {
		return nil
	}

	err := e.db.Close()
	e.db = nil
	if err != nil {
		return fmt.Errorf("chgo close: %w", err)
	}

	e.logger.Info("ClickHouse disconnected")

	return nil
}

func (e *LogsExporter) Start(ctx context.Context, _ component.Host) error {
	err := e.connectDB(ctx)
	if err != nil {
		_ = e.closeDB()
		e.logger.Error(fmt.Sprintf("initial connection failed: %s", err))
	}

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

	e.resourceAttributesJSONBuffer = &JSONBuffer{
		buf:          make([]byte, 0, 4096),
		base64Buffer: make([]byte, 0, 1024),
	}
	e.scopeAttributesJSONBuffer = &JSONBuffer{
		buf:          make([]byte, 0, 4096),
		base64Buffer: make([]byte, 0, 1024),
	}
	e.logAttributesJSONBuffer = &JSONBuffer{
		buf:          make([]byte, 0, 4096),
		base64Buffer: make([]byte, 0, 1024),
	}

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
	return e.closeDB()
}

func (e *LogsExporter) PushLogsData(ctx context.Context, ld plog.Logs) error {
	if e.db == nil {
		if err := e.connectDB(ctx); err != nil {
			return err
		}
	}

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
			}
		}
	}

	if err := e.db.Do(ctx, ch.Query{
		Body:  e.insertSQL,
		Input: e.insertInput,
	}); err != nil {
		_ = e.closeDB()

		return fmt.Errorf("chgo insert: %w", err)
	}

	duration := time.Since(start)
	e.logger.Debug("insert logs", zap.Int("records", ld.LogRecordCount()),
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
	sLen := s.Len()
	for i := 0; i < sLen; i++ {
		if i > 0 {
			b.WriteByte(',')
		}

		valueToJSON(b, s.At(i))
	}
	b.buf = append(b.buf, ']')
}

func serializeBytesBase64(b *JSONBuffer, bs pcommon.ByteSlice) {
	b.base64Buffer = copyByteSlice(b.base64Buffer, bs)
	n := base64.StdEncoding.EncodedLen(len(b.base64Buffer))

	start := len(b.buf)
	b.grow(n + 2)

	b.buf = append(b.buf, '"')

	dst := b.buf[start+1 : start+1+n]
	base64.StdEncoding.Encode(dst, b.base64Buffer)
	b.buf = b.buf[:start+1+n]

	b.buf = append(b.buf, '"')
}

// copyByteSlice copies the data from the ByteSlice into the dst.
// There's no way to simply get the underlying *[]byte, unfortunately
// Removes an allocation in exchange for CPU
func copyByteSlice(dst []byte, bs pcommon.ByteSlice) []byte {
	dst = dst[:0]

	bsLen := bs.Len()
	for i := 0; i < bsLen; i++ {
		dst = append(dst, bs.At(i))
	}

	return dst
}

// JSONBuffer is a reusable buffer for faster JSON serialization
type JSONBuffer struct {
	buf          []byte
	base64Buffer []byte // TODO: this shouldn't go here
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
