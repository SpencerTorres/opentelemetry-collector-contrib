package chgo

import (
	"context"
	"fmt"
	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"time"
)

type TracesExporter struct {
	cfg    *ChConfig
	logger *zap.Logger

	db *ch.Client

	maxBatchSize int
	columns      *traceColumns
	insertSQL    string
	insertInput  proto.Input

	resourceAttributesJSONBuffer *JSONBuffer
	spanAttributesJSONBuffer     *JSONBuffer
	eventsAttributesJSONBuffer   *JSONBuffer
	linksAttributesJSONBuffer    *JSONBuffer

	hexEncodeBuffer []byte
}

type traceColumns struct {
	timestamp          proto.ColDateTime64Raw
	traceID            proto.ColBytes
	spanID             proto.ColBytes
	parentSpanID       proto.ColBytes
	traceState         proto.ColStr
	spanName           *proto.ColLowCardinality[string]
	spanKind           *proto.ColLowCardinality[string]
	serviceName        *proto.ColLowCardinality[string]
	resourceAttributes proto.ColJSONBytes
	//scopeSchemaUrl     *proto.ColLowCardinality[string]
	scopeName        proto.ColStr
	scopeVersion     proto.ColStr
	spanAttributes   proto.ColJSONBytes
	duration         proto.ColUInt64
	statusCode       *proto.ColLowCardinality[string]
	statusMessage    proto.ColStr
	eventsTimestamps *proto.ColArr[proto.DateTime64]
	eventsNames      *proto.ColArr[string]
	eventsAttributes *proto.ColArr[[]byte]
	linksTraceIDs    *proto.ColArr[[]byte]
	linksSpanIDs     *proto.ColArr[[]byte]
	linksTraceStates *proto.ColArr[string]
	linksAttributes  *proto.ColArr[[]byte]
}

func NewTracesExporter(cfg *ChConfig, logger *zap.Logger) (*TracesExporter, error) {
	return &TracesExporter{
		cfg:          cfg,
		logger:       logger.Named("clickhouse"),
		maxBatchSize: 8192,
		insertSQL:    fmt.Sprintf(`INSERT INTO "%s"."%s" VALUES`, cfg.Database, cfg.Table),
	}, nil
}

func (e *TracesExporter) Start(ctx context.Context, _ component.Host) error {
	err := connectDB(ctx, &e.db, e.cfg)
	if err != nil {
		_ = closeDB(&e.db)
		e.logger.Error(fmt.Sprintf("initial connection failed: %s", err))
	}

	jsonSize := 512
	strSize := 64
	bSize := e.maxBatchSize

	cols := &traceColumns{
		timestamp:          newColDateTime64Raw(bSize),
		traceID:            newColBytes(strSize, bSize),
		spanID:             newColBytes(strSize, bSize),
		parentSpanID:       newColBytes(strSize, bSize),
		traceState:         newColString(strSize, bSize),
		spanName:           newColLowCardinalityString(strSize, bSize),
		spanKind:           newColLowCardinalityString(strSize, bSize),
		serviceName:        newColLowCardinalityString(strSize, bSize),
		resourceAttributes: newColJSONBytes(jsonSize, bSize),
		scopeName:          newColString(strSize, bSize),
		scopeVersion:       newColString(strSize, bSize),
		spanAttributes:     newColJSONBytes(jsonSize, bSize),
		duration:           make(proto.ColUInt64, 0, bSize),
		statusCode:         newColLowCardinalityString(strSize, bSize),
		statusMessage:      newColString(strSize, bSize),
		eventsTimestamps:   newColArrayDateTime64Raw(bSize),
		eventsNames:        newColArrayLowCardinalityString(strSize, bSize),
		eventsAttributes:   newColArrayJSONBytes(jsonSize, bSize),
		linksTraceIDs:      newColArrayBytes(strSize, bSize),
		linksSpanIDs:       newColArrayBytes(strSize, bSize),
		linksTraceStates:   newColArrayString(strSize, bSize),
		linksAttributes:    newColArrayJSONBytes(jsonSize, bSize),
	}
	e.columns = cols

	e.insertInput = proto.Input{
		{Name: "Timestamp", Data: &cols.timestamp},
		{Name: "TraceId", Data: &cols.traceID},
		{Name: "SpanId", Data: &cols.spanID},
		{Name: "ParentSpanId", Data: &cols.parentSpanID},
		{Name: "TraceState", Data: &cols.traceState},
		{Name: "SpanName", Data: cols.spanName},
		{Name: "SpanKind", Data: cols.spanKind},
		{Name: "ServiceName", Data: cols.serviceName},
		{Name: "ResourceAttributes", Data: &cols.resourceAttributes},
		{Name: "ScopeName", Data: &cols.scopeName},
		{Name: "ScopeVersion", Data: &cols.scopeVersion},
		{Name: "SpanAttributes", Data: &cols.spanAttributes},
		{Name: "Duration", Data: &cols.duration},
		{Name: "StatusCode", Data: cols.statusCode},
		{Name: "StatusMessage", Data: &cols.statusMessage},
		{Name: "Events.Timestamp", Data: cols.eventsTimestamps},
		{Name: "Events.Name", Data: cols.eventsNames},
		{Name: "Events.Attributes", Data: cols.eventsAttributes},
		{Name: "Links.TraceId", Data: cols.linksTraceIDs},
		{Name: "Links.SpanId", Data: cols.linksSpanIDs},
		{Name: "Links.TraceState", Data: cols.linksTraceStates},
		{Name: "Links.Attributes", Data: cols.linksAttributes},
	}

	e.resourceAttributesJSONBuffer = newJSONBuffer(jsonSize, strSize)
	e.spanAttributesJSONBuffer = newJSONBuffer(jsonSize, strSize)
	e.eventsAttributesJSONBuffer = newJSONBuffer(jsonSize, strSize)
	e.linksAttributesJSONBuffer = newJSONBuffer(jsonSize, strSize)

	e.hexEncodeBuffer = make([]byte, 0, 128)

	return nil
}

func (e *TracesExporter) Shutdown(_ context.Context) error {
	return closeDB(&e.db)
}

func (e *TracesExporter) PushTraceData(ctx context.Context, td ptrace.Traces) error {
	if e.db == nil {
		if err := connectDB(ctx, &e.db, e.cfg); err != nil {
			return err
		}
	}

	cols := e.columns
	e.insertInput.Reset()

	start := time.Now()

	var spanCount int
	rsSpans := td.ResourceSpans()
	rsLen := rsSpans.Len()
	for i := 0; i < rsLen; i++ {
		spans := rsSpans.At(i)
		res := spans.Resource()
		resAttr := res.Attributes()
		serviceName := internal.GetServiceName(resAttr)
		e.resourceAttributesJSONBuffer.Reset()
		attributesToJSON(e.resourceAttributesJSONBuffer, resAttr)

		ssRootLen := spans.ScopeSpans().Len()
		for j := 0; j < ssRootLen; j++ {
			scopeSpanRoot := spans.ScopeSpans().At(j)
			scopeSpanScope := scopeSpanRoot.Scope()
			scopeName := scopeSpanScope.Name()
			scopeVersion := scopeSpanScope.Version()
			scopeSpans := scopeSpanRoot.Spans()

			ssLen := scopeSpans.Len()
			for k := 0; k < ssLen; k++ {
				span := scopeSpans.At(k)
				spanStatus := span.Status()

				e.spanAttributesJSONBuffer.Reset()
				attributesToJSON(e.spanAttributesJSONBuffer, span.Attributes())

				cols.timestamp.Append(proto.DateTime64(span.StartTimestamp()))
				e.hexEncodeBuffer = appendTraceIDToHex(e.hexEncodeBuffer[:0], span.TraceID())
				cols.traceID.Append(e.hexEncodeBuffer)
				e.hexEncodeBuffer = appendSpanIDToHex(e.hexEncodeBuffer[:0], span.SpanID())
				cols.spanID.Append(e.hexEncodeBuffer)
				e.hexEncodeBuffer = appendSpanIDToHex(e.hexEncodeBuffer[:0], span.ParentSpanID())
				cols.parentSpanID.Append(e.hexEncodeBuffer)
				cols.traceState.Append(span.TraceState().AsRaw())
				cols.spanName.Append(span.Name())
				cols.spanKind.Append(span.Kind().String())
				cols.serviceName.Append(serviceName)
				cols.resourceAttributes.Append(e.resourceAttributesJSONBuffer.Bytes())
				cols.scopeName.Append(scopeName)
				cols.scopeVersion.Append(scopeVersion)
				cols.spanAttributes.Append(e.spanAttributesJSONBuffer.Bytes())
				cols.duration.Append(uint64(span.EndTimestamp() - span.StartTimestamp()))
				cols.statusCode.Append(spanStatus.Code().String())
				cols.statusMessage.Append(spanStatus.Message())
				appendEvents(cols.eventsTimestamps, cols.eventsNames, e.eventsAttributesJSONBuffer, cols.eventsAttributes, span.Events())
				appendLinks(cols.linksTraceIDs, cols.linksSpanIDs, cols.linksTraceStates, e.hexEncodeBuffer, e.linksAttributesJSONBuffer, cols.linksAttributes, span.Links())

				spanCount++
			}
		}
	}

	if err := e.db.Do(ctx, ch.Query{
		Body:  e.insertSQL,
		Input: e.insertInput,
	}); err != nil {
		_ = closeDB(&e.db)

		return fmt.Errorf("chgo traces insert: %w", err)
	}

	duration := time.Since(start)
	e.logger.Debug("insert traces", zap.Int("records", spanCount),
		zap.String("cost", duration.String()))

	return nil
}

func appendEvents(times *proto.ColArr[proto.DateTime64], names *proto.ColArr[string], attrBuf *JSONBuffer, attrs *proto.ColArr[[]byte], events ptrace.SpanEventSlice) {
	eLen := events.Len()
	for i := 0; i < eLen; i++ {
		event := events.At(i)

		times.Data.Append(proto.DateTime64(event.Timestamp()))
		names.Data.Append(event.Name())

		attrBuf.Reset()
		attributesToJSON(attrBuf, event.Attributes())
		attrs.Data.Append(attrBuf.Bytes())
	}

	times.Offsets = append(times.Offsets, uint64(times.Data.Rows()))
	names.Offsets = append(names.Offsets, uint64(names.Data.Rows()))
	attrs.Offsets = append(attrs.Offsets, uint64(attrs.Data.Rows()))
}

func appendLinks(traceIDs, spanIDs *proto.ColArr[[]byte], states *proto.ColArr[string], hexEncodeBuffer []byte, attrBuf *JSONBuffer, attrs *proto.ColArr[[]byte], links ptrace.SpanLinkSlice) {
	lLen := links.Len()
	for i := 0; i < lLen; i++ {
		link := links.At(i)

		hexEncodeBuffer = appendTraceIDToHex(hexEncodeBuffer[:0], link.TraceID())
		traceIDs.Data.Append(hexEncodeBuffer)
		hexEncodeBuffer = appendSpanIDToHex(hexEncodeBuffer[:0], link.SpanID())
		spanIDs.Data.Append(hexEncodeBuffer)
		states.Data.Append(link.TraceState().AsRaw())

		attrBuf.Reset()
		attributesToJSON(attrBuf, link.Attributes())
		attrs.Data.Append(attrBuf.Bytes())
	}

	traceIDs.Offsets = append(traceIDs.Offsets, uint64(traceIDs.Data.Rows()))
	spanIDs.Offsets = append(spanIDs.Offsets, uint64(spanIDs.Data.Rows()))
	states.Offsets = append(states.Offsets, uint64(states.Data.Rows()))
	attrs.Offsets = append(attrs.Offsets, uint64(attrs.Data.Rows()))
}
