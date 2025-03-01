package chgo

import (
	"context"
	"fmt"
	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"time"
)

type logsExporter struct {
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

	hexEncodeBuffer []byte

	batchMetricsQueue chan batchMetrics
}

type logColumns struct {
	timestamp          proto.ColDateTime64Raw
	traceID            proto.ColBytes
	spanID             proto.ColBytes
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

func newLogsExporter(cfg *ChConfig, logger *zap.Logger, batchMetricsQueue chan batchMetrics) (*logsExporter, error) {
	return &logsExporter{
		cfg:          cfg,
		logger:       logger.Named("clickhouse"),
		maxBatchSize: 10 * 1024,
		insertSQL:    fmt.Sprintf(`INSERT INTO "%s"."%s" VALUES`, cfg.Database, cfg.Table),

		batchMetricsQueue: batchMetricsQueue,
	}, nil
}

func (e *logsExporter) start(ctx context.Context, _ component.Host) error {
	err := connectDB(ctx, &e.db, e.cfg)
	if err != nil {
		_ = closeDB(&e.db)
		e.logger.Error(fmt.Sprintf("initial connection failed: %s", err))
	}

	jsonSize := 1024
	strSize := 128
	bSize := e.maxBatchSize

	cols := &logColumns{
		timestamp:          newColDateTime64Raw(bSize),
		traceID:            newColBytes(strSize, bSize),
		spanID:             newColBytes(strSize, bSize),
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

	e.hexEncodeBuffer = make([]byte, 0, 256)

	return nil
}

func (e *logsExporter) shutdown(_ context.Context) error {
	return closeDB(&e.db)
}

func (e *logsExporter) pushLogsData(ctx context.Context, ld plog.Logs) error {
	if e.db == nil {
		if err := connectDB(ctx, &e.db, e.cfg); err != nil {
			return err
		}
	}

	cols := e.columns
	e.insertInput.Reset()

	processStart := time.Now()

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
				e.hexEncodeBuffer = appendTraceIDToHex(e.hexEncodeBuffer[:0], r.TraceID())
				cols.traceID.Append(e.hexEncodeBuffer)
				e.hexEncodeBuffer = appendSpanIDToHex(e.hexEncodeBuffer[:0], r.SpanID())
				cols.spanID.Append(e.hexEncodeBuffer)
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

	processDuration := time.Since(processStart)
	networkStart := time.Now()
	if err := e.db.Do(ctx, ch.Query{
		Body:  e.insertSQL,
		Input: e.insertInput,
	}); err != nil {
		_ = closeDB(&e.db)

		return fmt.Errorf("chgo logs insert: %w", err)
	}

	networkDuration := time.Since(networkStart)
	//totalDuration := time.Since(processStart)
	e.reportBatchMetrics(processStart, logCount, processDuration, networkDuration)
	//e.logger.Debug("insert logs", zap.Int("records", logCount),
	//	zap.String("process_cost", processDuration.String()),
	//	zap.String("network_cost", networkDuration.String()),
	//	zap.String("total_cost", totalDuration.String()))

	return nil
}

func (e *logsExporter) reportBatchMetrics(batchCompleteTime time.Time, batchSize int, processDuration, networkDuration time.Duration) {
	m := batchMetrics{
		timestamp: batchCompleteTime,
		count:     uint64(batchSize),
		process:   uint64(processDuration.Nanoseconds()),
		network:   uint64(networkDuration.Nanoseconds()),
	}

	e.batchMetricsQueue <- m
}

type LogsExporterPool struct {
	pool chan *logsExporter

	debugInput        proto.Input
	debugColVersion   proto.ColStr
	debugColTimestamp proto.ColDateTime64Raw
	debugColCount     proto.ColUInt64
	debugColProcess   proto.ColUInt64
	debugColNetwork   proto.ColUInt64

	cfg                 *ChConfig
	batchMetricsClient  *ch.Client
	batchMetricsVersion string
	batchMetricsQueue   chan batchMetrics
}

type batchMetrics struct {
	timestamp time.Time
	count     uint64
	process   uint64
	network   uint64
}

func (p *LogsExporterPool) listenBatchMetrics() {
	for m := range p.batchMetricsQueue {
		ctx := context.Background()

		if p.batchMetricsClient == nil {
			if err := connectDB(ctx, &p.batchMetricsClient, p.cfg); err != nil {
				fmt.Println("batch metrics connect err", err)
				continue
			}
		}

		p.debugInput.Reset()
		p.debugColVersion.Append("ch-go-json")
		p.debugColTimestamp.Append(proto.DateTime64(m.timestamp.UnixMilli()))
		p.debugColCount.Append(m.count)
		p.debugColProcess.Append(m.process)
		p.debugColNetwork.Append(m.network)
		if err := p.batchMetricsClient.Do(ctx, ch.Query{
			Body:     "INSERT INTO otel_chgo.perf VALUES",
			Input:    p.debugInput,
			Settings: []ch.Setting{{Key: "async_insert", Value: "1"}, {Key: "wait_for_async_insert", Value: "0"}},
		}); err != nil {
			_ = closeDB(&p.batchMetricsClient)
			fmt.Println("batch metrics insert err", err)
		}
	}
}

func NewLogsExporterPool(cfg *ChConfig, logger *zap.Logger, poolSize int) (*LogsExporterPool, error) {
	pool := LogsExporterPool{
		pool: make(chan *logsExporter, poolSize),
		cfg:  cfg,

		batchMetricsVersion: "ch-go-json",
		batchMetricsQueue:   make(chan batchMetrics, 1000),
	}

	for i := 0; i < poolSize; i++ {
		instance, err := newLogsExporter(cfg, logger, pool.batchMetricsQueue)
		if err != nil {
			return nil, err
		}

		pool.pool <- instance
	}

	return &pool, nil
}

func (p *LogsExporterPool) acquire() *logsExporter {
	return <-p.pool
}

func (p *LogsExporterPool) release(e *logsExporter) {
	p.pool <- e
}

func (p *LogsExporterPool) Start(ctx context.Context, host component.Host) error {
	for i := 0; i < cap(p.pool); i++ {
		exporter := p.acquire()

		err := exporter.start(ctx, host)
		if err != nil {
			p.release(exporter)
			return err
		}

		p.release(exporter)
	}

	if err := connectDB(ctx, &p.batchMetricsClient, p.cfg); err != nil {
		return err
	}

	p.debugColVersion = newColString(16, 1)
	p.debugColTimestamp = newColDateTime64Raw(1)
	p.debugColCount = make(proto.ColUInt64, 0, 1)
	p.debugColProcess = make(proto.ColUInt64, 0, 1)
	p.debugColNetwork = make(proto.ColUInt64, 0, 1)

	p.debugInput = proto.Input{
		{Name: "Version", Data: &p.debugColVersion},
		{Name: "Timestamp", Data: &p.debugColTimestamp},
		{Name: "Count", Data: &p.debugColCount},
		{Name: "Process", Data: &p.debugColProcess},
		{Name: "Network", Data: &p.debugColNetwork},
	}

	go p.listenBatchMetrics()

	return nil
}

func (p *LogsExporterPool) Shutdown(ctx context.Context) error {
	var err error
	for i := 0; i < cap(p.pool); i++ {
		exporter := p.acquire()

		shutdownErr := exporter.shutdown(ctx)
		if shutdownErr != nil && err == nil {
			err = shutdownErr
		}
	}

	close(p.pool)

	if p.batchMetricsClient != nil {
		p.batchMetricsClient.Close()
	}
	close(p.batchMetricsQueue)

	if err != nil {
		return err
	}

	return nil
}

func (p *LogsExporterPool) PushLogsData(ctx context.Context, ld plog.Logs) error {
	exporter := p.acquire()
	defer p.release(exporter)

	return exporter.pushLogsData(ctx, ld)
}
