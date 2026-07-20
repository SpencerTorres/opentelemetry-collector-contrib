// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metricsv2 // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal/metricsv2"

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-faster/city"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal"
)

// Values stored in the LowCardinality(String) MetricType column.
const (
	metricTypeGauge        = "gauge"
	metricTypeSum          = "sum"
	metricTypeHistogram    = "histogram"
	metricTypeExpHistogram = "exponential_histogram"
	metricTypeSummary      = "summary"
)

// Values stored in the LowCardinality(String) Temporality column.
const (
	temporalityUnspecified = "unspecified"
	temporalityDelta       = "delta"
	temporalityCumulative  = "cumulative"
)

const secondsPerDay = 86400

// InsertSQLs holds the rendered INSERT statements for every v2 table.
type InsertSQLs struct {
	Series        string
	Points        string
	HistPoints    string
	ExpHistPoints string
	SummaryPoints string
	Exemplars     string
	Families      string
}

type seriesDayKey struct {
	day  int32
	hash uint64
}

type seriesRow struct {
	date         time.Time
	metricName   string
	hash         uint64
	serviceName  string
	metricType   string
	temporality  string
	isMonotonic  bool
	unit         string
	scopeName    string
	scopeVersion string
	resURL       string
	scopeURL     string
	resAttrs     column.IterableOrderedMap
	scopeAttrs   column.IterableOrderedMap
	attrs        column.IterableOrderedMap
	bounds       []float64
	quantiles    []float64
	seen         time.Time
}

type floatPointRow struct {
	metricName string
	hash       uint64
	startTime  time.Time
	timestamp  time.Time
	value      float64
	flags      uint8
}

type histPointRow struct {
	metricName   string
	hash         uint64
	startTime    time.Time
	timestamp    time.Time
	count        uint64
	sum          float64
	min          float64
	max          float64
	bucketCounts []uint64
	flags        uint8
}

type expHistPointRow struct {
	metricName     string
	hash           uint64
	startTime      time.Time
	timestamp      time.Time
	count          uint64
	sum            float64
	min            float64
	max            float64
	scale          int8
	zeroCount      uint64
	zeroThreshold  float64
	positiveOffset int32
	positiveCounts []uint64
	negativeOffset int32
	negativeCounts []uint64
	flags          uint8
}

type exemplarRow struct {
	metricName string
	hash       uint64
	timestamp  time.Time
	value      float64
	traceID    string
	spanID     string
	attrs      column.IterableOrderedMap
}

type summaryPointRow struct {
	metricName     string
	hash           uint64
	startTime      time.Time
	timestamp      time.Time
	count          uint64
	sum            float64
	quantileValues []float64
	flags          uint8
}

type familyKey struct {
	name       string
	metricType string
	unit       string
}

// Batch accumulates converted rows for one push and inserts them.
// A Batch is not safe for concurrent use; create one per push.
type Batch struct {
	cache  *SeriesCache
	logger *zap.Logger

	h        hasher
	resEnd   int // hasher buffer length after the resource section
	scopeEnd int // hasher buffer length after the scope section

	resPairs   []attrPair
	scopePairs []attrPair
	dpPairs    []attrPair
	exPairs    []attrPair

	serviceName  string
	resURL       string
	scopeName    string
	scopeVersion string
	scopeURL     string
	resMap       column.IterableOrderedMap
	scopeMap     column.IterableOrderedMap

	series        []seriesRow
	seriesSeen    map[seriesDayKey]struct{}
	newSeries     map[int32][]uint64
	floatPoints   []floatPointRow
	histPoints    []histPointRow
	expHistPoints []expHistPointRow
	summaryPoints []summaryPointRow
	exemplars     []exemplarRow
	families      map[familyKey]string
}

// NewBatch returns an empty batch backed by the shared series cache.
func NewBatch(cache *SeriesCache, logger *zap.Logger) *Batch {
	return &Batch{
		cache:      cache,
		logger:     logger,
		seriesSeen: make(map[seriesDayKey]struct{}),
		newSeries:  make(map[int32][]uint64),
		families:   make(map[familyKey]string),
	}
}

// SetResource must be called before AddMetric whenever the resource changes.
func (b *Batch) SetResource(resAttr pcommon.Map, resURL string) {
	b.resPairs = sortedPairs(resAttr, b.resPairs)
	b.resMap = orderedMapFromPairs(b.resPairs)
	b.serviceName = internal.GetServiceName(resAttr)
	b.resURL = resURL

	b.h.reset()
	b.h.writeAttrs(sectionResourceAttrs, b.resPairs)
	b.resEnd = b.h.len()
	b.scopeEnd = b.resEnd
}

// SetScope must be called before AddMetric whenever the scope changes.
func (b *Batch) SetScope(scope pcommon.InstrumentationScope, scopeURL string) {
	b.scopePairs = sortedPairs(scope.Attributes(), b.scopePairs)
	b.scopeMap = orderedMapFromPairs(b.scopePairs)
	b.scopeName = scope.Name()
	b.scopeVersion = scope.Version()
	b.scopeURL = scopeURL

	b.h.truncate(b.resEnd)
	b.h.writeByte(sectionScope)
	b.h.writeString(b.scopeName)
	b.h.writeString(b.scopeVersion)
	b.h.writeAttrs(sectionScope, b.scopePairs)
	b.scopeEnd = b.h.len()
}

// AddMetric converts one metric (all its data points) into batch rows.
func (b *Batch) AddMetric(metric pmetric.Metric) error {
	b.families[familyKey{name: metric.Name(), metricType: metricTypeString(metric.Type()), unit: metric.Unit()}] = metric.Description()

	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		b.addNumberPoints(metric, metricTypeGauge, temporalityUnspecified, false, metric.Gauge().DataPoints())
	case pmetric.MetricTypeSum:
		sum := metric.Sum()
		b.addNumberPoints(metric, metricTypeSum, temporalityString(sum.AggregationTemporality()), sum.IsMonotonic(), sum.DataPoints())
	case pmetric.MetricTypeHistogram:
		b.addHistogramPoints(metric)
	case pmetric.MetricTypeExponentialHistogram:
		b.addExpHistogramPoints(metric)
	case pmetric.MetricTypeSummary:
		b.addSummaryPoints(metric)
	default:
		return fmt.Errorf("unsupported metric type %q", metric.Type())
	}

	return nil
}

// finishPoint finalizes the hash for a data point whose sections (including
// any extras) have been written, then registers the series row if it is new.
func (b *Batch) finishPoint(metric pmetric.Metric, metricType, temporality string, isMonotonic bool, ts time.Time, bounds, quantiles []float64) uint64 {
	hash := b.h.sum()

	day := int32(ts.Unix() / secondsPerDay)
	key := seriesDayKey{day: day, hash: hash}
	if _, ok := b.seriesSeen[key]; ok {
		return hash
	}
	b.seriesSeen[key] = struct{}{}
	if b.cache.Has(day, hash) {
		return hash
	}

	b.newSeries[day] = append(b.newSeries[day], hash)
	b.series = append(b.series, seriesRow{
		date:         time.Unix(int64(day)*secondsPerDay, 0).UTC(),
		metricName:   metric.Name(),
		hash:         hash,
		serviceName:  b.serviceName,
		metricType:   metricType,
		temporality:  temporality,
		isMonotonic:  isMonotonic,
		unit:         metric.Unit(),
		scopeName:    b.scopeName,
		scopeVersion: b.scopeVersion,
		resURL:       b.resURL,
		scopeURL:     b.scopeURL,
		resAttrs:     b.resMap,
		scopeAttrs:   b.scopeMap,
		attrs:        orderedMapFromPairs(b.dpPairs),
		bounds:       bounds,
		quantiles:    quantiles,
		seen:         ts,
	})
	return hash
}

// writePointSections writes the per-point hash sections shared by all metric
// types: metric name and sorted data point attributes.
func (b *Batch) writePointSections(metric pmetric.Metric, dpAttrs pcommon.Map) {
	b.dpPairs = sortedPairs(dpAttrs, b.dpPairs)
	b.h.truncate(b.scopeEnd)
	b.h.writeByte(sectionMetricName)
	b.h.writeString(metric.Name())
	b.h.writeAttrs(sectionDataPointAttrs, b.dpPairs)
}

func (b *Batch) addNumberPoints(metric pmetric.Metric, metricType, temporality string, isMonotonic bool, dps pmetric.NumberDataPointSlice) {
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		ts := dp.Timestamp().AsTime()

		b.writePointSections(metric, dp.Attributes())
		hash := b.finishPoint(metric, metricType, temporality, isMonotonic, ts, nil, nil)

		b.floatPoints = append(b.floatPoints, floatPointRow{
			metricName: metric.Name(),
			hash:       hash,
			startTime:  dp.StartTimestamp().AsTime(),
			timestamp:  ts,
			value:      numberValue(dp),
			flags:      pointFlags(dp.Flags()),
		})
		b.addExemplars(metric.Name(), hash, dp.Exemplars())
	}
}

func (b *Batch) addHistogramPoints(metric pmetric.Metric) {
	hist := metric.Histogram()
	temporality := temporalityString(hist.AggregationTemporality())
	dps := hist.DataPoints()
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		ts := dp.Timestamp().AsTime()
		bounds := dp.ExplicitBounds().AsRaw()

		b.writePointSections(metric, dp.Attributes())
		b.h.writeFloats(sectionBounds, bounds)
		hash := b.finishPoint(metric, metricTypeHistogram, temporality, false, ts, bounds, nil)

		b.histPoints = append(b.histPoints, histPointRow{
			metricName:   metric.Name(),
			hash:         hash,
			startTime:    dp.StartTimestamp().AsTime(),
			timestamp:    ts,
			count:        dp.Count(),
			sum:          dp.Sum(),
			min:          dp.Min(),
			max:          dp.Max(),
			bucketCounts: dp.BucketCounts().AsRaw(),
			flags:        pointFlags(dp.Flags()),
		})
		b.addExemplars(metric.Name(), hash, dp.Exemplars())
	}
}

func (b *Batch) addExpHistogramPoints(metric pmetric.Metric) {
	hist := metric.ExponentialHistogram()
	temporality := temporalityString(hist.AggregationTemporality())
	dps := hist.DataPoints()
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		ts := dp.Timestamp().AsTime()

		b.writePointSections(metric, dp.Attributes())
		hash := b.finishPoint(metric, metricTypeExpHistogram, temporality, false, ts, nil, nil)

		b.expHistPoints = append(b.expHistPoints, expHistPointRow{
			metricName:     metric.Name(),
			hash:           hash,
			startTime:      dp.StartTimestamp().AsTime(),
			timestamp:      ts,
			count:          dp.Count(),
			sum:            dp.Sum(),
			min:            dp.Min(),
			max:            dp.Max(),
			scale:          int8(dp.Scale()),
			zeroCount:      dp.ZeroCount(),
			zeroThreshold:  dp.ZeroThreshold(),
			positiveOffset: dp.Positive().Offset(),
			positiveCounts: dp.Positive().BucketCounts().AsRaw(),
			negativeOffset: dp.Negative().Offset(),
			negativeCounts: dp.Negative().BucketCounts().AsRaw(),
			flags:          pointFlags(dp.Flags()),
		})
		b.addExemplars(metric.Name(), hash, dp.Exemplars())
	}
}

func (b *Batch) addSummaryPoints(metric pmetric.Metric) {
	dps := metric.Summary().DataPoints()
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		ts := dp.Timestamp().AsTime()
		quantiles, values := sortedQuantiles(dp.QuantileValues())

		b.writePointSections(metric, dp.Attributes())
		b.h.writeFloats(sectionQuantiles, quantiles)
		hash := b.finishPoint(metric, metricTypeSummary, temporalityUnspecified, false, ts, nil, quantiles)

		b.summaryPoints = append(b.summaryPoints, summaryPointRow{
			metricName:     metric.Name(),
			hash:           hash,
			startTime:      dp.StartTimestamp().AsTime(),
			timestamp:      ts,
			count:          dp.Count(),
			sum:            dp.Sum(),
			quantileValues: values,
			flags:          pointFlags(dp.Flags()),
		})
	}
}

func (b *Batch) addExemplars(metricName string, hash uint64, exemplars pmetric.ExemplarSlice) {
	for i := 0; i < exemplars.Len(); i++ {
		ex := exemplars.At(i)
		b.exPairs = sortedPairs(ex.FilteredAttributes(), b.exPairs)
		traceID, spanID := ex.TraceID(), ex.SpanID()
		b.exemplars = append(b.exemplars, exemplarRow{
			metricName: metricName,
			hash:       hash,
			timestamp:  ex.Timestamp().AsTime(),
			value:      exemplarValue(ex),
			traceID:    hex.EncodeToString(traceID[:]),
			spanID:     hex.EncodeToString(spanID[:]),
			attrs:      orderedMapFromPairs(b.exPairs),
		})
	}
}

// Insert writes all accumulated rows, one concurrent insert per non-empty
// table. Newly seen series are committed to the cache only after the series
// insert succeeds, so a failed (and retried) push cannot leave the cache
// claiming rows that never landed.
//
// Point and exemplar inserts carry a deterministic insert_deduplication_token
// derived from the batch contents: the tables set a non-replicated dedup
// window, so a retried push (exporterhelper retries the whole batch after a
// partial failure) cannot double-insert into tables that already succeeded —
// and since deduplicated blocks do not fire materialized views, the rollups
// stay exact under retries too. Series and families tables are naturally
// retry-safe (Aggregating/ReplacingMergeTree collapse duplicates).
func (b *Batch) Insert(ctx context.Context, db driver.Conn, sqls InsertSQLs) error {
	inserts := []struct {
		name  string
		sql   string
		count int
		token string
		fn    func(driver.Batch) error
	}{
		{"points", sqls.Points, len(b.floatPoints), b.floatPointsToken(), b.appendFloatPoints},
		{"histogram points", sqls.HistPoints, len(b.histPoints), b.histPointsToken(), b.appendHistPoints},
		{"exp histogram points", sqls.ExpHistPoints, len(b.expHistPoints), b.expHistPointsToken(), b.appendExpHistPoints},
		{"summary points", sqls.SummaryPoints, len(b.summaryPoints), b.summaryPointsToken(), b.appendSummaryPoints},
		{"exemplars", sqls.Exemplars, len(b.exemplars), b.exemplarsToken(), b.appendExemplars},
		{"families", sqls.Families, len(b.families), "", b.appendFamilies},
	}

	var wg sync.WaitGroup
	errsChan := make(chan error, len(inserts)+1) // every sender must have a slot: wg.Wait() runs before any receive
	var seriesErr error

	if len(b.series) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			seriesErr = b.insertSeries(ctx, db, sqls.Series)
			errsChan <- seriesErr
		}()
	}
	for _, ins := range inserts {
		if ins.count == 0 {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			errsChan <- b.insertRows(ctx, db, ins.name, ins.sql, ins.token, ins.fn)
		}()
	}
	wg.Wait()
	close(errsChan)

	if seriesErr == nil {
		dropped := 0
		for day, hashes := range b.newSeries {
			dropped += b.cache.Add(day, hashes)
		}
		if dropped > 0 {
			b.logger.Warn("series cache saturated; uncached series will re-write series rows every push (harmless but write-amplifying)",
				zap.Int("dropped_entries", dropped),
				zap.String("hint", "raise metrics_v2::series_cache_size to at least the number of active series"))
		}
	}

	var errs error
	for err := range errsChan {
		errs = errors.Join(errs, err)
	}
	return errs
}

// dedupToken builds a deterministic token for a batch of rows from boundary
// values. Conversion is deterministic for a given OTLP payload, so a retried
// push produces the identical token while distinct batches diverge in row
// count, boundary series, timestamps, or boundary values.
func dedupToken(parts ...uint64) string {
	buf := make([]byte, 0, len(parts)*8)
	for _, p := range parts {
		buf = binary.LittleEndian.AppendUint64(buf, p)
	}
	return fmt.Sprintf("otelv2-%016x", city.CH64(buf))
}

func (b *Batch) floatPointsToken() string {
	if len(b.floatPoints) == 0 {
		return ""
	}
	f, l := &b.floatPoints[0], &b.floatPoints[len(b.floatPoints)-1]
	return dedupToken(uint64(len(b.floatPoints)),
		f.hash, uint64(f.timestamp.UnixMilli()), math.Float64bits(f.value),
		l.hash, uint64(l.timestamp.UnixMilli()), math.Float64bits(l.value))
}

func (b *Batch) histPointsToken() string {
	if len(b.histPoints) == 0 {
		return ""
	}
	f, l := &b.histPoints[0], &b.histPoints[len(b.histPoints)-1]
	return dedupToken(uint64(len(b.histPoints)),
		f.hash, uint64(f.timestamp.UnixMilli()), f.count,
		l.hash, uint64(l.timestamp.UnixMilli()), l.count)
}

func (b *Batch) expHistPointsToken() string {
	if len(b.expHistPoints) == 0 {
		return ""
	}
	f, l := &b.expHistPoints[0], &b.expHistPoints[len(b.expHistPoints)-1]
	return dedupToken(uint64(len(b.expHistPoints)),
		f.hash, uint64(f.timestamp.UnixMilli()), f.count,
		l.hash, uint64(l.timestamp.UnixMilli()), l.count)
}

func (b *Batch) summaryPointsToken() string {
	if len(b.summaryPoints) == 0 {
		return ""
	}
	f, l := &b.summaryPoints[0], &b.summaryPoints[len(b.summaryPoints)-1]
	return dedupToken(uint64(len(b.summaryPoints)),
		f.hash, uint64(f.timestamp.UnixMilli()), f.count,
		l.hash, uint64(l.timestamp.UnixMilli()), l.count)
}

func (b *Batch) exemplarsToken() string {
	if len(b.exemplars) == 0 {
		return ""
	}
	f, l := &b.exemplars[0], &b.exemplars[len(b.exemplars)-1]
	return dedupToken(uint64(len(b.exemplars)),
		f.hash, uint64(f.timestamp.UnixMilli()), math.Float64bits(f.value),
		l.hash, uint64(l.timestamp.UnixMilli()), math.Float64bits(l.value))
}

func (b *Batch) insertRows(ctx context.Context, db driver.Conn, name, sql, token string, appendFn func(driver.Batch) error) error {
	if token != "" {
		// The token alone protects only the raw table: materialized views
		// still fire for a deduplicated source insert (verified on 26.2), so
		// the rollups would double-count on retries without the second
		// setting, which extends token-based dedup through the MV chain.
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
			"insert_deduplication_token":                         token,
			"deduplicate_blocks_in_dependent_materialized_views": 1,
		}))
	}
	processStart := time.Now()
	batch, err := db.PrepareBatch(ctx, sql)
	if err != nil {
		return fmt.Errorf("prepare %s batch: %w", name, err)
	}
	defer func() {
		if closeErr := batch.Close(); closeErr != nil {
			b.logger.Warn("failed to close batch", zap.String("table", name), zap.Error(closeErr))
		}
	}()

	if err := appendFn(batch); err != nil {
		return fmt.Errorf("append %s rows: %w", name, err)
	}
	rows := batch.Rows()
	if err := batch.Send(); err != nil {
		return fmt.Errorf("%s insert failed: %w", name, err)
	}

	b.logger.Debug("insert metrics v2 rows",
		zap.String("table", name),
		zap.Int("records", rows),
		zap.String("cost", time.Since(processStart).String()))
	return nil
}

func (b *Batch) insertSeries(ctx context.Context, db driver.Conn, sql string) error {
	return b.insertRows(ctx, db, "series", sql, "", func(batch driver.Batch) error {
		for i := range b.series {
			s := &b.series[i]
			if err := batch.Append(
				s.date,
				s.metricName,
				s.hash,
				s.serviceName,
				s.metricType,
				s.temporality,
				s.isMonotonic,
				s.unit,
				s.scopeName,
				s.scopeVersion,
				s.resURL,
				s.scopeURL,
				s.resAttrs,
				s.scopeAttrs,
				s.attrs,
				s.bounds,
				s.quantiles,
				s.seen,
				s.seen,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *Batch) appendFloatPoints(batch driver.Batch) error {
	for i := range b.floatPoints {
		p := &b.floatPoints[i]
		if err := batch.Append(p.metricName, p.hash, p.startTime, p.timestamp, p.value, p.flags); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) appendHistPoints(batch driver.Batch) error {
	for i := range b.histPoints {
		p := &b.histPoints[i]
		if err := batch.Append(
			p.metricName, p.hash, p.startTime, p.timestamp,
			p.count, p.sum, p.min, p.max, p.bucketCounts, p.flags,
		); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) appendExpHistPoints(batch driver.Batch) error {
	for i := range b.expHistPoints {
		p := &b.expHistPoints[i]
		if err := batch.Append(
			p.metricName, p.hash, p.startTime, p.timestamp,
			p.count, p.sum, p.min, p.max,
			p.scale, p.zeroCount, p.zeroThreshold,
			p.positiveOffset, p.positiveCounts,
			p.negativeOffset, p.negativeCounts,
			p.flags,
		); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) appendSummaryPoints(batch driver.Batch) error {
	for i := range b.summaryPoints {
		p := &b.summaryPoints[i]
		if err := batch.Append(
			p.metricName, p.hash, p.startTime, p.timestamp,
			p.count, p.sum, p.quantileValues, p.flags,
		); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) appendExemplars(batch driver.Batch) error {
	for i := range b.exemplars {
		e := &b.exemplars[i]
		if err := batch.Append(e.metricName, e.hash, e.timestamp, e.value, e.traceID, e.spanID, e.attrs); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) appendFamilies(batch driver.Batch) error {
	for key, description := range b.families {
		if err := batch.Append(key.name, key.metricType, key.unit, description); err != nil {
			return err
		}
	}
	return nil
}

func metricTypeString(t pmetric.MetricType) string {
	switch t {
	case pmetric.MetricTypeGauge:
		return metricTypeGauge
	case pmetric.MetricTypeSum:
		return metricTypeSum
	case pmetric.MetricTypeHistogram:
		return metricTypeHistogram
	case pmetric.MetricTypeExponentialHistogram:
		return metricTypeExpHistogram
	case pmetric.MetricTypeSummary:
		return metricTypeSummary
	default:
		return ""
	}
}

func temporalityString(t pmetric.AggregationTemporality) string {
	switch t {
	case pmetric.AggregationTemporalityDelta:
		return temporalityDelta
	case pmetric.AggregationTemporalityCumulative:
		return temporalityCumulative
	default:
		return temporalityUnspecified
	}
}

func numberValue(dp pmetric.NumberDataPoint) float64 {
	if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
		return float64(dp.IntValue())
	}
	return dp.DoubleValue()
}

func exemplarValue(ex pmetric.Exemplar) float64 {
	if ex.ValueType() == pmetric.ExemplarValueTypeInt {
		return float64(ex.IntValue())
	}
	return ex.DoubleValue()
}

func pointFlags(flags pmetric.DataPointFlags) uint8 {
	return uint8(uint32(flags) & 0xFF)
}

// sortedQuantiles extracts (quantile, value) pairs sorted by quantile level.
// The quantile levels are part of the series identity; the values are stored
// positionally on the data point row.
func sortedQuantiles(qs pmetric.SummaryDataPointValueAtQuantileSlice) ([]float64, []float64) {
	n := qs.Len()
	if n == 0 {
		return nil, nil
	}
	type qv struct{ q, v float64 }
	pairs := make([]qv, 0, n)
	for i := 0; i < n; i++ {
		pairs = append(pairs, qv{q: qs.At(i).Quantile(), v: qs.At(i).Value()})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].q < pairs[j].q })
	quantiles := make([]float64, n)
	values := make([]float64, n)
	for i, p := range pairs {
		quantiles[i] = p.q
		values[i] = p.v
	}
	return quantiles, values
}
