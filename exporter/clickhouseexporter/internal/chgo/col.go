package chgo

import (
	"github.com/ClickHouse/ch-go/proto"
	"time"
)

func newColLowCardinalityString(strSize, batchSize int) *proto.ColLowCardinality[string] {
	lc := proto.NewLowCardinality[string](&proto.ColStr{
		Buf: make([]byte, 0, strSize*batchSize),
		Pos: make([]proto.Position, 0, batchSize),
	})
	lc.Values = make([]string, 0, batchSize)

	return lc
}

func newColArrayLowCardinalityString(strSize, batchSize int) *proto.ColArr[string] {
	col := newColLowCardinalityString(strSize, batchSize)
	return &proto.ColArr[string]{
		Offsets: make(proto.ColUInt64, 0, batchSize),
		Data:    col,
	}
}

func newColString(strSize, batchSize int) proto.ColStr {
	return proto.ColStr{
		Buf: make([]byte, 0, strSize*batchSize),
		Pos: make([]proto.Position, 0, batchSize),
	}
}

func newColArrayString(strSize, batchSize int) *proto.ColArr[string] {
	col := newColString(strSize, batchSize)
	return &proto.ColArr[string]{
		Offsets: make(proto.ColUInt64, 0, batchSize),
		Data:    &col,
	}
}

func newColBytes(strSize, batchSize int) proto.ColBytes {
	return proto.ColBytes{
		ColStr: newColString(strSize, batchSize),
	}
}

func newColArrayBytes(strSize, batchSize int) *proto.ColArr[[]byte] {
	col := newColBytes(strSize, batchSize)
	return &proto.ColArr[[]byte]{
		Offsets: make(proto.ColUInt64, 0, batchSize),
		Data:    &col,
	}
}

func newColJSONBytes(jsonSize, batchSize int) proto.ColJSONBytes {
	return proto.ColJSONBytes{
		ColJSONStr: proto.ColJSONStr{
			Str: proto.ColStr{
				Buf: make([]byte, 0, jsonSize*batchSize),
				Pos: make([]proto.Position, 0, batchSize),
			},
		},
	}
}

func newColArrayJSONBytes(jsonSize, batchSize int) *proto.ColArr[[]byte] {
	col := newColJSONBytes(jsonSize, batchSize)
	return &proto.ColArr[[]byte]{
		Offsets: make(proto.ColUInt64, 0, batchSize),
		Data:    &col,
	}
}

func newColDateTime64Raw(batchSize int) proto.ColDateTime64Raw {
	return proto.ColDateTime64Raw{
		ColDateTime64: proto.ColDateTime64{
			Data:         make([]proto.DateTime64, 0, batchSize),
			Location:     time.UTC,
			Precision:    proto.PrecisionNano,
			PrecisionSet: true,
		},
	}
}

func newColArrayDateTime64Raw(batchSize int) *proto.ColArr[proto.DateTime64] {
	col := newColDateTime64Raw(batchSize)
	return &proto.ColArr[proto.DateTime64]{
		Offsets: make(proto.ColUInt64, 0, batchSize),
		Data:    &col,
	}
}
