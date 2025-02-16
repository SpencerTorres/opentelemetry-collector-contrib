package chgo

import "github.com/ClickHouse/ch-go/proto"

func newLowCardinalityString(strSize, bufSize int) *proto.ColLowCardinality[string] {
	lc := proto.NewLowCardinality[string](&proto.ColStr{
		Buf: make([]byte, 0, strSize*bufSize),
		Pos: make([]proto.Position, 0, bufSize),
	})
	lc.Values = make([]string, 0, bufSize)

	return lc
}
