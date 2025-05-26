package etl

import (
	"context"
	"fmt"

	ct "github.com/google/certificate-transparency-go"
)

// StubPipeline is a test pipeline that records all transformed records and chunk boundaries.
type StubPipeline struct {
	Records   [][]byte
	ChunkEnds []int // indices in Records where a chunk ended
	CurChunk  int
}

func NewStubPipeline() *StubPipeline {
	return &StubPipeline{
		Records:   make([][]byte, 0),
		ChunkEnds: make([]int, 0),
	}
}

func (s *StubPipeline) StreamProcess(ctx context.Context, entries <-chan *ct.RawLogEntry) error {
	for entry := range entries {
		// In a real stub, just record the "transformed" entry.
		rawBytes := []byte(fmt.Sprintf("%v", entry))
		s.Records = append(s.Records, rawBytes)
		// Example: add chunk logic for test (not real splitting)
		if len(s.Records)%5 == 0 { // Every 5 records, fake a chunk end
			s.ChunkEnds = append(s.ChunkEnds, len(s.Records))
		}
	}
	return nil
}
