package etl

import (
	"context"
	"fmt"

	"github.com/chtzvt/ctsnarf/internal/sink"
	ct "github.com/google/certificate-transparency-go"
)

// StreamProcess processes records from entries and writes to a single sink output.
// The ctx parameter is passed down to all operations, including Sink.Open.
func (p *Pipeline) StreamProcess(ctx context.Context, entries <-chan *ct.RawLogEntry) error {
	var (
		writer     sink.SinkWriter
		curBytes   int
		curRecs    int
		chunkNum   int = 1
		needHeader bool
	)
	openChunk := func() (sink.SinkWriter, error) {
		name := p.BaseName
		if p.MaxChunkBytes > 0 || p.MaxChunkRecs > 0 {
			name = fmt.Sprintf("%s.%04d", p.BaseName, chunkNum)
		}
		w, err := p.Sink.Open(ctx, name)
		if err != nil {
			return nil, err
		}
		needHeader = true
		return w, nil
	}
	closeChunk := func() error {
		// Write footer if needed
		if writer != nil {
			if footer, _ := p.Transformer.Footer(p.Ctx); len(footer) > 0 {
				if _, err := writer.Write(footer); err != nil {
					return err
				}
			}
			return writer.Close()
		}
		return nil
	}

	for entry := range entries {
		if writer == nil {
			var err error
			writer, err = openChunk()
			if err != nil {
				return fmt.Errorf("open sink: %w", err)
			}
			curBytes = 0
			curRecs = 0
			chunkNum++
		}

		if needHeader {
			if header, _ := p.Transformer.Header(p.Ctx); len(header) > 0 {
				if _, err := writer.Write(header); err != nil {
					return fmt.Errorf("header write: %w", err)
				}
			}
			needHeader = false
		}

		// Extract and transform
		extracted, err := p.Extractor.Extract(p.Ctx, entry)
		if err != nil {
			return fmt.Errorf("extract: %w", err)
		}
		data, err := p.Transformer.Transform(p.Ctx, extracted)
		if err != nil {
			return fmt.Errorf("transform: %w", err)
		}
		n, err := writer.Write(data)
		if err != nil {
			return fmt.Errorf("write: %w", err)
		}
		curBytes += n
		curRecs++

		// Should we rotate?
		rotate := false
		if p.MaxChunkBytes > 0 && curBytes >= p.MaxChunkBytes {
			rotate = true
		}
		if p.MaxChunkRecs > 0 && curRecs >= p.MaxChunkRecs {
			rotate = true
		}
		if rotate {
			if err := closeChunk(); err != nil {
				return fmt.Errorf("close sink: %w", err)
			}
			writer = nil
		}
	}

	if writer != nil {
		if err := closeChunk(); err != nil {
			return fmt.Errorf("close sink: %w", err)
		}
	}
	return nil
}
