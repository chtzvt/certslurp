package etl

import (
	"fmt"

	"github.com/chtzvt/ctsnarf/internal/etl_core"
	"github.com/chtzvt/ctsnarf/internal/extractor"
	"github.com/chtzvt/ctsnarf/internal/job"
	"github.com/chtzvt/ctsnarf/internal/secrets"
	"github.com/chtzvt/ctsnarf/internal/sink"
	"github.com/chtzvt/ctsnarf/internal/transformer"
)

// Pipeline orchestrates the ETL process for a stream of records, with chunking support.
type Pipeline struct {
	Extractor     extractor.Extractor
	Transformer   transformer.Transformer
	Sink          sink.Sink
	Ctx           *etl_core.Context
	MaxChunkBytes int // 0 means unlimited
	MaxChunkRecs  int // 0 means unlimited
	BaseName      string
}

func NewPipeline(spec *job.JobSpec, secrets *secrets.Store, baseName string) (*Pipeline, error) {
	ext, err := extractor.ForName(spec.Options.Output.Extractor)
	if err != nil {
		return nil, fmt.Errorf("extractor: %w", err)
	}
	tr, err := transformer.ForName(spec.Options.Output.Transformer)
	if err != nil {
		return nil, fmt.Errorf("transformer: %w", err)
	}
	sinkFactory, ok := sink.ForName(spec.Options.Output.Sink)
	if !ok {
		return nil, fmt.Errorf("sink: not found: %s", spec.Options.Output.Sink)
	}
	sinkInst, err := sinkFactory(spec.Options.Output.SinkOptions, secrets)
	if err != nil {
		return nil, fmt.Errorf("sink init: %w", err)
	}
	return &Pipeline{
		Extractor:     ext,
		Transformer:   tr,
		Sink:          sinkInst,
		Ctx:           &etl_core.Context{Spec: spec},
		BaseName:      baseName,
		MaxChunkBytes: spec.Options.Output.ChunkBytes,
		MaxChunkRecs:  spec.Options.Output.ChunkRecords,
	}, nil
}
