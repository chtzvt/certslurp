package etl

import (
	"fmt"

	"github.com/chtzvt/ctsnarf/internal/etl_core"
	"github.com/chtzvt/ctsnarf/internal/extractor"
	"github.com/chtzvt/ctsnarf/internal/sink"
	"github.com/chtzvt/ctsnarf/internal/transformer"
)

type Pipeline struct {
	Extractor   extractor.Extractor
	Transformer transformer.Transformer
	Sink        sink.Sink
	Ctx         *etl_core.Context
}

func (p *Pipeline) Process(rawEntry interface{}) error {
	extracted, err := p.Extractor.Extract(p.Ctx, rawEntry)
	if err != nil {
		return fmt.Errorf("extract: %w", err)
	}
	transformed, err := p.Transformer.Transform(p.Ctx, extracted)
	if err != nil {
		return fmt.Errorf("transform: %w", err)
	}
	if err := p.Sink.Write(p.Ctx, transformed); err != nil {
		return fmt.Errorf("sink: %w", err)
	}
	return nil
}

func (p *Pipeline) ProcessStream(entries <-chan interface{}) error {
	for raw := range entries {
		if err := p.Process(raw); err != nil {
			return err // Or collect/report errors, as needed
		}
	}
	return nil
}

func NewPipeline(ctx *etl_core.Context) (*Pipeline, error) {
	// Instantiate the components based on JobSpec in ctx.Spec
	extractor, err := extractor.ForName(ctx.Spec.Options.Output.Extractor.Name)
	if err != nil {
		return nil, err
	}
	transformer, err := transformer.ForName(ctx.Spec.Options.Output.Transformer.Name)
	if err != nil {
		return nil, err
	}
	sink, err := sink.ForName(ctx.Spec.Options.Output.Target.Name)
	if err != nil {
		return nil, err
	}
	return &Pipeline{
		Extractor:   extractor,
		Transformer: transformer,
		Sink:        sink,
		Ctx:         ctx,
	}, nil
}
