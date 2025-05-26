package extractor

import (
	"fmt"

	"github.com/chtzvt/ctsnarf/internal/etl_core"
	ct "github.com/google/certificate-transparency-go"
)

type Extractor interface {
	Extract(ctx *etl_core.Context, raw *ct.RawLogEntry) (map[string]interface{}, error)
}

var extractors = make(map[string]Extractor)

func Register(name string, extractor Extractor) {
	extractors[name] = extractor
}

func ForName(name string) (Extractor, error) {
	ex, ok := extractors[name]
	if !ok {
		return nil, fmt.Errorf("extractor not found: %s", name)
	}
	return ex, nil
}
