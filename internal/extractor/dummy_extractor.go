package extractor

import (
	"github.com/chtzvt/ctsnarf/internal/etl_core"
	ct "github.com/google/certificate-transparency-go"
)

type DummyExtractor struct{}

func (e *DummyExtractor) Extract(ctx *etl_core.Context, raw *ct.RawLogEntry) (map[string]interface{}, error) {
	return map[string]interface{}{"raw": raw}, nil
}

func init() {
	Register("dummy", &DummyExtractor{})
}
