package extractor

import (
	"github.com/chtzvt/ctsnarf/internal/etl_core"
	ct "github.com/google/certificate-transparency-go"
)

type RawExtractor struct{}

func (e *RawExtractor) Extract(ctx *etl_core.Context, raw *ct.RawLogEntry) (map[string]interface{}, error) {
	return map[string]interface{}{"raw": raw.Cert.Data}, nil
}

func init() {
	Register("raw", &RawExtractor{})
}
