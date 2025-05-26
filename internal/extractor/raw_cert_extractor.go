package extractor

import (
	"github.com/chtzvt/ctsnarf/internal/etl_core"
	ct "github.com/google/certificate-transparency-go"
)

type RawCertExtractor struct{}

func (e *RawCertExtractor) Extract(ctx *etl_core.Context, raw *ct.RawLogEntry) (map[string]interface{}, error) {
	return map[string]interface{}{"raw_cert": raw.Cert}, nil
}

func init() {
	Register("raw_cert", &RawCertExtractor{})
}
