package extractor

import (
	"errors"
	"reflect"

	"github.com/chtzvt/ctsnarf/internal/etl_core"
	ct "github.com/google/certificate-transparency-go"
)

type CertFieldsExtractor struct{}

func (e *CertFieldsExtractor) Extract(ctx *etl_core.Context, raw *ct.RawLogEntry) (map[string]interface{}, error) {
	if raw == nil {
		return nil, errors.New("got a nil log entry")
	}

	if reflect.DeepEqual(raw.Leaf, ct.MerkleTreeLeaf{}) {
		return nil, errors.New("got log entry with unset Leaf")
	}

	entry := make(map[string]interface{})
	parsed, err := raw.ToLogEntry()
	if err != nil {
		return nil, err
	}

	if parsed.X509Cert != nil {
		entry["subject"] = parsed.X509Cert.Subject.String()
		entry["issuer"] = parsed.X509Cert.Issuer.String()
		// add more fields as needed
	} else if parsed.Precert != nil {
		entry["subject"] = parsed.Precert.TBSCertificate.Subject.String()
		entry["issuer"] = parsed.Precert.TBSCertificate.Issuer.String()
	}
	return entry, nil
}

func init() {
	Register("cert_fields", &CertFieldsExtractor{})
}
