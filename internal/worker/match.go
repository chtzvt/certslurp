package worker

import (
	"context"

	"math/big"
	"regexp"

	"github.com/chtzvt/certslurp/internal/job"
	ct "github.com/google/certificate-transparency-go"
	"github.com/google/certificate-transparency-go/client"
	"github.com/google/certificate-transparency-go/scanner"
	x509 "github.com/google/certificate-transparency-go/x509"
)

type SkipPrecerts struct {
	Inner scanner.Matcher
}

func (s SkipPrecerts) CertificateMatches(cert *x509.Certificate) bool {
	return s.Inner.CertificateMatches(cert)
}

func (s SkipPrecerts) PrecertificateMatches(_ *ct.Precertificate) bool {
	// Always skip precerts
	return false
}

// buildMatcher creates a Matcher (or LeafMatcher) and optional initialization.
// Returns (matcher, initFunc). initFunc may be nil unless matcher requires it.
func buildMatcher(cfg job.MatchConfig) (matcher interface{}, initFunc func(context.Context, *client.LogClient) error) {

	if cfg.ValidationErrors == true {
		vm := &scanner.CertVerifyFailMatcher{}
		initFunc = func(ctx context.Context, cl *client.LogClient) error {
			vm.PopulateRoots(ctx, cl)
			return nil
		}

		return vm, initFunc
	}

	var m scanner.Matcher

	switch {
	case cfg.SubjectRegex != "":
		r := regexp.MustCompile(cfg.SubjectRegex)
		m = &scanner.MatchSubjectRegex{
			CertificateSubjectRegex:    r,
			PrecertificateSubjectRegex: r,
		}
	case cfg.IssuerRegex != "":
		r := regexp.MustCompile(cfg.IssuerRegex)
		m = &scanner.MatchIssuerRegex{
			CertificateIssuerRegex:    r,
			PrecertificateIssuerRegex: r,
		}
	case cfg.Serial != "":
		var sn big.Int
		_, ok := sn.SetString(cfg.Serial, 10)
		if !ok {
			m = &scanner.MatchNone{}
		} else {
			m = &scanner.MatchSerialNumber{SerialNumber: sn}
		}
	case cfg.SCTTimestamp != 0:
		return scanner.MatchSCTTimestamp{Timestamp: cfg.SCTTimestamp}, nil
	case cfg.ParseErrors != "":
		return &scanner.CertParseFailMatcher{MatchNonFatalErrs: cfg.ParseErrors == "all"}, nil
	default:
		m = scanner.MatchAll{}
	}

	if cfg.SkipPrecerts {
		return SkipPrecerts{Inner: m}, initFunc
	}

	return m, initFunc
}
