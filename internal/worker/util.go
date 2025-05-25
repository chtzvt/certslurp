package worker

import (
	"context"
	"math/big"
	"regexp"

	"github.com/chtzvt/ctsnarf/internal/job"
	"github.com/google/certificate-transparency-go/client"
	"github.com/google/certificate-transparency-go/scanner"
)

// buildMatcher creates a Matcher (or LeafMatcher) and optional initialization.
// Returns (matcher, initFunc). initFunc may be nil unless matcher requires it.
func buildMatcher(cfg job.MatchConfig) (interface{}, func(context.Context, *client.LogClient) error) {
	// Match by subject regex
	if cfg.SubjectRegex != "" {
		r := regexp.MustCompile(cfg.SubjectRegex)
		return &scanner.MatchSubjectRegex{
			CertificateSubjectRegex:    r,
			PrecertificateSubjectRegex: r,
		}, nil
	}
	// Match by issuer regex
	if cfg.IssuerRegex != "" {
		r := regexp.MustCompile(cfg.IssuerRegex)
		return &scanner.MatchIssuerRegex{
			CertificateIssuerRegex:    r,
			PrecertificateIssuerRegex: r,
		}, nil
	}
	// Match by serial number
	if cfg.Serial != "" {
		var sn big.Int
		_, ok := sn.SetString(cfg.Serial, 10)
		if !ok {
			return &scanner.MatchNone{}, nil // Defensive fallback
		}
		return &scanner.MatchSerialNumber{SerialNumber: sn}, nil
	}
	// Match by SCT timestamp (as LeafMatcher)
	if cfg.SCTTimestamp != 0 {
		return scanner.MatchSCTTimestamp{Timestamp: cfg.SCTTimestamp}, nil
	}
	// Parse errors (LeafMatcher)
	if cfg.ParseErrors != "" {
		return &scanner.CertParseFailMatcher{MatchNonFatalErrs: cfg.ParseErrors == "all"}, nil
	}
	// Validation errors (LeafMatcher with required root pool setup)
	if cfg.ValidationErrors {
		m := &scanner.CertVerifyFailMatcher{}
		initFunc := func(ctx context.Context, cl *client.LogClient) error {
			m.PopulateRoots(ctx, cl)
			return nil
		}
		return m, initFunc
	}
	// Default: match everything
	return &scanner.MatchAll{}, nil
}
