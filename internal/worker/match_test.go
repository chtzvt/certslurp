package worker

import (
	"regexp"
	"testing"

	"github.com/chtzvt/certslurp/internal/job"
	ct "github.com/google/certificate-transparency-go"
	"github.com/google/certificate-transparency-go/scanner"
	x509 "github.com/google/certificate-transparency-go/x509"
	"github.com/google/certificate-transparency-go/x509/pkix"
)

func TestBuildMatcher_SubjectRegex(t *testing.T) {
	cfg := job.MatchConfig{SubjectRegex: "CN=foo"}
	matcher, _ := buildMatcher(cfg)
	m, ok := matcher.(*scanner.MatchSubjectRegex)
	if !ok || m == nil {
		t.Fatalf("Expected MatchSubjectRegex, got %T", matcher)
	}
}

func TestBuildMatcher_IssuerRegex(t *testing.T) {
	cfg := job.MatchConfig{IssuerRegex: "C=US"}
	matcher, _ := buildMatcher(cfg)
	m, ok := matcher.(*scanner.MatchIssuerRegex)
	if !ok || m == nil {
		t.Fatalf("Expected MatchIssuerRegex, got %T", matcher)
	}
}

func TestBuildMatcher_Serial(t *testing.T) {
	cfg := job.MatchConfig{Serial: "12345"}
	matcher, _ := buildMatcher(cfg)
	m, ok := matcher.(*scanner.MatchSerialNumber)
	if !ok || m == nil {
		t.Fatalf("Expected MatchSerialNumber, got %T", matcher)
	}
}

func TestBuildMatcher_SCTTimestamp(t *testing.T) {
	cfg := job.MatchConfig{SCTTimestamp: 9876543210}
	matcher, _ := buildMatcher(cfg)
	_, ok := matcher.(scanner.MatchSCTTimestamp)
	if !ok {
		t.Fatalf("Expected MatchSCTTimestamp, got %T", matcher)
	}
}

func TestBuildMatcher_ParseErrors(t *testing.T) {
	cfg := job.MatchConfig{ParseErrors: "all"}
	matcher, _ := buildMatcher(cfg)
	m, ok := matcher.(*scanner.CertParseFailMatcher)
	if !ok || m == nil {
		t.Fatalf("Expected CertParseFailMatcher, got %T", matcher)
	}
	if !m.MatchNonFatalErrs {
		t.Fatal("Expected MatchNonFatalErrs = true")
	}
}

func TestBuildMatcher_ValidationErrors(t *testing.T) {
	cfg := job.MatchConfig{ValidationErrors: true}
	matcher, initFunc := buildMatcher(cfg)
	_, ok := matcher.(*scanner.CertVerifyFailMatcher)
	if !ok {
		t.Fatalf("Expected CertVerifyFailMatcher, got %T", matcher)
	}
	if initFunc == nil {
		t.Fatal("Expected non-nil initFunc for validation errors")
	}
}

func TestBuildMatcher_Default(t *testing.T) {
	cfg := job.MatchConfig{}
	matcher, _ := buildMatcher(cfg)
	if _, ok := matcher.(scanner.MatchAll); !ok {
		t.Fatalf("Expected MatchAll, got %T", matcher)
	}
}

func TestBuildMatcher_SkipPrecerts(t *testing.T) {
	cfg := job.MatchConfig{SkipPrecerts: true}
	matcher, _ := buildMatcher(cfg)
	s, ok := matcher.(SkipPrecerts)
	if !ok {
		t.Fatalf("Expected SkipPrecerts, got %T", matcher)
	}
	// The inner matcher should be MatchAll by default
	if _, ok := s.Inner.(scanner.MatchAll); !ok {
		t.Fatalf("Expected SkipPrecerts.Inner to be MatchAll, got %T", s.Inner)
	}
}

func TestBuildMatcher_SkipPrecerts_WrapsOther(t *testing.T) {
	cfg := job.MatchConfig{SkipPrecerts: true, SubjectRegex: "CN=test"}
	matcher, _ := buildMatcher(cfg)
	s, ok := matcher.(SkipPrecerts)
	if !ok {
		t.Fatalf("Expected SkipPrecerts, got %T", matcher)
	}
	if _, ok := s.Inner.(*scanner.MatchSubjectRegex); !ok {
		t.Fatalf("Expected inner to be MatchSubjectRegex, got %T", s.Inner)
	}
}

func TestBuildMatcher_Domain(t *testing.T) {
	cfg := job.MatchConfig{DomainInclude: `^foo\.example\.com$`}
	matcher, _ := buildMatcher(cfg)
	m, ok := matcher.(MatchDomainRegex)
	if !ok {
		t.Fatalf("Expected MatchDomainRegex, got %T", matcher)
	}
	if !m.Include.MatchString("foo.example.com") {
		t.Fatal("DomainRegex does not match foo.example.com")
	}
}

func TestMatchDomainRegex_CertificateMatches(t *testing.T) {
	m := MatchDomainRegex{Include: regexp.MustCompile(`\.example\.com$`)}
	cert := &x509.Certificate{
		DNSNames: []string{"foo.example.com", "bar.notme.org"},
		Subject:  pkix.Name{CommonName: "backup.example.com"},
	}
	if !m.CertificateMatches(cert) {
		t.Error("Expected CertificateMatches to match on DNSNames")
	}

	// Should match on CommonName if no SANs present
	certNoSAN := &x509.Certificate{
		DNSNames: nil,
		Subject:  pkix.Name{CommonName: "backup.example.com"},
	}
	if !m.CertificateMatches(certNoSAN) {
		t.Error("Expected CertificateMatches to match on CommonName")
	}

	// Should not match if neither matches
	certFail := &x509.Certificate{
		DNSNames: []string{"something.org"},
		Subject:  pkix.Name{CommonName: "somethingelse.org"},
	}
	if m.CertificateMatches(certFail) {
		t.Error("Did not expect CertificateMatches to match")
	}
}

func TestMatchDomainRegex_PrecertificateMatches(t *testing.T) {
	m := MatchDomainRegex{Include: regexp.MustCompile(`\.example\.com$`)}
	pre := &ct.Precertificate{
		TBSCertificate: &x509.Certificate{
			DNSNames: []string{"foo.example.com", "other.org"},
			Subject:  pkix.Name{CommonName: "cn.example.com"},
		},
	}
	if !m.PrecertificateMatches(pre) {
		t.Error("Expected PrecertificateMatches to match on DNSNames")
	}

	preNoSAN := &ct.Precertificate{
		TBSCertificate: &x509.Certificate{
			DNSNames: nil,
			Subject:  pkix.Name{CommonName: "cn.example.com"},
		},
	}
	if !m.PrecertificateMatches(preNoSAN) {
		t.Error("Expected PrecertificateMatches to match on CommonName")
	}

	preFail := &ct.Precertificate{
		TBSCertificate: &x509.Certificate{
			DNSNames: []string{"other.org"},
			Subject:  pkix.Name{CommonName: "another.org"},
		},
	}
	if m.PrecertificateMatches(preFail) {
		t.Error("Did not expect PrecertificateMatches to match")
	}
}

func TestMatchDomainRegex(t *testing.T) {
	m := MatchDomainRegex{
		Include: regexp.MustCompile(`\.example\.com$`),
		Exclude: regexp.MustCompile(`^foo\.example\.com$`),
	}

	// Should be excluded: contains a DNS name that matches Exclude, even though bar.example.com would match Include.
	cert := &x509.Certificate{
		DNSNames: []string{"foo.example.com", "bar.example.com", "baz.notme.org"},
		Subject:  pkix.Name{CommonName: "fallback.example.com"},
	}
	if m.CertificateMatches(cert) {
		t.Error("Did not expect CertificateMatches to match when any SAN is excluded")
	}

	// Should be excluded: only DNS is in both include and exclude
	certNoMatch := &x509.Certificate{
		DNSNames: []string{"foo.example.com"},
	}
	if m.CertificateMatches(certNoMatch) {
		t.Error("Did not expect CertificateMatches to match when only DNS is excluded")
	}

	// Should not match: doesn't match include at all
	certOther := &x509.Certificate{
		DNSNames: []string{"otherdomain.org"},
	}
	if m.CertificateMatches(certOther) {
		t.Error("Did not expect CertificateMatches to match when not included")
	}

	// Should match: included and not excluded
	certIncluded := &x509.Certificate{
		DNSNames: []string{"bar.example.com"},
	}
	if !m.CertificateMatches(certIncluded) {
		t.Error("Expected CertificateMatches to match bar.example.com (included, not excluded)")
	}
}

func TestBuildMatcher_DomainIncludeExclude(t *testing.T) {
	cfg := job.MatchConfig{
		DomainInclude: `\.example\.com$`,
		DomainExclude: `^foo\.example\.com$`,
	}
	matcher, _ := buildMatcher(cfg)
	m, ok := matcher.(MatchDomainRegex)
	if !ok {
		t.Fatalf("Expected MatchDomainRegex, got %T", matcher)
	}
	if m.Include == nil || m.Exclude == nil {
		t.Fatal("Expected both Include and Exclude regex to be set")
	}
}

func TestMatchDomainRegex_ExcludeOnly(t *testing.T) {
	m := MatchDomainRegex{
		Exclude: regexp.MustCompile(`^bar\.example\.com$`),
	}

	// Should NOT match: one SAN is excluded ("bar.example.com")
	cert := &x509.Certificate{
		DNSNames: []string{"foo.example.com", "bar.example.com"},
	}
	if m.CertificateMatches(cert) {
		t.Error("Did not expect CertificateMatches to match when any SAN is excluded")
	}

	// Should NOT match: the only SAN is excluded
	cert2 := &x509.Certificate{
		DNSNames: []string{"bar.example.com"},
	}
	if m.CertificateMatches(cert2) {
		t.Error("Did not expect CertificateMatches to match bar.example.com")
	}

	// Should match: no SANs are excluded
	cert3 := &x509.Certificate{
		DNSNames: []string{"foo.example.com", "baz.example.com"},
	}
	if !m.CertificateMatches(cert3) {
		t.Error("Expected CertificateMatches to match when no SANs are excluded")
	}
}
