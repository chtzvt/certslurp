package worker

import (
	"testing"

	"github.com/chtzvt/certslurp/internal/job"
	"github.com/google/certificate-transparency-go/scanner"
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
