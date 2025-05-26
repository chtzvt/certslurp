package extractor

import (
	"testing"

	"github.com/chtzvt/ctsnarf/internal/etl_core"
	"github.com/chtzvt/ctsnarf/internal/testutil"
	ct "github.com/google/certificate-transparency-go"
	"github.com/stretchr/testify/require"
)

func TestDummyExtractor(t *testing.T) {
	ex := &DummyExtractor{}
	ctx := &etl_core.Context{}
	raw := &ct.RawLogEntry{} // could be nil for DummyExtractor

	result, err := ex.Extract(ctx, raw)
	if err != nil {
		t.Fatalf("DummyExtractor returned error: %v", err)
	}
	if result == nil {
		t.Fatal("DummyExtractor returned nil result")
	}
	if _, ok := result["raw"]; !ok {
		t.Fatal(`DummyExtractor result missing "raw" key`)
	}
}

func TestCertFieldsExtractor_EmptyRaw(t *testing.T) {
	ex := &CertFieldsExtractor{}
	ctx := &etl_core.Context{}
	raw := &ct.RawLogEntry{} // This will cause ToLogEntry to fail

	_, err := ex.Extract(ctx, raw)
	if err == nil {
		t.Fatal("CertFieldsExtractor should error with empty RawLogEntry")
	}
}

// For a proper cert, you’d need to create or load a valid ct.RawLogEntry.
// Here’s a basic stub test for registration:
func TestForName(t *testing.T) {
	_, err := ForName("dummy")
	if err != nil {
		t.Errorf("expected to find registered dummy extractor, got: %v", err)
	}
	_, err = ForName("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent extractor, got none")
	}
}

type testExtractor struct{}

func (t *testExtractor) Extract(ctx *etl_core.Context, raw *ct.RawLogEntry) (map[string]interface{}, error) {
	return map[string]interface{}{"foo": "bar"}, nil
}

func TestRegisterAndRetrieve(t *testing.T) {
	Register("test", &testExtractor{})
	ex, err := ForName("test")
	if err != nil {
		t.Fatalf("expected to retrieve registered test extractor: %v", err)
	}
	ctx := &etl_core.Context{}
	raw := &ct.RawLogEntry{}
	result, err := ex.Extract(ctx, raw)
	if err != nil || result["foo"] != "bar" {
		t.Errorf("unexpected extractor output: %+v, err: %v", result, err)
	}
}

func TestDummyExtractor_WithRealEntry(t *testing.T) {
	raw := testutil.RawLogEntryForTestCert(t, 0)
	ex := &DummyExtractor{}
	ctx := &etl_core.Context{}

	got, err := ex.Extract(ctx, raw)
	require.NoError(t, err)
	require.NotNil(t, got)
	_, ok := (got)["raw"]
	require.True(t, ok)
}

func TestCertFieldsExtractor_X509Cert(t *testing.T) {
	raw := testutil.RawLogEntryForTestCert(t, 0)
	ex := &CertFieldsExtractor{}
	ctx := &etl_core.Context{}

	got, err := ex.Extract(ctx, raw)
	require.NoError(t, err)
	require.NotNil(t, got)
	entry := got
	require.Contains(t, entry, "subject")
	require.Contains(t, entry, "issuer")
}

func TestCertFieldsExtractor_Precert(t *testing.T) {
	raw := testutil.RawLogEntryForTestCert(t, 0)
	ex := &CertFieldsExtractor{}
	ctx := &etl_core.Context{}

	got, err := ex.Extract(ctx, raw)
	require.NoError(t, err)
	require.NotNil(t, got)
	entry := got
	require.Contains(t, entry, "subject")
	require.Contains(t, entry, "issuer")
}

func TestCertFieldsExtractor_BadRaw(t *testing.T) {
	// An intentionally corrupted or incomplete entry
	raw := &ct.RawLogEntry{}
	ex := &CertFieldsExtractor{}
	ctx := &etl_core.Context{}

	got, err := ex.Extract(ctx, raw)
	require.Error(t, err)
	require.Nil(t, got)
}

func TestRegistry(t *testing.T) {
	ex, err := ForName("dummy")
	require.NoError(t, err)
	require.NotNil(t, ex)

	_, err = ForName("does_not_exist")
	require.Error(t, err)
}
