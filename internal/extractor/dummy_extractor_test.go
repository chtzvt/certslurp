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
