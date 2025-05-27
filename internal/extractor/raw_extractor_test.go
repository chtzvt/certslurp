package extractor

import (
	"testing"

	"github.com/chtzvt/certslurp/internal/etl_core"
	"github.com/chtzvt/certslurp/internal/testutil"
	ct "github.com/google/certificate-transparency-go"
	"github.com/stretchr/testify/require"
)

func TestRawExtractor(t *testing.T) {
	ex := &RawExtractor{}
	ctx := &etl_core.Context{}
	raw := &ct.RawLogEntry{} // could be nil for RawExtractor

	result, err := ex.Extract(ctx, raw)
	if err != nil {
		t.Fatalf("RawExtractor returned error: %v", err)
	}
	if result == nil {
		t.Fatal("RawExtractor returned nil result")
	}
	if _, ok := result["raw"]; !ok {
		t.Fatal(`RawExtractor result missing "raw" key`)
	}
}

func TestRawExtractor_WithRealEntry(t *testing.T) {
	raw := testutil.RawLogEntryForTestCert(t, 0)
	ex := &RawExtractor{}
	ctx := &etl_core.Context{}

	got, err := ex.Extract(ctx, raw)
	require.NoError(t, err)
	require.NotNil(t, got)
	_, ok := (got)["raw"]
	require.True(t, ok)
}
