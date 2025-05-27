package extractor

import (
	"testing"

	"github.com/chtzvt/ctsnarf/internal/etl_core"
	"github.com/chtzvt/ctsnarf/internal/testutil"
	ct "github.com/google/certificate-transparency-go"
	"github.com/stretchr/testify/require"
)

func TestCertFieldsExtractor_EmptyRaw(t *testing.T) {
	ex := &CertFieldsExtractor{}
	ctx := &etl_core.Context{}
	raw := &ct.RawLogEntry{} // This will cause ToLogEntry to fail

	_, err := ex.Extract(ctx, raw)
	if err == nil {
		t.Fatal("CertFieldsExtractor should error with empty RawLogEntry")
	}
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
