package extractor

import (
	"testing"
	"time"

	"github.com/chtzvt/ctsnarf/internal/etl_core"
	"github.com/chtzvt/ctsnarf/internal/job"
	"github.com/chtzvt/ctsnarf/internal/testutil"
	ct "github.com/google/certificate-transparency-go"
	"github.com/stretchr/testify/require"
)

func TestCertFieldsExtractor_EmptyRaw(t *testing.T) {
	ex := &CertFieldsExtractor{}
	ctx := &etl_core.Context{}
	raw := &ct.RawLogEntry{} // Invalid entry
	_, err := ex.Extract(ctx, raw)
	require.Error(t, err)
}

func TestCertFieldsExtractor_BadRaw(t *testing.T) {
	raw := &ct.RawLogEntry{}
	ex := &CertFieldsExtractor{}
	ctx := &etl_core.Context{}
	got, err := ex.Extract(ctx, raw)
	require.Error(t, err)
	require.Nil(t, got)
}

func TestCertFieldsExtractor_X509Cert_DefaultFields(t *testing.T) {
	raw := testutil.RawLogEntryForTestCert(t, 0)
	ex := &CertFieldsExtractor{}

	jobSpec := job.JobSpec{
		Options: job.JobOptions{
			Output: job.OutputOptions{
				ExtractorOptions: map[string]interface{}{},
			},
		},
	}

	ctx := &etl_core.Context{
		Spec: &jobSpec,
	}

	got, err := ex.Extract(ctx, raw)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Contains(t, got, "cn")
	require.Contains(t, got, "iss")
	require.Contains(t, got, "sub")
	require.Contains(t, got, "nbf")
}

func TestCertFieldsExtractor_CertFields_InclusionExclusion(t *testing.T) {
	raw := testutil.RawLogEntryForTestCert(t, 0)
	ex := &CertFieldsExtractor{
		Options: CertFieldsExtractorOptions{
			CertFields: "*,!email_addresses,!ip_addresses",
		},
	}
	ctx := &etl_core.Context{}
	got, err := ex.Extract(ctx, raw)
	require.NoError(t, err)
	require.NotContains(t, got, "em") // email_addresses excluded
	require.NotContains(t, got, "ip") // ip_addresses excluded
	require.Contains(t, got, "cn")    // common_name included by default
}

func TestCertFieldsExtractor_CertFields_SpecificFields(t *testing.T) {
	raw := testutil.RawLogEntryForTestCert(t, 0)
	ex := &CertFieldsExtractor{
		Options: CertFieldsExtractorOptions{
			CertFields: "organization,common_name",
		},
	}
	ctx := &etl_core.Context{}
	got, err := ex.Extract(ctx, raw)
	require.NoError(t, err)
	require.Contains(t, got, "org")
	require.Contains(t, got, "cn")
	require.NotContains(t, got, "em")
}

func TestCertFieldsExtractor_LogFields_SpecificField(t *testing.T) {
	raw := testutil.RawLogEntryForTestCert(t, 0)
	ex := &CertFieldsExtractor{
		Options: CertFieldsExtractorOptions{
			CertFields: "",
			LogFields:  "log_index",
		},
	}
	ctx := &etl_core.Context{}
	got, err := ex.Extract(ctx, raw)
	require.NoError(t, err)
	require.Contains(t, got, "li")
	require.Len(t, got, 1)
}

func TestCertFieldsExtractor_EmptySpec_UsesDefaults(t *testing.T) {
	raw := testutil.RawLogEntryForTestCert(t, 0)
	ex := &CertFieldsExtractor{}

	jobSpec := job.JobSpec{
		Options: job.JobOptions{
			Output: job.OutputOptions{
				ExtractorOptions: map[string]interface{}{},
			},
		},
	}

	ctx := &etl_core.Context{
		Spec: &jobSpec,
	}

	got, err := ex.Extract(ctx, raw)
	require.NoError(t, err)
	require.Contains(t, got, "cn") // Should have common name by default
}

func TestCertFieldsExtractor_FieldTypes(t *testing.T) {
	raw := testutil.RawLogEntryForTestCert(t, 0)
	ex := &CertFieldsExtractor{}

	jobSpec := job.JobSpec{
		Options: job.JobOptions{
			Output: job.OutputOptions{
				ExtractorOptions: map[string]interface{}{},
			},
		},
	}

	ctx := &etl_core.Context{
		Spec: &jobSpec,
	}

	got, err := ex.Extract(ctx, raw)
	require.NoError(t, err)

	// Types according to CertFieldsExtractorOutput
	if v, ok := got["nbf"]; ok {
		_, ok := v.(time.Time)
		require.True(t, ok)
	}
	if v, ok := got["li"]; ok {
		_, ok := v.(int64)
		require.True(t, ok)
	}
	if v, ok := got["dns"]; ok {
		_, ok := v.([]string)
		require.True(t, ok)
	}
}

func TestCertFieldsExtractor_ExcludesAllIfRequested(t *testing.T) {
	raw := testutil.RawLogEntryForTestCert(t, 0)
	ex := &CertFieldsExtractor{
		Options: CertFieldsExtractorOptions{
			CertFields: "!common_name,!organization,!email_addresses,!organizational_unit,!locality,!province,!country,!street_address,!postal_code,!dns_names,!ip_addresses,!uris,!subject,!issuer,!serial,!not_before,!not_after",
		},
	}
	ctx := &etl_core.Context{}
	got, err := ex.Extract(ctx, raw)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestCertFieldsExtractor_HandlesNilOptions(t *testing.T) {
	raw := testutil.RawLogEntryForTestCert(t, 0)
	ex := &CertFieldsExtractor{}

	jobSpec := job.JobSpec{
		Options: job.JobOptions{
			Output: job.OutputOptions{
				ExtractorOptions: nil,
			},
		},
	}

	ctx := &etl_core.Context{
		Spec: &jobSpec,
	}

	// Should not panic or error
	_, err := ex.Extract(ctx, raw)
	require.NoError(t, err)
}
