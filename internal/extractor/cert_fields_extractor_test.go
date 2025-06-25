package extractor

import (
	"testing"
	"time"

	"github.com/chtzvt/certslurp/internal/etl_core"
	"github.com/chtzvt/certslurp/internal/job"
	"github.com/chtzvt/certslurp/internal/testutil"
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

func TestCertFieldsExtractor_X509Cert_AllGlobDefaultFields(t *testing.T) {
	raw := testutil.RawLogEntryForTestCert(t, 0)
	ex := &CertFieldsExtractor{}

	jobSpec := job.JobSpec{
		Options: job.JobOptions{
			Output: job.OutputOptions{
				ExtractorOptions: map[string]interface{}{
					"cert_fields": "*",
				},
			},
		},
	}

	ctx := &etl_core.Context{
		Spec: &jobSpec,
	}

	got, err := ex.Extract(ctx, raw)
	require.NoError(t, err)
	require.NotNil(t, got)
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
	require.Empty(t, got) // Should be empty by default
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

func TestCertFieldsExtractor_HandlesMetadataOptions(t *testing.T) {
	raw := testutil.RawLogEntryForTestCert(t, 0)
	ex := &CertFieldsExtractor{}

	ctx := &etl_core.Context{
		Spec: &job.JobSpec{
			LogURI: "https://testlog.example.com",
			Options: job.JobOptions{
				Output: job.OutputOptions{
					ExtractorOptions: map[string]interface{}{
						"metadata_fields": "log_url",
					},
				},
			},
		},
	}

	// Should not panic or error
	got, err := ex.Extract(ctx, raw)
	require.NoError(t, err)

	require.Contains(t, got, "log")
	require.Equal(t, got["log"], "https://testlog.example.com")
	require.Equal(t, got["fts"], nil)
}

func TestCertFieldsExtractor_Precert_AllFields(t *testing.T) {
	raw := testutil.RawLogEntryForTestPrecert(t, 0)
	ex := &CertFieldsExtractor{
		Options: CertFieldsExtractorOptions{
			PrecertFields: "*",
		},
	}
	ctx := &etl_core.Context{}
	got, err := ex.Extract(ctx, raw)
	require.NoError(t, err)
	require.NotNil(t, got)

	require.Equal(t, "precert", got["t"])
	require.Contains(t, got, "co")
	require.Contains(t, got, "loc")
	require.Contains(t, got, "naf")
	require.Contains(t, got, "prv")
	require.Contains(t, got, "sn")
	require.Contains(t, got, "sub")
	require.Contains(t, got, "org")
	require.Contains(t, got, "iss")
	require.Contains(t, got, "nbf")
}

func TestCertFieldsExtractor_Precert_SpecificFields(t *testing.T) {
	raw := testutil.RawLogEntryForTestPrecert(t, 0)
	ex := &CertFieldsExtractor{
		Options: CertFieldsExtractorOptions{
			PrecertFields: "locality,organization",
		},
	}
	ctx := &etl_core.Context{}
	got, err := ex.Extract(ctx, raw)
	require.NoError(t, err)
	require.Contains(t, got, "loc")
	require.Contains(t, got, "org")
}

func TestCertFieldsExtractor_Precert_Exclusion(t *testing.T) {
	raw := testutil.RawLogEntryForTestPrecert(t, 0)
	ex := &CertFieldsExtractor{
		Options: CertFieldsExtractorOptions{
			PrecertFields: "*,!organizational_unit,!country",
		},
	}
	ctx := &etl_core.Context{}
	got, err := ex.Extract(ctx, raw)
	require.NoError(t, err)
	require.NotContains(t, got, "ou")
	require.NotContains(t, got, "co")
	require.Contains(t, got, "org")
}

func TestCertFieldsExtractor_Precert_InheritsCertFields(t *testing.T) {
	raw := testutil.RawLogEntryForTestPrecert(t, 0)
	ex := &CertFieldsExtractor{
		Options: CertFieldsExtractorOptions{
			CertFields: "locality,organization",
		},
	}
	ctx := &etl_core.Context{}
	got, err := ex.Extract(ctx, raw)
	require.NoError(t, err)
	require.Contains(t, got, "loc")
	require.Contains(t, got, "org")
}

func TestCertFieldsExtractor_Precert_EmptyOption(t *testing.T) {
	raw := testutil.RawLogEntryForTestPrecert(t, 0)
	ex := &CertFieldsExtractor{
		Options: CertFieldsExtractorOptions{
			PrecertFields: "",
		},
	}

	ctx := &etl_core.Context{
		Spec: &job.JobSpec{
			Options: job.JobOptions{
				Output: job.OutputOptions{
					ExtractorOptions: map[string]interface{}{
						"precert_fields": "",
					},
				},
			},
		},
	}

	got, err := ex.Extract(ctx, raw)
	require.NoError(t, err)
	require.NotContains(t, "t", got)
	require.Len(t, got, 0)
}
