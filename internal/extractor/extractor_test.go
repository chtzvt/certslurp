package extractor

import (
	"testing"

	"github.com/chtzvt/ctsnarf/internal/etl_core"
	ct "github.com/google/certificate-transparency-go"
	"github.com/stretchr/testify/require"
)

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

func TestRegistry(t *testing.T) {
	ex, err := ForName("dummy")
	require.NoError(t, err)
	require.NotNil(t, ex)

	_, err = ForName("does_not_exist")
	require.Error(t, err)
}
