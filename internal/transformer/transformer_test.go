package transformer

import (
	"testing"

	"github.com/chtzvt/ctsnarf/internal/etl_core"
	"github.com/chtzvt/ctsnarf/internal/job"
)

// makeCtx builds an etl_core.Context with fields set as desired.
func makeCtx(fields ...string) *etl_core.Context {
	var transformerOptions map[string]interface{}
	if len(fields) > 0 {
		fs := make([]interface{}, len(fields))
		for i, f := range fields {
			fs[i] = f
		}
		transformerOptions = map[string]interface{}{"fields": fs}
	} else {
		transformerOptions = map[string]interface{}{}
	}
	return &etl_core.Context{
		Spec: &job.JobSpec{
			Version: "1.0.0",
			LogURI:  "test-uri",
			Options: job.JobOptions{
				Output: job.OutputOptions{
					TransformerOptions: transformerOptions,
				},
			},
		},
	}
}

func TestTransformerFactoryErrors(t *testing.T) {
	_, err := ForName("notexist")
	if err == nil {
		t.Error("ForName should error for unknown transformer")
	}
}
