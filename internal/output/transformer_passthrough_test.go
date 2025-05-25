package output

import (
	"testing"
)

func TestPassthroughTransformer(t *testing.T) {
	cfg := TransformerConfig{Name: "passthrough"}
	tr, err := NewTransformerFromConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	in := map[string]interface{}{"foo": "bar"}
	out, err := tr.Transform(in)
	if err != nil || out["foo"] != "bar" {
		t.Fatalf("unexpected transform result: %v %v", out, err)
	}
}
