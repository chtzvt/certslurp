package transformer

import (
	"encoding/json"
	"testing"
)

func TestJSONLTransformer(t *testing.T) {
	tr, err := ForName("jsonl")
	if err != nil {
		t.Fatal(err)
	}
	ctx := makeCtx()
	input := map[string]interface{}{"foo": "bar", "num": 42}

	// Header/Footer
	header, err := tr.Header(ctx)
	if err != nil || len(header) != 0 {
		t.Fatalf("jsonl.Header got: %q, want empty", header)
	}
	footer, err := tr.Footer(ctx)
	if err != nil || len(footer) != 0 {
		t.Fatalf("jsonl.Footer got: %q, want empty", footer)
	}

	// Transform
	out, err := tr.Transform(ctx, input)
	if err != nil {
		t.Fatal("jsonl.Transform error:", err)
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("jsonl.Transform produced invalid JSON: %v", err)
	}
	if parsed["foo"] != "bar" || int(parsed["num"].(float64)) != 42 {
		t.Errorf("jsonl.Transform unexpected content: %v", parsed)
	}
}
