package transformer

import "testing"

func TestCBORTransformer(t *testing.T) {
	tr, err := ForName("cbor")
	if err != nil {
		t.Fatal(err)
	}
	ctx := makeCtx()
	input := map[string]interface{}{"foo": "bar", "num": 42}
	out, err := tr.Transform(ctx, input)
	if err != nil {
		t.Fatal("cbor.Transform error:", err)
	}
	// Just check it's nonempty for now (fxamacker/cbor tests cover correctness)
	if len(out) == 0 {
		t.Fatal("cbor.Transform returned empty")
	}
}
