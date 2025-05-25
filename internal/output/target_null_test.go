package output

import (
	"testing"
)

func TestNullTarget(t *testing.T) {
	cfg := TargetConfig{Name: "null"}
	tgt, err := NewTargetFromConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := tgt.Write(map[string]interface{}{"baz": 1}); err != nil {
		t.Errorf("unexpected write error: %v", err)
	}
	if err := tgt.Close(); err != nil {
		t.Errorf("unexpected close error: %v", err)
	}
}
