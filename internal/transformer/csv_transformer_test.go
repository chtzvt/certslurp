package transformer

import (
	"bytes"
	"encoding/csv"
	"strings"
	"testing"
)

func TestCSVTransformer(t *testing.T) {
	tr, err := ForName("csv")
	if err != nil {
		t.Fatal(err)
	}
	fields := []string{"foo", "num"}
	ctx := makeCtx(fields...)
	input := map[string]interface{}{"foo": "bar", "num": 42}

	// Header
	header, err := tr.Header(ctx)
	if err != nil {
		t.Fatal("csv.Header error:", err)
	}
	r := csv.NewReader(bytes.NewReader(header))
	cols, _ := r.Read()
	if len(cols) != len(fields) || cols[0] != "foo" || cols[1] != "num" {
		t.Errorf("csv.Header got: %v, want %v", cols, fields)
	}

	// Transform
	row, err := tr.Transform(ctx, input)
	if err != nil {
		t.Fatal("csv.Transform error:", err)
	}
	r = csv.NewReader(bytes.NewReader(row))
	cells, _ := r.Read()
	if len(cells) != 2 || cells[0] != "bar" || cells[1] != "42" {
		t.Errorf("csv.Transform got: %v, want %v", cells, []string{"bar", "42"})
	}

	// Footer
	footer, err := tr.Footer(ctx)
	if err != nil {
		t.Fatal("csv.Footer error:", err)
	}
	if len(footer) != 0 {
		t.Errorf("csv.Footer got: %q, want empty", footer)
	}
}

func TestCSVTransformer_Errors(t *testing.T) {
	tr, _ := ForName("csv")
	ctx := makeCtx() // No fields provided

	_, err := tr.Header(ctx)
	if err == nil || !strings.Contains(err.Error(), "fields") {
		t.Errorf("csv.Header should error on missing fields, got: %v", err)
	}

	_, err = tr.Transform(ctx, map[string]interface{}{"foo": "bar"})
	if err == nil || !strings.Contains(err.Error(), "fields") {
		t.Errorf("csv.Transform should error on missing fields, got: %v", err)
	}
}
