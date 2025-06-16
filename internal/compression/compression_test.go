package compression

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/dsnet/compress/bzip2"
	"github.com/klauspost/compress/zstd"
)

func TestNewWriter_Zstd(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, "zstd")
	if err != nil {
		t.Fatalf("NewWriter zstd: %v", err)
	}
	original := []byte("hello zstd world")
	_, err = w.Write(original)
	if err != nil {
		t.Fatalf("Write zstd: %v", err)
	}
	w.Close()

	// Try to decompress and verify
	r, err := zstd.NewReader(&buf)
	if err != nil {
		t.Fatalf("zstd.NewReader: %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll zstd: %v", err)
	}
	if string(out) != string(original) {
		t.Errorf("zstd decompress mismatch: got %q, want %q", out, original)
	}
}

func TestNewWriter_Gzip(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, "gzip")
	if err != nil {
		t.Fatalf("NewWriter gzip: %v", err)
	}
	original := []byte("hello gzip world")
	_, err = w.Write(original)
	if err != nil {
		t.Fatalf("Write gzip: %v", err)
	}
	w.Close()

	// Try to decompress and verify
	r, err := gzip.NewReader(&buf)
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll gzip: %v", err)
	}
	if string(out) != string(original) {
		t.Errorf("gzip decompress mismatch: got %q, want %q", out, original)
	}
}

func TestNewWriter_Bzip2(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, "bzip2")
	if err != nil {
		t.Fatalf("NewWriter bzip2: %v", err)
	}
	original := []byte("hello bzip2 world")
	_, err = w.Write(original)
	if err != nil {
		t.Fatalf("Write bzip2: %v", err)
	}
	w.Close()

	// Try to decompress and verify
	r, err := bzip2.NewReader(&buf, nil)
	if err != nil {
		t.Fatalf("bzip2.NewReader: %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll bzip2: %v", err)
	}
	if string(out) != string(original) {
		t.Errorf("bzip2 decompress mismatch: got %q, want %q", out, original)
	}
}

func TestNewWriter_None(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, "none")
	if err != nil {
		t.Fatalf("NewWriter none: %v", err)
	}
	original := []byte("plain text passthrough")
	_, err = w.Write(original)
	if err != nil {
		t.Fatalf("Write none: %v", err)
	}
	w.Close()

	if buf.String() != string(original) {
		t.Errorf("none passthrough mismatch: got %q, want %q", buf.String(), original)
	}
}

func TestNewWriter_Unsupported(t *testing.T) {
	var buf bytes.Buffer
	_, err := NewWriter(&buf, "lzma")
	if err == nil {
		t.Error("Expected error for unsupported compression, got nil")
	}
}
