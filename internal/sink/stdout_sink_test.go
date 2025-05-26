package sink

import (
	"bytes"
	"context"
	"os"
	"testing"
)

func TestStdoutSinkWrite(t *testing.T) {
	// Save original stdout and restore later
	origStdout := os.Stdout
	defer func() { os.Stdout = origStdout }()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}
	os.Stdout = w

	sink, err := NewStdoutSink(nil, nil)
	if err != nil {
		t.Fatalf("Failed to create StdoutSink: %v", err)
	}
	writer, err := sink.Open(context.Background(), "ignored")
	if err != nil {
		t.Fatalf("Failed to open writer: %v", err)
	}

	testData := []byte("stdout test output\n")
	_, err = writer.Write(testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	writer.Close()
	w.Close() // Needed to signal EOF to reader

	var buf bytes.Buffer
	_, err = buf.ReadFrom(r)
	if err != nil {
		t.Fatalf("ReadFrom pipe failed: %v", err)
	}

	if buf.String() != string(testData) {
		t.Errorf("Expected %q, got %q", string(testData), buf.String())
	}
}
