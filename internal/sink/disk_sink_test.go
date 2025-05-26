package sink

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestDiskSinkWriteAndRead(t *testing.T) {
	// Set up temp dir for test output
	dir := t.TempDir()

	opts := map[string]interface{}{
		"path": dir,
	}
	sink, err := NewDiskSink(opts, nil)
	if err != nil {
		t.Fatalf("Failed to create DiskSink: %v", err)
	}

	// Write data
	writer, err := sink.Open(context.Background(), "testout.dat")
	if err != nil {
		t.Fatalf("Failed to open sink writer: %v", err)
	}
	data := []byte("hello, disksink!\n1234")
	n, err := writer.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Partial write: wrote %d, expected %d", n, len(data))
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read file back and check contents
	fpath := filepath.Join(dir, "testout.dat")
	b, err := os.ReadFile(fpath)
	if err != nil {
		t.Fatalf("Failed to read written file: %v", err)
	}
	// No compression by default, so content must match
	if !bytes.Equal(data, b) {
		t.Errorf("File contents do not match: got %q, want %q", b, data)
	}
}

func TestDiskSinkMkdirAll(t *testing.T) {
	// This test checks that nested directories are created as needed
	dir := t.TempDir()
	opts := map[string]interface{}{
		"path": dir,
	}
	sink, err := NewDiskSink(opts, nil)
	if err != nil {
		t.Fatalf("Failed to create DiskSink: %v", err)
	}

	writer, err := sink.Open(context.Background(), "nested/dir/test2.txt")
	if err != nil {
		t.Fatalf("Failed to open writer for nested path: %v", err)
	}
	defer writer.Close()
	_, err = writer.Write([]byte("abc"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}
