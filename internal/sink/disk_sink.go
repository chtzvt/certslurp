package sink

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/chtzvt/certslurp/internal/secrets"
)

type DiskSink struct {
	baseDir string
}

func NewDiskSink(opts map[string]interface{}, _ *secrets.Store) (Sink, error) {
	baseDir, ok := opts["path"].(string)
	if !ok || baseDir == "" {
		return nil, fmt.Errorf("disk sink requires 'path' option")
	}
	return &DiskSink{baseDir: baseDir}, nil
}

func (d *DiskSink) Open(ctx context.Context, name string) (SinkWriter, error) {
	fullPath := filepath.Join(d.baseDir, name)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return nil, err
	}
	f, err := os.Create(fullPath)
	if err != nil {
		return nil, err
	}
	return &diskSinkWriter{f}, nil
}

type diskSinkWriter struct {
	f *os.File
}

func (d *diskSinkWriter) Write(p []byte) (int, error) {
	return d.f.Write(p)
}

func (d *diskSinkWriter) Close() error {
	return d.f.Close()
}

func init() {
	Register("disk", NewDiskSink)
}
