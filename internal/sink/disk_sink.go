package sink

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/chtzvt/ctsnarf/internal/compression"
	"github.com/chtzvt/ctsnarf/internal/secrets"
)

type DiskSink struct {
	baseDir     string
	compression string
}

func NewDiskSink(opts map[string]interface{}, _ *secrets.Store) (Sink, error) {
	baseDir, ok := opts["path"].(string)
	if !ok || baseDir == "" {
		return nil, fmt.Errorf("disk sink requires 'path' option")
	}
	compression, _ := opts["compression"].(string)
	if compression == "" {
		compression = "none"
	}
	return &DiskSink{baseDir: baseDir, compression: compression}, nil
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
	w, err := compression.NewWriter(f, d.compression)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &diskSinkWriter{w, f}, nil
}

type diskSinkWriter struct {
	io.WriteCloser
	f *os.File
}

func (d *diskSinkWriter) Close() error {
	err1 := d.WriteCloser.Close()
	err2 := d.f.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func init() {
	Register("disk", NewDiskSink)
}
