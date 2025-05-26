package sink

import (
	"context"
	"io"
	"os"

	"github.com/chtzvt/ctsnarf/internal/secrets"
)

// StdoutSink writes output to os.Stdout (for testing/dev/benchmark).
type StdoutSink struct{}

func NewStdoutSink(_ map[string]interface{}, _ *secrets.Store) (Sink, error) {
	return &StdoutSink{}, nil
}

func (s *StdoutSink) Open(ctx context.Context, name string) (SinkWriter, error) {
	return &stdoutWriter{Writer: os.Stdout}, nil
}

type stdoutWriter struct {
	io.Writer
}

func (w *stdoutWriter) Close() error {
	// Don't close os.Stdout!
	return nil
}

func init() {
	Register("stdout", NewStdoutSink)
}
