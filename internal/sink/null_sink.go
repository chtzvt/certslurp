package sink

import (
	"context"

	"github.com/chtzvt/ctsnarf/internal/secrets"
)

type NullSink struct{}

func NewNullSink(_ map[string]interface{}, _ *secrets.Store) (Sink, error) {
	return &NullSink{}, nil
}

func (s *NullSink) Open(ctx context.Context, name string) (SinkWriter, error) {
	return &nullWriter{}, nil
}

type nullWriter struct{}

func (w *nullWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func (w *nullWriter) Close() error { return nil }

func init() {
	Register("null", NewNullSink)
}
