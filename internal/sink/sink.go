// Package sink provides pluggable ETL output sinks (disk, S3, etc).
package sink

import (
	"context"
	"io"
	"sync"

	"github.com/chtzvt/ctsnarf/internal/secrets"
)

// Sink is an abstraction for any destination to which ETL output data can be written.
type Sink interface {
	// Open initializes a new logical output stream (file, blob, object, etc) named 'name'.
	Open(ctx context.Context, name string) (SinkWriter, error)
}

// SinkWriter represents a single output stream, such as a file or cloud object.
type SinkWriter interface {
	io.WriteCloser // Write(p []byte) (n int, err error); Close() error
}

// SinkFactory constructs a Sink given options and access to a secrets store.
type SinkFactory func(opts map[string]interface{}, secrets *secrets.Store) (Sink, error)

var (
	registryMu   sync.RWMutex
	sinkRegistry = make(map[string]SinkFactory)
)

// Register adds a sink factory to the global registry under the given name.
func Register(name string, factory SinkFactory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	sinkRegistry[name] = factory
}

// ForName returns a sink factory from the registry by name.
func ForName(name string) (SinkFactory, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	f, ok := sinkRegistry[name]
	return f, ok
}
