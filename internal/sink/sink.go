package sink

import (
	"github.com/chtzvt/ctsnarf/internal/etl_core"
)

// Sink is the interface for output sinks (disk, s3, etc).
type Sink interface {
	Write(ctx *etl_core.Context, data []byte) error
}
