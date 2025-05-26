package transformer

import (
	"github.com/chtzvt/ctsnarf/internal/etl_core"
)

type Transformer interface {
	Transform(ctx *etl_core.Context, data map[string]interface{}) ([]byte, error)
}
