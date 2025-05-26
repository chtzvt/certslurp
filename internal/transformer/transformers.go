package transformer

import (
	"fmt"

	"github.com/chtzvt/ctsnarf/internal/etl_core"
)

type Transformer interface {
	Transform(ctx *etl_core.Context, data map[string]interface{}) ([]byte, error)

	// Header returns any leading bytes (e.g., header row, opening bracket, etc).
	// Should return nil/empty if not needed.
	Header(ctx *etl_core.Context) ([]byte, error)

	// Footer returns any trailing bytes (e.g., closing bracket, sentinel value, etc).
	// Should return nil/empty if not needed.
	Footer(ctx *etl_core.Context) ([]byte, error)
}

var registry = make(map[string]Transformer)

func Register(name string, t Transformer) {
	registry[name] = t
}

func ForName(name string) (Transformer, error) {
	tr, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("transformer not found: %s", name)
	}
	return tr, nil
}
