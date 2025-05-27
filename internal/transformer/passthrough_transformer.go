package transformer

import (
	"github.com/chtzvt/ctsnarf/internal/etl_core"
)

type PassthroughTransformer struct{}

func (c *PassthroughTransformer) Transform(ctx *etl_core.Context, data map[string]interface{}) ([]byte, error) {
	return data["raw"].([]byte), nil
}

func (c *PassthroughTransformer) Header(ctx *etl_core.Context) ([]byte, error) {
	return []byte{}, nil
}

func (c *PassthroughTransformer) Footer(ctx *etl_core.Context) ([]byte, error) {
	return []byte{}, nil
}

func init() {
	Register("passthrough", &PassthroughTransformer{})
}
