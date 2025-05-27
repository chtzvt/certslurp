package transformer

import (
	"github.com/chtzvt/ctsnarf/internal/etl_core"
)

type DummyTransformer struct{}

func (c *DummyTransformer) Transform(ctx *etl_core.Context, data map[string]interface{}) ([]byte, error) {
	return data["raw"].([]byte), nil
}

func (c *DummyTransformer) Header(ctx *etl_core.Context) ([]byte, error) {
	return []byte{}, nil
}

func (c *DummyTransformer) Footer(ctx *etl_core.Context) ([]byte, error) {
	return []byte{}, nil
}

func init() {
	Register("dummy", &DummyTransformer{})
}
