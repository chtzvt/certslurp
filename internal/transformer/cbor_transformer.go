package transformer

import (
	"github.com/chtzvt/certslurp/internal/etl_core"
	"github.com/fxamacker/cbor/v2"
)

type CBORTransformer struct{}

func (c *CBORTransformer) Transform(ctx *etl_core.Context, data map[string]interface{}) ([]byte, error) {
	return cbor.Marshal(data)
}

func (c *CBORTransformer) Header(ctx *etl_core.Context) ([]byte, error) {
	return []byte{}, nil
}

func (c *CBORTransformer) Footer(ctx *etl_core.Context) ([]byte, error) {
	return []byte{}, nil
}

func init() {
	Register("cbor", &CBORTransformer{})
}
