package transformer

import (
	"bytes"
	"encoding/json"

	"github.com/chtzvt/certslurp/internal/etl_core"
)

type JSONLTransformer struct{}

func (j *JSONLTransformer) Transform(ctx *etl_core.Context, data map[string]interface{}) ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)

	if err := enc.Encode(data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *JSONLTransformer) Header(ctx *etl_core.Context) ([]byte, error) {
	return []byte{}, nil
}

func (c *JSONLTransformer) Footer(ctx *etl_core.Context) ([]byte, error) {
	return []byte{}, nil
}

func init() {
	Register("jsonl", &JSONLTransformer{})
}
