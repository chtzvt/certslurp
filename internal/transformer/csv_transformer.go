package transformer

import (
	"bytes"
	"encoding/csv"
	"fmt"

	"github.com/chtzvt/ctsnarf/internal/etl_core"
)

type CSVTransformer struct{}

func (c *CSVTransformer) Transform(ctx *etl_core.Context, data map[string]interface{}) ([]byte, error) {
	fields, _ := ctx.Spec.Options.Output.TransformerOptions["fields"].([]interface{})
	if len(fields) == 0 {
		return nil, fmt.Errorf("CSV transformer requires fields option")
	}
	row := make([]string, len(fields))
	for i, f := range fields {
		key, _ := f.(string)
		val, _ := data[key]
		if val == nil {
			row[i] = ""
		} else {
			row[i] = fmt.Sprintf("%v", val)
		}
	}
	buf := &bytes.Buffer{}
	w := csv.NewWriter(buf)
	if err := w.Write(row); err != nil {
		return nil, err
	}
	w.Flush()
	return buf.Bytes(), w.Error()
}

func (c *CSVTransformer) Header(ctx *etl_core.Context) ([]byte, error) {
	fields, _ := ctx.Spec.Options.Output.TransformerOptions["fields"].([]interface{})
	if len(fields) == 0 {
		return nil, fmt.Errorf("CSV transformer requires fields option for header")
	}
	header := make([]string, len(fields))
	for i, f := range fields {
		header[i], _ = f.(string)
	}
	buf := &bytes.Buffer{}
	w := csv.NewWriter(buf)
	if err := w.Write(header); err != nil {
		return nil, err
	}
	w.Flush()
	return buf.Bytes(), w.Error()
}

func (c *CSVTransformer) Footer(ctx *etl_core.Context) ([]byte, error) {
	return []byte{}, nil
}

func init() {
	Register("csv", &CSVTransformer{})
}
