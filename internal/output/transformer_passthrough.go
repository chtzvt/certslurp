package output

type PassthroughTransformer struct{}

func (PassthroughTransformer) Init(options map[string]interface{}) error { return nil }
func (PassthroughTransformer) Transform(entry map[string]interface{}) (map[string]interface{}, error) {
	return entry, nil
}
func (PassthroughTransformer) Name() string { return "passthrough" }

func init() {
	RegisterTransformer("passthrough", func() Transformer { return PassthroughTransformer{} })
}
