package output

type NullTarget struct{}

func (NullTarget) Init(options map[string]interface{}) error { return nil }
func (NullTarget) Write(entry map[string]interface{}) error  { return nil }
func (NullTarget) Close() error                              { return nil }
func (NullTarget) Name() string                              { return "null" }

func init() {
	RegisterTarget("null", func() Target { return NullTarget{} })
}
