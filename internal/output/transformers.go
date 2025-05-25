package output

import (
	"fmt"
)

// Transformer is the interface all transformers must implement.
type Transformer interface {
	Init(options map[string]interface{}) error
	Transform(entry map[string]interface{}) (map[string]interface{}, error)
	Name() string
}

type TransformerConfig struct {
	Name    string                 `json:"transformer"`
	Options map[string]interface{} `json:"options,omitempty"`
}

// Registry for transformer implementations
var transformerRegistry = map[string]func() Transformer{}

// RegisterTransformer registers a transformer constructor by name.
func RegisterTransformer(name string, ctor func() Transformer) {
	transformerRegistry[name] = ctor
}

// NewTransformerFromConfig instantiates and configures a transformer by config.
func NewTransformerFromConfig(cfg TransformerConfig) (Transformer, error) {
	ctor, ok := transformerRegistry[cfg.Name]
	if !ok {
		return nil, fmt.Errorf("unknown transformer: %q", cfg.Name)
	}
	tr := ctor()
	if err := tr.Init(cfg.Options); err != nil {
		return nil, fmt.Errorf("init transformer: %w", err)
	}
	return tr, nil
}

func RegisteredTransformers() []string {
	keys := make([]string, 0, len(transformerRegistry))
	for k := range transformerRegistry {
		keys = append(keys, k)
	}
	return keys
}

func registerTransformers() {
	RegisterTransformer("passthrough", func() Transformer {
		return &PassthroughTransformer{}
	})
}
