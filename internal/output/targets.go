package output

import (
	"fmt"
)

// Target is the interface for output targets (disk, s3, etc).
type Target interface {
	Init(options map[string]interface{}) error
	Write(entry map[string]interface{}) error
	Close() error
	Name() string
}

type TargetConfig struct {
	Name    string                 `json:"target"`
	Options map[string]interface{} `json:"options,omitempty"`
}

// Registry for targets
var targetRegistry = map[string]func() Target{}

// RegisterTarget registers a target constructor by name.
func RegisterTarget(name string, ctor func() Target) {
	targetRegistry[name] = ctor
}

// NewTargetFromConfig instantiates and configures a target.
func NewTargetFromConfig(cfg TargetConfig) (Target, error) {
	ctor, ok := targetRegistry[cfg.Name]
	if !ok {
		return nil, fmt.Errorf("unknown target: %q", cfg.Name)
	}
	tgt := ctor()
	if err := tgt.Init(cfg.Options); err != nil {
		return nil, fmt.Errorf("init target: %w", err)
	}
	return tgt, nil
}

func RegisteredTargets() []string {
	keys := make([]string, 0, len(targetRegistry))
	for k := range targetRegistry {
		keys = append(keys, k)
	}
	return keys
}

func registerTargets() {
	RegisterTarget("null", func() Target {
		return &NullTarget{}
	})
}
