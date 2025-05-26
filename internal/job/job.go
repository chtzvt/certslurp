package job

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

type JobSpec struct {
	Version string     `json:"version"`
	Note    string     `json:"note,omitempty"`
	LogURI  string     `json:"log_uri"`
	Options JobOptions `json:"options"`
}

type JobOptions struct {
	Fetch  FetchConfig   `json:"fetch"`
	Match  MatchConfig   `json:"match"`
	Output OutputOptions `json:"output"`
}

type OutputOptions struct {
	Extractor          string                 `json:"extractor"`
	ExtractorOptions   map[string]interface{} `json:"extractor_options"`
	Transformer        string                 `json:"transformer"`
	TransformerOptions map[string]interface{} `json:"transformer_options"`
	Sink               string                 `json:"sink"`
	SinkOptions        map[string]interface{} `json:"sink_options"`
}

func LoadFromFile(path string) (*JobSpec, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Load(f)
}

func Load(r io.Reader) (*JobSpec, error) {
	var js JobSpec
	dec := json.NewDecoder(r)
	if err := dec.Decode(&js); err != nil {
		return nil, err
	}
	if err := js.Validate(); err != nil {
		return nil, err
	}
	return &js, nil
}

func (j *JobSpec) Validate() error {
	var missing []string

	if j.Version == "" {
		missing = append(missing, "version")
	}
	if j.LogURI == "" {
		missing = append(missing, "log_uri")
	}
	if j.Options.Fetch.BatchSize <= 0 {
		missing = append(missing, "options.fetch.batch_size")
	}
	if j.Options.Fetch.Workers <= 0 {
		missing = append(missing, "options.fetch.workers")
	}
	if j.Options.Output.Extractor == "" {
		missing = append(missing, "options.output.extractor")
	}
	if j.Options.Output.Transformer == "" {
		missing = append(missing, "options.output.transformer")
	}
	if j.Options.Output.Sink == "" {
		missing = append(missing, "options.output.target")
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing/invalid job fields: %s", strings.Join(missing, ", "))
	}
	return nil
}
