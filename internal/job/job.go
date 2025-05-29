package job

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

type JobSpec struct {
	Version string     `json:"version" yaml:"version"`
	Note    string     `json:"note,omitempty" yaml:"note"`
	LogURI  string     `json:"log_uri" yaml:"log_uri"`
	Options JobOptions `json:"options" yaml:"options"`
}

type JobOptions struct {
	Fetch  FetchConfig   `json:"fetch" yaml:"fetch"`
	Match  MatchConfig   `json:"match" yaml:"match"`
	Output OutputOptions `json:"output" yaml:"output"`
}

type FetchConfig struct {
	// These settings configure the CT scanner brought up by each worker
	// FetchSize controls the size of the batches of records scanned from the CT log
	// FetchWorkers controls the parallelism of the scan
	FetchSize    int `json:"fetch_size" yaml:"fetch_size"`
	FetchWorkers int `json:"fetch_workers" yaml:"fetch_workers"`

	// Optional number of shards to create for the job
	ShardSize int `json:"shard_size" yaml:"shard_size"`

	// CT log index range to scan
	IndexStart int64 `json:"index_start" yaml:"index_start"`
	IndexEnd   int64 `json:"index_end" yaml:"index_end"` // Non-inclusive; 0 = end of log
}

type MatchConfig struct {
	SubjectRegex     string `json:"subject_regex,omitempty" yaml:"subject_regex"`
	IssuerRegex      string `json:"issuer_regex,omitempty" yaml:"issuer_regex"`
	Serial           string `json:"serial,omitempty" yaml:"serial"`
	SCTTimestamp     uint64 `json:"sct_timestamp,omitempty" yaml:"sct_timestamp"`
	Domain           string `json:"domain,omitempty" yaml:"domain"`
	ParseErrors      string `json:"parse_errors,omitempty" yaml:"parse_errors"` // "all" or "nonfatal"
	ValidationErrors bool   `json:"validation_errors,omitempty" yaml:"validation_errors"`
	SkipPrecerts     bool   `json:"skip_precerts,omitempty" yaml:"skip_precerts"`
	PrecertsOnly     bool   `json:"precerts_only,omitempty" yaml:"precerts_only"`
	Workers          int    `json:"workers,omitempty" yaml:"workers"`
}

type OutputOptions struct {
	ChunkRecords       int                    `json:"chunk_records" yaml:"chunk_records"`
	ChunkBytes         int                    `json:"chunk_bytes" yaml:"chunk_bytes"`
	Extractor          string                 `json:"extractor" yaml:"extractor"`
	ExtractorOptions   map[string]interface{} `json:"extractor_options" yaml:"extractor_options"`
	Transformer        string                 `json:"transformer" yaml:"transformer"`
	TransformerOptions map[string]interface{} `json:"transformer_options" yaml:"transformer_options"`
	Sink               string                 `json:"sink" yaml:"sink"`
	SinkOptions        map[string]interface{} `json:"sink_options" yaml:"sink_options"`
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
	if j.Options.Fetch.FetchSize <= 0 {
		missing = append(missing, "options.fetch.fetch_size")
	}
	if j.Options.Fetch.FetchWorkers <= 0 {
		missing = append(missing, "options.fetch.workers")
	}
	if j.Options.Output.Extractor == "" {
		missing = append(missing, "options.output.extractor")
	}
	if j.Options.Output.Transformer == "" {
		missing = append(missing, "options.output.transformer")
	}
	if j.Options.Output.Sink == "" {
		missing = append(missing, "options.output.sink")
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing/invalid job fields: %s", strings.Join(missing, ", "))
	}
	return nil
}
