package job

import (
	"strings"
	"testing"
)

func TestLoadJobSpecJSON(t *testing.T) {
	jobJSON := `
{
	"type": "certslurp:job",
	"id": "1234-5678",
	"version": "1.0.0",
	"note": "Sample job spec",
	"log_uri": "https://ct.googleapis.com/aviator",
	"options": {
		"fetch": {
			"fetch_size": 1000,
			"fetch_workers": 2,
			"index_start": 0,
			"index_end": 0
		},
		"match": {
			"subject_regex": ".*google.com"
		},
		"output": {
			"extractor": "cert_fields",
			"extractor_options": {"fields": ["subject", "issuer"]},
			"transformer": "jsonl",
			"transformer_options": {"pretty": true},
			"sink": "disk",
			"sink_options": {"path": "/tmp/certslurp-out", "compression": "gzip"}
		}
	}
}`

	spec, err := Load(strings.NewReader(jobJSON))
	if err != nil {
		t.Fatalf("Failed to load job spec: %v", err)
	}

	if spec.Version != "1.0.0" {
		t.Errorf("expected version 1.0.0, got %s", spec.Version)
	}
	if spec.Options.Output.Extractor != "cert_fields" {
		t.Errorf("wrong extractor: got %s", spec.Options.Output.Extractor)
	}
	if spec.Options.Output.Transformer != "jsonl" {
		t.Errorf("wrong transformer: got %s", spec.Options.Output.Transformer)
	}
	if spec.Options.Output.Sink != "disk" {
		t.Errorf("wrong sink: got %s", spec.Options.Output.Sink)
	}
	// Check nested option is accessible
	if val, ok := spec.Options.Output.ExtractorOptions["fields"]; !ok || val == nil {
		t.Errorf("extractor_options.fields missing or nil")
	}
}

func TestJobLoadAndValidate(t *testing.T) {
	const minimal = `{
		"version": "0.1.0",
		"log_uri": "https://ct.googleapis.com/rocketeer",
		"options": {
			"fetch": {"fetch_size": 100, "fetch_workers": 2, "index_start": 0, "index_end": 0},
			"match": {},
			"output": {
				"extractor": "cert_fields",	
				"transformer": "jsonl",
				"sink": "disk"
			}
		}
	}`
	job, err := Load(strings.NewReader(minimal))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if job.Version != "0.1.0" {
		t.Errorf("expected version 0.1.0, got %q", job.Version)
	}
}

func TestJobLoad_MissingFields(t *testing.T) {
	const missing = `{
		"options": {
			"fetch": {"fetch_size": 0, "fetch_workers": 0, "index_start": 0, "index_end": 0},
			"output": {"transformer": {}, "sink": {}}
		}
	}`
	_, err := Load(strings.NewReader(missing))
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	t.Logf("got expected error: %v", err)
}
