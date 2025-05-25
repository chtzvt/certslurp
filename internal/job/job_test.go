package job

import (
	"strings"
	"testing"
)

func TestJobLoadAndValidate(t *testing.T) {
	const minimal = `{
		"version": "0.1.0",
		"log_uri": "https://ct.googleapis.com/rocketeer",
		"options": {
			"fetch": {"batch_size": 100, "workers": 2, "index_start": 0, "index_end": 0},
			"match": {},
			"output": {
				"transformer": {"transformer": "jsonl"},
				"target": {"target": "disk"}
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
			"fetch": {"batch_size": 0, "workers": 0, "index_start": 0, "index_end": 0},
			"output": {"transformer": {}, "target": {}}
		}
	}`
	_, err := Load(strings.NewReader(missing))
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	t.Logf("got expected error: %v", err)
}
