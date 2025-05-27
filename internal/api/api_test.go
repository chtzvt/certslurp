package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/chtzvt/certslurp/internal/cluster"
	"github.com/chtzvt/certslurp/internal/job"
)

func TestSubmitJob(t *testing.T) {
	server, _ := setupAuthTestServer("testtoken")
	defer server.Close()

	client := &http.Client{}
	body := `{"version":"1.0.0","log_uri":"test","options":{"fetch":{"batch_size":10,"workers":1,"index_start":0,"index_end":100},"match":{},"output":{"extractor":"raw","transformer":"none","sink":"null"}}}`
	req, _ := http.NewRequest("POST", server.URL+"/api/jobs", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer testtoken")
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	var out map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if _, ok := out["job_id"]; !ok {
		t.Fatal("missing job_id in response")
	}
}

func TestGetJob(t *testing.T) {
	server, stub := setupAuthTestServer("testtoken")
	defer server.Close()

	// Pre-load a job:
	spec := &job.JobSpec{Version: "1.0.0", LogURI: "test", Options: job.JobOptions{Fetch: job.FetchConfig{BatchSize: 10, Workers: 1}}}
	jobID, _ := stub.SubmitJob(context.Background(), spec)

	client := &http.Client{}
	req, _ := http.NewRequest("GET", server.URL+"/api/jobs/"+jobID, nil)
	req.Header.Set("Authorization", "Bearer testtoken")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var out cluster.JobInfo
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.ID != jobID {
		t.Fatalf("wrong job ID: %s", out.ID)
	}
}

func TestListJobs(t *testing.T) {
	server, stub := setupAuthTestServer("testtoken")
	defer server.Close()

	spec := &job.JobSpec{Version: "1.0.0", LogURI: "test", Options: job.JobOptions{Fetch: job.FetchConfig{BatchSize: 10, Workers: 1}}}
	_, _ = stub.SubmitJob(context.Background(), spec)

	client := &http.Client{}
	req, _ := http.NewRequest("GET", server.URL+"/api/jobs", nil)
	req.Header.Set("Authorization", "Bearer testtoken")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var jobs []cluster.JobInfo
	_ = json.NewDecoder(resp.Body).Decode(&jobs)
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
}
