package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/chtzvt/certslurp/internal/cluster"
	"github.com/chtzvt/certslurp/internal/job"
	"github.com/chtzvt/certslurp/internal/testcluster"
	"github.com/stretchr/testify/require"
)

func setupJobAPI(t *testing.T) (*httptest.Server, cluster.Cluster, string) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	t.Cleanup(cleanup)
	mux := http.NewServeMux()
	RegisterJobHandlers(mux, cl)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	// Create a job for testing
	spec := &job.JobSpec{
		Version: "1.0.0",
		LogURI:  "test",
		Options: job.JobOptions{Fetch: job.FetchConfig{FetchSize: 10, FetchWorkers: 1}},
	}
	jobID, err := cl.SubmitJob(context.Background(), spec)
	require.NoError(t, err)
	return ts, cl, jobID
}

func setupTestServerWithCluster(cl cluster.Cluster) *httptest.Server {
	mux := http.NewServeMux()
	RegisterJobHandlers(mux, cl)
	RegisterWorkerHandlers(mux, cl)
	RegisterSecretHandlers(mux, cl)
	server := httptest.NewServer(mux)
	return server
}

func TestAPI_UpdateJobStatus(t *testing.T) {
	ts, cl, jobID := setupJobAPI(t)

	body := `{"status":"cancelled"}`
	req, _ := http.NewRequest("PATCH", ts.URL+"/api/jobs/"+jobID+"/status", strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	info, err := cl.GetJob(context.Background(), jobID)
	require.NoError(t, err)
	require.Equal(t, cluster.JobState("cancelled"), info.Status)
}

func TestAPI_MarkJobStartedCompletedCancelled(t *testing.T) {
	ts, cl, jobID := setupJobAPI(t)

	// Start
	req, _ := http.NewRequest("POST", ts.URL+"/api/jobs/"+jobID+"/start", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	// Complete
	req, _ = http.NewRequest("POST", ts.URL+"/api/jobs/"+jobID+"/complete", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	// Cancel
	req, _ = http.NewRequest("POST", ts.URL+"/api/jobs/"+jobID+"/cancel", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	info, err := cl.GetJob(context.Background(), jobID)
	require.NoError(t, err)
	require.Equal(t, cluster.JobState("cancelled"), info.Status)
}

func TestAPI_GetShardAssignmentsAndStatus(t *testing.T) {
	ts, cl, jobID := setupJobAPI(t)

	// Add 3 fake shards for the job
	_ = cl.BulkCreateShards(context.Background(), jobID, []cluster.ShardRange{
		{ShardID: 0, IndexFrom: 0, IndexTo: 10},
		{ShardID: 1, IndexFrom: 10, IndexTo: 20},
		{ShardID: 2, IndexFrom: 20, IndexTo: 30},
	})

	// Shard assignments (all)
	resp, err := http.Get(ts.URL + "/api/jobs/" + jobID + "/shards")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var assignments map[string]cluster.ShardAssignmentStatus
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&assignments))
	require.Len(t, assignments, 3)

	// Shard assignments (windowed)
	resp, err = http.Get(ts.URL + "/api/jobs/" + jobID + "/shards?start=0&end=2")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&assignments))
	require.Len(t, assignments, 3) // still all 3; adapt window logic as needed

	// Individual shard status
	resp, err = http.Get(ts.URL + "/api/jobs/" + jobID + "/shards/1")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var status cluster.ShardStatus
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&status))
}

func submitJobAndGetID(t *testing.T, serverURL, token string, spec *job.JobSpec) string {
	b, _ := json.Marshal(spec)
	req, _ := http.NewRequest("POST", serverURL+"/api/jobs", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	var out map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	return out["job_id"]
}

func getShardCount(t *testing.T, cl cluster.Cluster, jobID string) int {
	assignments, err := cl.GetShardAssignments(context.Background(), jobID)
	require.NoError(t, err)
	return len(assignments)
}

func TestAutoShardCreation(t *testing.T) {
	type tc struct {
		name      string
		start     int64
		end       int64
		shardSize int
		expect    int // Expected shard count
	}

	tests := []tc{
		{"1000-range, default", 0, 1000, 0, 2},
		{"10k-range, default", 0, 10_000, 0, 10},
		{"10k-range, shardSize-1k", 0, 10_000, 1000, 10},
		{"10k-range, shardSize-2k", 0, 10_000, 2000, 5},
		{"10k-range, shardSize-3000", 0, 10_000, 3000, 4},
		{"2500-range, default", 0, 2500, 0, 5},
		{"100-range, default", 0, 100, 0, 1},
	}

	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	server := setupTestServerWithCluster(cl)
	defer server.Close()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := &job.JobSpec{
				Version: "0.1.0",
				LogURI:  "https://example.com", // not actually fetched unless IndexEnd == 0
				Options: job.JobOptions{
					Fetch: job.FetchConfig{
						FetchSize:    10,
						FetchWorkers: 1,
						IndexStart:   tt.start,
						IndexEnd:     tt.end,
						ShardSize:    tt.shardSize,
					},
					Output: job.OutputOptions{
						Extractor:   "raw",
						Transformer: "passthrough",
						Sink:        "null",
					},
				},
			}
			jobID := submitJobAndGetID(t, server.URL, "testtoken", spec)
			shardCount := getShardCount(t, cl, jobID)
			require.Equal(t, tt.expect, shardCount, "shard count mismatch for %q", tt.name)
		})
	}
}

func TestAutoShardCreation_IndexEndZero(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()

	// Fake CT log HTTP server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ct/v1/get-sth" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"tree_size":2500}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	server := setupTestServerWithCluster(cl)
	defer server.Close()

	spec := &job.JobSpec{
		Version: "0.1.0",
		LogURI:  ts.URL, // Use test server as CT log
		Options: job.JobOptions{
			Fetch: job.FetchConfig{
				FetchSize:    10,
				FetchWorkers: 1,
				IndexStart:   0,
				IndexEnd:     0, // Should trigger fetchCTLogTreeSize
				ShardSize:    0, // Use default
			},
			Output: job.OutputOptions{
				Extractor:   "raw",
				Transformer: "passthrough",
				Sink:        "null",
			},
		},
	}
	jobID := submitJobAndGetID(t, server.URL, "testtoken", spec)
	shardCount := getShardCount(t, cl, jobID)
	require.Equal(t, 5, shardCount, "shard count should match for auto tree size (2500, default 500)")
}

func TestAPI_JobSubmission_BadInputs(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	server := setupTestServerWithCluster(cl)
	defer server.Close()

	type tc struct {
		name       string
		spec       *job.JobSpec
		expectCode int
		expectMsg  string
	}

	tests := []tc{
		{
			name:       "missing Fetch config",
			spec:       &job.JobSpec{Version: "1.0.0", LogURI: "test"},
			expectCode: 400,
		},
		{
			name: "negative IndexStart",
			spec: &job.JobSpec{
				Version: "1.0.0", LogURI: "test",
				Options: job.JobOptions{
					Fetch: job.FetchConfig{IndexStart: -10, IndexEnd: 100},
					Output: job.OutputOptions{
						Extractor:   "raw",
						Transformer: "passthrough",
						Sink:        "null",
					},
				},
			},
			expectCode: 400,
		},
		{
			name: "end less than start",
			spec: &job.JobSpec{
				Version: "1.0.0", LogURI: "test",
				Options: job.JobOptions{
					Fetch: job.FetchConfig{IndexStart: 100, IndexEnd: 50},
					Output: job.OutputOptions{
						Extractor:   "raw",
						Transformer: "passthrough",
						Sink:        "null",
					},
				},
			},
			expectCode: 400,
		},
		{
			name: "missing LogURI",
			spec: &job.JobSpec{
				Version: "1.0.0",
				Options: job.JobOptions{
					Fetch: job.FetchConfig{IndexStart: 0, IndexEnd: 100},
					Output: job.OutputOptions{
						Extractor:   "raw",
						Transformer: "passthrough",
						Sink:        "null",
					},
				},
			},
			expectCode: 400,
		},
		{
			name: "shardSize zero and range zero",
			spec: &job.JobSpec{
				Version: "1.0.0", LogURI: "test",
				Options: job.JobOptions{
					Fetch: job.FetchConfig{IndexStart: 0, IndexEnd: 0, ShardSize: 0},
					Output: job.OutputOptions{
						Extractor:   "raw",
						Transformer: "passthrough",
						Sink:        "null",
					},
				},
			},
			expectCode: 400,
		},
		{
			name:       "invalid JSON body",
			spec:       nil, // Will handle as raw bad JSON
			expectCode: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req *http.Request
			var err error
			if tt.name == "invalid JSON body" {
				req, err = http.NewRequest("POST", server.URL+"/api/jobs", strings.NewReader("{not-json}"))
				require.NoError(t, err)
			} else {
				b, _ := json.Marshal(tt.spec)
				req, err = http.NewRequest("POST", server.URL+"/api/jobs", bytes.NewReader(b))
				require.NoError(t, err)
				req.Header.Set("Content-Type", "application/json")
			}
			req.Header.Set("Authorization", "Bearer testtoken")
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, tt.expectCode, resp.StatusCode)

			// Optionally: check error message
			if tt.expectCode != 201 && tt.expectMsg != "" {
				var out map[string]interface{}
				_ = json.NewDecoder(resp.Body).Decode(&out)
				require.Contains(t, out["error"], tt.expectMsg)
			}
		})
	}
}
