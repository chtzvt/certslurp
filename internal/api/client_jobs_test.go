package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/chtzvt/certslurp/internal/cluster"
	"github.com/stretchr/testify/require"
)

func TestClient_UpdateJobStatus(t *testing.T) {
	jobID := "abc"
	status := "running"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/jobs/"+jobID+"/status", r.URL.Path)
		require.Equal(t, "PATCH", r.Method)
		var req struct{ Status string }
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.Equal(t, status, req.Status)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "tok")
	err := client.UpdateJobStatus(context.Background(), jobID, cluster.JobState(status))
	require.NoError(t, err)
}

func TestClient_MarkJobStartedCompletedCancelled(t *testing.T) {
	jobID := "abc"
	paths := []string{"/api/jobs/" + jobID + "/start", "/api/jobs/" + jobID + "/complete", "/api/jobs/" + jobID + "/cancel"}
	for _, p := range paths {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, p, r.URL.Path)
			require.Equal(t, "POST", r.Method)
			w.WriteHeader(http.StatusNoContent)
		}))
		client := NewClient(srv.URL, "tok")
		var err error
		switch {
		case strings.Contains(p, "/start"):
			err = client.MarkJobStarted(context.Background(), jobID)
		case strings.Contains(p, "/complete"):
			err = client.MarkJobCompleted(context.Background(), jobID)
		case strings.Contains(p, "/cancel"):
			err = client.CancelJob(context.Background(), jobID)
		}
		require.NoError(t, err)
		srv.Close()
	}
}

func TestClient_GetShardAssignments(t *testing.T) {
	jobID := "abc"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/jobs/"+jobID+"/shards", r.URL.Path)
		require.Equal(t, "GET", r.Method)
		assignments := map[int]cluster.ShardAssignmentStatus{
			0: {ShardID: 0, WorkerID: "w1"},
			1: {ShardID: 1, WorkerID: "w2"},
		}
		_ = json.NewEncoder(w).Encode(assignments)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "tok")
	result, err := client.GetShardAssignments(context.Background(), jobID, nil, nil)
	require.NoError(t, err)
	require.Len(t, result, 2)
}

func TestClient_GetShardAssignmentsWindow(t *testing.T) {
	jobID := "abc"
	start, end := 0, 1
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/jobs/"+jobID+"/shards", r.URL.Path)
		require.Equal(t, "GET", r.Method)
		require.Contains(t, r.URL.RawQuery, "start=0")
		require.Contains(t, r.URL.RawQuery, "end=1")
		assignments := map[int]cluster.ShardAssignmentStatus{
			0: {ShardID: 0, WorkerID: "w1"},
		}
		_ = json.NewEncoder(w).Encode(assignments)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "tok")
	result, err := client.GetShardAssignments(context.Background(), jobID, &start, &end)
	require.NoError(t, err)
	require.Len(t, result, 1)
}

func TestClient_GetShardStatus(t *testing.T) {
	jobID := "abc"
	shardID := 2
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/jobs/"+jobID+"/shards/2", r.URL.Path)
		require.Equal(t, "GET", r.Method)
		status := cluster.ShardStatus{WorkerID: "someworker"}
		_ = json.NewEncoder(w).Encode(status)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "tok")
	status, err := client.GetShardStatus(context.Background(), jobID, shardID)
	require.NoError(t, err)
	require.Equal(t, "someworker", status.WorkerID)
}

func TestClient_ResetFailedShards(t *testing.T) {
	// Simulate a response for the ResetFailedShards endpoint
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "POST", r.Method)
		require.Contains(t, r.URL.Path, "/reset-failed")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"reset_shards": []int{0, 1}})
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "tok")
	shards, err := client.ResetFailedShards(context.Background(), "jobid")
	require.NoError(t, err)
	require.ElementsMatch(t, []int{0, 1}, shards)
}

func TestClient_ResetFailedShard(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "POST", r.Method)
		require.Contains(t, r.URL.Path, "/shards/0/reset-failed")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "tok")
	err := client.ResetFailedShard(context.Background(), "jobid", 0)
	require.NoError(t, err)
}
