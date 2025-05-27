package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/chtzvt/certslurp/internal/cluster"
	"github.com/stretchr/testify/require"
)

func TestClient_ListWorkers(t *testing.T) {
	fixed := time.Date(2025, 5, 27, 19, 18, 19, 0, time.UTC)
	expected := []cluster.WorkerInfo{
		{ID: "w1", Host: "host1", LastSeen: fixed},
		{ID: "w2", Host: "host2", LastSeen: fixed},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer testtoken", r.Header.Get("Authorization"))
		require.Equal(t, "/api/workers", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(expected)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "testtoken")

	workers, err := client.ListWorkers(context.Background())
	require.NoError(t, err)
	require.Equal(t, expected, workers)
}

func TestClient_GetWorkerMetrics(t *testing.T) {
	expected := cluster.WorkerMetricsView{
		WorkerID:         "w1",
		ShardsProcessed:  42,
		ShardsFailed:     2,
		ProcessingTimeNs: int64(123 * time.Second),
		LastUpdated:      time.Now(),
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer testtoken", r.Header.Get("Authorization"))
		require.Equal(t, "/api/workers/w1/metrics", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(expected)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "testtoken")
	metrics, err := client.GetWorkerMetrics(context.Background(), "w1")
	require.NoError(t, err)
	require.Equal(t, expected.WorkerID, metrics.WorkerID)
	require.Equal(t, expected.ShardsProcessed, metrics.ShardsProcessed)
	require.Equal(t, expected.ShardsFailed, metrics.ShardsFailed)
	require.Equal(t, expected.ProcessingTimeNs, metrics.ProcessingTimeNs)
}
