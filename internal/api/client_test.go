package api

import (
	"context"
	"encoding/base64"
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
	expected := []WorkerStatus{
		{
			ID: "w1", Host: "host1", LastSeen: fixed,
			ShardsProcessed: 12, ShardsFailed: 1,
			ProcessingTimeNs: 123456789,
			LastUpdated:      fixed,
		},
		{
			ID: "w2", Host: "host2", LastSeen: fixed,
			ShardsProcessed: 99, ShardsFailed: 0,
			ProcessingTimeNs: 6543210,
			LastUpdated:      fixed,
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer testtoken", r.Header.Get("Authorization"))
		require.Equal(t, "/api/workers", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expected)
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
		require.Equal(t, "/api/workers/w1", r.URL.Path)
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

func TestClient_ListPendingNodes(t *testing.T) {
	respData := []map[string]string{
		{"node_id": "abc123", "public_key": "b64pubkey"},
		{"node_id": "xyz789", "public_key": "pubkey2"},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer testtoken", r.Header.Get("Authorization"))
		require.Equal(t, "/api/secrets/nodes/pending", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(respData)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "testtoken")
	nodes, err := client.ListPendingNodes(context.Background())
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	require.Equal(t, "abc123", nodes[0].NodeID)
	require.Equal(t, "b64pubkey", nodes[0].PubKeyB64)
}

func TestClient_ApproveNode(t *testing.T) {
	// Expect: POST body with node_id
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		require.Equal(t, "Bearer testtoken", r.Header.Get("Authorization"))
		require.Equal(t, "/api/secrets/nodes/approve", r.URL.Path)
		require.Equal(t, "POST", r.Method)
		var req map[string]string
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.Equal(t, "n123", req["node_id"])
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "testtoken")
	err := client.ApproveNode(context.Background(), "n123")
	require.NoError(t, err)
	require.True(t, called)
}

func TestClient_ListSecrets(t *testing.T) {
	keys := []string{"a", "b/x", "c"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer testtoken", r.Header.Get("Authorization"))
		// test prefix handling
		require.Contains(t, r.URL.String(), "/api/secrets/store")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(keys)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "testtoken")
	out, err := client.ListSecrets(context.Background(), "")
	require.NoError(t, err)
	require.Equal(t, keys, out)

	// with prefix
	_, err = client.ListSecrets(context.Background(), "b/")
	require.NoError(t, err)
}

func TestClient_GetSecret(t *testing.T) {
	expected := []byte("my encrypted value")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer testtoken", r.Header.Get("Authorization"))
		require.Contains(t, r.URL.Path, "/api/secrets/store/mykey")
		val := base64.StdEncoding.EncodeToString(expected)
		_ = json.NewEncoder(w).Encode(map[string]string{"value": val})
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "testtoken")
	val, err := client.GetSecret(context.Background(), "mykey")
	require.NoError(t, err)
	require.Equal(t, expected, val)
}

func TestClient_PutSecret(t *testing.T) {
	received := []byte{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "PUT", r.Method)
		require.Equal(t, "Bearer testtoken", r.Header.Get("Authorization"))
		var req map[string]string
		_ = json.NewDecoder(r.Body).Decode(&req)
		data, err := base64.StdEncoding.DecodeString(req["value"])
		require.NoError(t, err)
		received = data
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "testtoken")
	val := []byte("write this")
	err := client.PutSecret(context.Background(), "mykey", val)
	require.NoError(t, err)
	require.Equal(t, val, received)
}

func TestClient_DeleteSecret(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		require.Equal(t, "DELETE", r.Method)
		require.Equal(t, "Bearer testtoken", r.Header.Get("Authorization"))
		require.Contains(t, r.URL.Path, "/api/secrets/store/mykey")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "testtoken")
	err := client.DeleteSecret(context.Background(), "mykey")
	require.NoError(t, err)
	require.True(t, called)
}
