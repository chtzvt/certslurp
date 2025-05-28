package api

import (
	"bytes"
	"context"
	"encoding/base64"
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

func TestAuthRequired_AllEndpoints(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()

	protected := http.NewServeMux()
	RegisterJobHandlers(protected, cl)
	RegisterWorkerHandlers(protected, cl)
	RegisterSecretHandlers(protected, cl)

	// Wrap with auth middleware using some fake tokens
	tokens := []string{"testtoken"}
	handler := TokenAuthMiddleware(tokens, protected)

	// Try job endpoints
	requireUnauthorized(t, "GET", "/api/jobs", handler)
	requireUnauthorized(t, "POST", "/api/jobs", handler)
	requireUnauthorized(t, "GET", "/api/jobs/someid", handler)
	// Try worker endpoints
	requireUnauthorized(t, "GET", "/api/workers", handler)
	requireUnauthorized(t, "GET", "/api/workers/someworker", handler)
	// Try secrets endpoints
	requireUnauthorized(t, "GET", "/api/secrets/store", handler)
	requireUnauthorized(t, "GET", "/api/secrets/store/somekey", handler)
	requireUnauthorized(t, "POST", "/api/secrets/nodes/approve", handler)
}

func TestAuthRequired_InvalidToken(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()

	protected := http.NewServeMux()
	RegisterJobHandlers(protected, cl)
	handler := TokenAuthMiddleware([]string{"testtoken"}, protected)

	req := httptest.NewRequest("GET", "/api/jobs", nil)
	req.Header.Set("Authorization", "Bearer WRONGTOKEN")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestSubmitJob(t *testing.T) {
	server, _ := setupAuthTestServer("testtoken")
	defer server.Close()

	client := &http.Client{}
	body := `{"version":"1.0.0","log_uri":"test","options":{"fetch":{"fetch_size":10,"fetch_workers":1,"index_start":0,"index_end":100},"match":{},"output":{"extractor":"raw","transformer":"passthrough","sink":"null"}}}`
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
	spec := &job.JobSpec{Version: "1.0.0", LogURI: "test", Options: job.JobOptions{Fetch: job.FetchConfig{FetchSize: 10, FetchWorkers: 1}}}
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

	spec := &job.JobSpec{Version: "1.0.0", LogURI: "test", Options: job.JobOptions{Fetch: job.FetchConfig{FetchSize: 10, FetchWorkers: 1}}}
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

func TestWorkerMetricsEndpoints(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	ctx := context.Background()

	// Register a worker and send some metrics
	worker := cluster.WorkerInfo{Host: "testhost"}
	workerID, err := cl.RegisterWorker(ctx, worker)
	require.NoError(t, err)

	metrics := &cluster.WorkerMetrics{}
	metrics.IncProcessed()
	metrics.IncFailed()
	metrics.AddProcessingTime(123456789)
	require.NoError(t, cl.SendMetrics(ctx, workerID, metrics))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		protected := http.NewServeMux()
		RegisterWorkerHandlers(protected, cl)
		protected.ServeHTTP(w, r)
	}))
	defer srv.Close()

	// GET /api/workers
	resp, err := http.Get(srv.URL + "/api/workers")
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	var workers []*cluster.WorkerMetricsView
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&workers))
	require.NotEmpty(t, workers)
	require.Equal(t, workerID, workers[0].WorkerID)

	// GET /api/workers/{id}
	resp, err = http.Get(srv.URL + "/api/workers/" + workerID)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	var wv cluster.WorkerMetricsView
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&wv))
	require.Equal(t, workerID, wv.WorkerID)
}

func TestAPI_ListPendingNodes(t *testing.T) {
	server, cl := setupSecretsTestServer(t)
	store := cl.Secrets()
	ctx := context.TODO()

	// Simulate a pending registration
	nodeID := "node123"
	pubB64 := base64.StdEncoding.EncodeToString([]byte("publickey_32bytes______________"))
	_, err := store.Client().Put(ctx, store.Prefix()+"/registration/pending/"+nodeID, pubB64)
	require.NoError(t, err)

	req, _ := http.NewRequest("GET", server.URL+"/api/secrets/nodes/pending", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	var out []map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.True(t, len(out) >= 1)
	require.Equal(t, nodeID, out[0]["node_id"])
	require.Equal(t, pubB64, out[0]["public_key"])
}

func TestAPI_ApproveNode(t *testing.T) {
	server, cl := setupSecretsTestServer(t)
	store := cl.Secrets()
	ctx := context.TODO()

	// Simulate pending registration
	nodeID := "n321"
	pub := [32]byte{}
	copy(pub[:], []byte("pubkey_for_approve_node______"))
	pubB64 := base64.StdEncoding.EncodeToString(pub[:])
	_, err := store.Client().Put(ctx, store.Prefix()+"/registration/pending/"+nodeID, pubB64)
	require.NoError(t, err)

	// Create a cluster key
	var clusterKey [32]byte
	copy(clusterKey[:], []byte("supersecret32byteclusterkey__"))
	clusterKeyB64 := base64.StdEncoding.EncodeToString(clusterKey[:])

	body := map[string]string{"node_id": nodeID, "cluster_key": clusterKeyB64}
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", server.URL+"/api/secrets/nodes/approve", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 204, resp.StatusCode)

	// The sealed key should now be present and pending deleted
	kv, err := store.Client().Get(ctx, store.Prefix()+"/secrets/keys/"+nodeID)
	require.NoError(t, err)
	require.True(t, len(kv.Kvs) == 1)
	// The pending registration should be gone
	kv, err = store.Client().Get(ctx, store.Prefix()+"/registration/pending/"+nodeID)
	require.NoError(t, err)
	require.True(t, len(kv.Kvs) == 0)
}

func TestAPI_SecretStoreLifecycle(t *testing.T) {
	server, cl := setupSecretsTestServer(t)
	if cl == nil {
		t.Error("no cluster")
	}
	testKey := "topsecret"
	testValue := []byte("this is the real secret")

	// --- PUT ---
	b64Val := base64.StdEncoding.EncodeToString(testValue)
	putBody := `{"value":"` + b64Val + `"}`

	putReq, _ := http.NewRequest("PUT", server.URL+"/api/secrets/store/"+testKey, strings.NewReader(putBody))
	putReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(putReq)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 204, resp.StatusCode)

	// --- GET ---
	getReq, _ := http.NewRequest("GET", server.URL+"/api/secrets/store/"+testKey, nil)
	resp, err = http.DefaultClient.Do(getReq)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	var getOut map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&getOut))
	encVal, ok := getOut["value"]
	require.True(t, ok)
	// Value should be base64 encoded (still encrypted)
	require.NotEmpty(t, encVal)
	// Can't test raw value match, as it's encrypted, but should be base64
	_, err = base64.StdEncoding.DecodeString(encVal)
	require.NoError(t, err)

	// --- LIST ---
	listReq, _ := http.NewRequest("GET", server.URL+"/api/secrets/store", nil)
	resp, err = http.DefaultClient.Do(listReq)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	var keys []string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&keys))
	require.Contains(t, keys, testKey)

	// --- DELETE ---
	delReq, _ := http.NewRequest("DELETE", server.URL+"/api/secrets/store/"+testKey, nil)
	resp, err = http.DefaultClient.Do(delReq)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 204, resp.StatusCode)

	// --- GET (deleted) ---
	getReq2, _ := http.NewRequest("GET", server.URL+"/api/secrets/store/"+testKey, nil)
	resp, err = http.DefaultClient.Do(getReq2)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 404, resp.StatusCode)
}

func TestAPI_ListSecretsWithPrefix(t *testing.T) {
	server, cl := setupSecretsTestServer(t)
	store := cl.Secrets()
	ctx := context.TODO()

	_ = store.Set(ctx, "a/b/c", []byte("v1"))
	_ = store.Set(ctx, "a/b/d", []byte("v2"))
	_ = store.Set(ctx, "x/y/z", []byte("v3"))

	req, _ := http.NewRequest("GET", server.URL+"/api/secrets/store?prefix=a/b", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	var keys []string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&keys))
	require.Contains(t, keys, "a/b/c")
	require.Contains(t, keys, "a/b/d")
	require.NotContains(t, keys, "x/y/z")
}
