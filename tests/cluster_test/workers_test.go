package cluster_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/chtzvt/certslurp/internal/cluster"
	"github.com/chtzvt/certslurp/internal/testcluster"
	"github.com/chtzvt/certslurp/internal/testutil"
	"github.com/chtzvt/certslurp/internal/worker"
	"github.com/stretchr/testify/require"
)

func TestWorkerLifecycle(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()

	ctx := context.Background()
	worker := cluster.WorkerInfo{Host: "testhost"}
	workerID, err := cl.RegisterWorker(ctx, worker)
	require.NoError(t, err)
	require.NotEmpty(t, workerID)

	workers, err := cl.ListWorkers(ctx)
	require.NoError(t, err)
	require.Len(t, workers, 1)
	require.Equal(t, workerID, workers[0].ID)

	// Heartbeat works
	require.NoError(t, cl.HeartbeatWorker(ctx, workerID))
}

func TestCluster_RapidWorkerChurn(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	ts := testutil.NewStubCTLogServer(t, testutil.CTLogFourEntrySTH, testutil.CTLogFourEntries)
	defer ts.Close()
	numShards := 10
	jobID := testcluster.SubmitTestJob(t, cl, ts.URL, numShards)
	logger := testutil.NewTestLogger(true)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	workerCount := 5
	for i := 0; i < workerCount; i++ {
		go func(workerIdx int) {
			for j := 0; j < 3; j++ {
				w := worker.NewWorker(cl, fmt.Sprintf("churn-%d-%d", workerIdx, j), logger)
				wCtx, cancel := context.WithTimeout(ctx, time.Duration(500+100*j)*time.Millisecond)
				go func() { _ = w.Run(wCtx) }()
				time.Sleep(300 * time.Millisecond)
				cancel()
			}
		}(i)
	}
	testutil.WaitFor(t, func() bool {
		return testcluster.AllShardsDone(t, cl, jobID)
	}, 7*time.Second, 150*time.Millisecond, "all shards complete despite churn")
}

func TestSendMetrics(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()

	ctx := context.Background()
	worker := cluster.WorkerInfo{Host: "testhost"}
	workerID, err := cl.RegisterWorker(ctx, worker)
	require.NoError(t, err)
	require.NotEmpty(t, workerID)

	// Simulate some activity
	metrics := &cluster.WorkerMetrics{}
	metrics.IncProcessed()
	metrics.IncProcessed()
	metrics.IncFailed()
	metrics.AddProcessingTime(5 * time.Second)

	// Send metrics
	require.NoError(t, cl.SendMetrics(ctx, workerID, metrics))

	// Read back from etcd to assert
	key := cl.Prefix() + "/workers/" + workerID + "/shards_processed"
	val := testcluster.MustGetEtcdKey(t, cl.Client(), key)
	require.Equal(t, "2", val)

	key = cl.Prefix() + "/workers/" + workerID + "/shards_failed"
	val = testcluster.MustGetEtcdKey(t, cl.Client(), key)
	require.Equal(t, "1", val)

	key = cl.Prefix() + "/workers/" + workerID + "/processing_time_ns"
	val = testcluster.MustGetEtcdKey(t, cl.Client(), key)
	i, err := strconv.ParseInt(val, 10, 64)
	require.NoError(t, err)
	require.True(t, i >= int64(5*time.Second))
}

func TestGetWorkerMetrics(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	ctx := context.Background()

	// Register a worker
	worker := cluster.WorkerInfo{Host: "metrics-host"}
	workerID, err := cl.RegisterWorker(ctx, worker)
	require.NoError(t, err)
	require.NotEmpty(t, workerID)

	// Simulate metrics
	metrics := &cluster.WorkerMetrics{}
	metrics.IncProcessed()
	metrics.IncProcessed()
	metrics.IncFailed()
	metrics.AddProcessingTime(42 * time.Second)

	// Send metrics
	require.NoError(t, cl.SendMetrics(ctx, workerID, metrics))

	// Retrieve metrics
	vm, err := cl.GetWorkerMetrics(ctx, workerID)
	require.NoError(t, err)
	require.NotNil(t, vm)

	require.Equal(t, workerID, vm.WorkerID)
	require.EqualValues(t, 2, vm.ShardsProcessed)
	require.EqualValues(t, 1, vm.ShardsFailed)
	require.GreaterOrEqual(t, vm.ProcessingTimeNs, int64(42*time.Second))
	require.False(t, vm.LastUpdated.IsZero())
}
