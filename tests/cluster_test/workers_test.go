package cluster_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/chtzvt/ctsnarf/internal/cluster"
	"github.com/chtzvt/ctsnarf/internal/testutil"
	"github.com/chtzvt/ctsnarf/internal/worker"
	"github.com/stretchr/testify/require"
)

func TestWorkerLifecycle(t *testing.T) {
	cl, cleanup := testutil.SetupEtcdCluster(t)
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
	cl, cleanup := testutil.SetupEtcdCluster(t)
	defer cleanup()
	ts := testutil.NewStubCTLogServer(t, testutil.CTLogFourEntrySTH, testutil.CTLogFourEntries)
	defer ts.Close()
	numShards := 10
	jobID := testutil.SubmitTestJob(t, cl, ts.URL, numShards)
	logger := testutil.NewTestLogger(true)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	workerCount := 5
	for i := 0; i < workerCount; i++ {
		w := worker.NewWorker(cl, jobID, fmt.Sprintf("churn-%d", i), logger)
		go func(w *worker.Worker) {
			for j := 0; j < 3; j++ { // Start/stop each worker multiple times
				wCtx, cancel := context.WithTimeout(ctx, time.Duration(500+100*j)*time.Millisecond)
				go func() { _ = w.Run(wCtx) }()
				time.Sleep(300 * time.Millisecond)
				cancel()
			}
		}(w)
	}
	testutil.WaitFor(t, func() bool {
		return testutil.AllShardsDone(t, cl, jobID)
	}, 7*time.Second, 150*time.Millisecond, "all shards complete despite churn")
}
