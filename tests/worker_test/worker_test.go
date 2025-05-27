package worker_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chtzvt/certslurp/internal/testcluster"
	"github.com/chtzvt/certslurp/internal/testutil"
	"github.com/chtzvt/certslurp/internal/testworkers"
	"github.com/chtzvt/certslurp/internal/worker"
	"github.com/stretchr/testify/require"
)

func TestWorkerE2EJobCompletion(t *testing.T) {
	ts := testutil.NewStubCTLogServer(t, testutil.CTLogFourEntrySTH, testutil.CTLogFourEntries)
	defer ts.Close()
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()

	jobID := testcluster.SubmitTestJob(t, cl, ts.URL, 2)
	logger := testutil.NewTestLogger(true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	workers := testworkers.RunWorkers(ctx, t, cl, jobID, 2, logger)

	testutil.WaitFor(t, func() bool {
		return testcluster.AllShardsDone(t, cl, jobID)
	}, 5*time.Second, 100*time.Millisecond, "job should complete")

	for _, w := range workers {
		w.Stop()
	}
}

func TestE2E_JobHappyPath(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	ts := testutil.NewStubCTLogServer(t, testutil.CTLogFourEntrySTH, testutil.CTLogFourEntries)
	defer ts.Close()
	jobID := testcluster.SubmitTestJob(t, cl, ts.URL, 4)
	logger := testutil.NewTestLogger(true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	workers := testworkers.RunWorkers(ctx, t, cl, jobID, 2, logger)
	testutil.WaitFor(t, func() bool {
		return testcluster.AllShardsDone(t, cl, jobID)
	}, 5*time.Second, 100*time.Millisecond, "job should complete")
	for _, w := range workers {
		w.Stop()
	}
}

// Simulate a worker dying mid-job: test reassign and recovery
func TestCluster_WorkerFailureRecovery(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	ts := testutil.NewStubCTLogServer(t, testutil.CTLogFourEntrySTH, testutil.CTLogFourEntries)
	defer ts.Close()
	jobID := testcluster.SubmitTestJob(t, cl, ts.URL, 4)
	logger := testutil.NewTestLogger(true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	workerCount := 2
	doneCh := make(chan struct{}, workerCount)
	workers := make([]*worker.Worker, workerCount)
	killed := int32(0)
	for i := 0; i < workerCount; i++ {
		w := worker.NewWorker(cl, fmt.Sprintf("fail-%d", i), logger)
		workers[i] = w
		go func(idx int, w *worker.Worker) {
			workerCtx, cancel := context.WithCancel(ctx)
			if idx == 0 { // kill first worker after a moment
				go func() {
					time.Sleep(1 * time.Second)
					cancel()
					atomic.StoreInt32(&killed, 1)
				}()
			}
			_ = w.Run(workerCtx)
			doneCh <- struct{}{}
		}(i, w)
	}
	testutil.WaitFor(t, func() bool {
		return testcluster.AllShardsDone(t, cl, jobID)
	}, 8*time.Second, 100*time.Millisecond, "all shards should finish even if a worker died")

	for _, w := range workers {
		w.Stop()
	}
	for i := 0; i < workerCount; i++ {
		<-doneCh
	}
	require.Equal(t, int32(1), killed, "simulated one worker kill")
	assignments, err := cl.GetShardAssignments(context.Background(), jobID)
	require.NoError(t, err)
	for shardID, stat := range assignments {
		require.True(t, stat.Done, "shard %d not done", shardID)
		require.False(t, stat.Failed, "shard %d failed", shardID)
	}
}

// Test racing for the same shard assignment
func TestCluster_ConcurrentAssignment(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	ts := testutil.NewStubCTLogServer(t, testutil.CTLogFourEntrySTH, testutil.CTLogFourEntries)
	defer ts.Close()
	jobID := testcluster.SubmitTestJob(t, cl, ts.URL, 1) // just one shard!
	shardID := 0
	workerCount := 10
	var wg sync.WaitGroup
	wg.Add(workerCount)
	var success int32
	for i := 0; i < workerCount; i++ {
		go func(idx int) {
			defer wg.Done()
			err := cl.AssignShard(context.Background(), jobID, shardID, fmt.Sprintf("w%d", idx))
			if err == nil {
				atomic.AddInt32(&success, 1)
			}
		}(i)
	}
	wg.Wait()
	require.Equal(t, int32(1), success, "only one worker should claim shard")
}

// Large-scale stress test: 10000 shards, 100 workers
func TestCluster_LargeScaleStress(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	ts := testutil.NewStubCTLogServer(t, testutil.CTLogFourEntrySTH, testutil.CTLogFourEntries)
	defer ts.Close()
	numShards := 1000 // start smaller for CI sanity, can go higher locally
	workerCount := 20
	jobID := testcluster.SubmitTestJob(t, cl, ts.URL, numShards)
	logger := testutil.NewTestLogger(true)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	workers := testworkers.RunWorkers(ctx, t, cl, jobID, workerCount, logger)
	testutil.WaitFor(t, func() bool {
		return testcluster.AllShardsDone(t, cl, jobID)
	}, 18*time.Second, 300*time.Millisecond, "large job should complete")
	for _, w := range workers {
		w.Stop()
	}
	assignments, err := cl.GetShardAssignments(context.Background(), jobID)
	require.NoError(t, err)
	doneCount := 0
	for _, stat := range assignments {
		if stat.Done {
			doneCount++
		}
		require.False(t, stat.Failed, "no shard should fail")
	}
	require.Equal(t, numShards, doneCount, "all shards done")
}
