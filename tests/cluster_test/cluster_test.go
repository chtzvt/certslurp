package cluster_test

import (
	"context"
	"testing"
	"time"

	"github.com/chtzvt/ctsnarf/internal/cluster"
	"github.com/chtzvt/ctsnarf/internal/job"
	"github.com/chtzvt/ctsnarf/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestGetClusterStatus(t *testing.T) {
	cl, cleanup := testutil.SetupEtcdCluster(t)
	defer cleanup()
	ctx := context.Background()

	// Setup: add a job, worker, and two shards
	spec := &job.JobSpec{
		Version: "0.1.0",
		LogURI:  "https://ct.googleapis.com/aviator",
		Options: job.JobOptions{},
	}
	jobID, err := cl.SubmitJob(ctx, spec)
	require.NoError(t, err)

	require.NoError(t, cl.BulkCreateShards(ctx, jobID, []cluster.ShardRange{
		{ShardID: 0, IndexFrom: 0, IndexTo: 100},
		{ShardID: 1, IndexFrom: 100, IndexTo: 200},
	}))

	worker := cluster.WorkerInfo{Host: "testhost"}
	workerID, err := cl.RegisterWorker(ctx, worker)
	require.NoError(t, err)

	// Assign and finish a shard
	require.NoError(t, cl.AssignShard(ctx, jobID, 0, workerID))
	require.NoError(t, cl.ReportShardDone(ctx, jobID, 0, cluster.ShardManifest{OutputPath: "/tmp/shard0.jsonl"}))

	status, err := cl.GetClusterStatus(ctx)
	require.NoError(t, err)
	require.Len(t, status.Jobs, 1)
	require.Len(t, status.Workers, 1)
	js := status.Jobs[0]
	require.Equal(t, jobID, js.Job.ID)
	require.Len(t, js.Shards, 2)

	for _, s := range js.Shards {
		if s.ShardID == 0 {
			require.True(t, s.Done)
		}
	}
}

func TestCluster_CompactionSafety(t *testing.T) {
	cl, cleanup := testutil.SetupEtcdCluster(t)
	defer cleanup()
	ts := testutil.NewStubCTLogServer(t, testutil.CTLogFourEntrySTH, testutil.CTLogFourEntries)
	defer ts.Close()
	numShards := 50
	jobID := testutil.SubmitTestJob(t, cl, ts.URL, numShards)
	logger := testutil.NewTestLogger(true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	workers := testutil.RunWorkers(ctx, t, cl, jobID, 5, logger)
	testutil.WaitFor(t, func() bool {
		return testutil.AllShardsDone(t, cl, jobID)
	}, 8*time.Second, 200*time.Millisecond, "shards should finish")

	// Force compaction
	_, err := cl.Client().Compact(context.Background(), 0)
	require.NoError(t, err)

	// Submit another job, process, check that cluster is still healthy
	jobID2 := testutil.SubmitTestJob(t, cl, ts.URL, 3)
	workers2 := testutil.RunWorkers(ctx, t, cl, jobID2, 2, logger)
	testutil.WaitFor(t, func() bool {
		return testutil.AllShardsDone(t, cl, jobID2)
	}, 5*time.Second, 100*time.Millisecond, "second job should also complete")

	for _, w := range workers {
		w.Stop()
	}
	for _, w := range workers2 {
		w.Stop()
	}
}
