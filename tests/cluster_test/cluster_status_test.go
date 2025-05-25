package cluster_test

import (
	"context"
	"testing"

	"github.com/chtzvt/ctsnarf/internal/cluster"
	"github.com/chtzvt/ctsnarf/internal/job"
	"github.com/stretchr/testify/require"
)

func TestGetClusterStatus(t *testing.T) {
	cl, cleanup := setupEtcdCluster(t)
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
