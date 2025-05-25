package cluster_test

import (
	"context"
	"testing"

	"github.com/chtzvt/ctsnarf/internal/cluster"
	"github.com/stretchr/testify/require"
)

func TestBulkCreateAndShardAssignmentLifecycle(t *testing.T) {
	cl, cleanup := setupEtcdCluster(t)
	defer cleanup()

	ctx := context.Background()
	jobID := "testjob"
	shards := []cluster.ShardRange{
		{ShardID: 0, IndexFrom: 0, IndexTo: 1000},
		{ShardID: 1, IndexFrom: 1000, IndexTo: 2000},
	}
	require.NoError(t, cl.BulkCreateShards(ctx, jobID, shards))

	// Idempotency
	require.NoError(t, cl.BulkCreateShards(ctx, jobID, shards))

	// No assignment/done yet
	statusMap, err := cl.GetShardAssignments(ctx, jobID)
	require.NoError(t, err)
	require.Len(t, statusMap, 2)
	for _, s := range shards {
		stat := statusMap[s.ShardID]
		require.Equal(t, s.IndexFrom, stat.IndexFrom)
		require.Equal(t, s.IndexTo, stat.IndexTo)
		require.False(t, stat.Assigned)
		require.False(t, stat.Done)
		require.False(t, stat.Failed)
	}

	// Assign a shard
	workerID := "worker1"
	require.NoError(t, cl.AssignShard(ctx, jobID, 0, workerID))

	// Assignment status updates
	stat, err := cl.GetShardStatus(ctx, jobID, 0)
	require.NoError(t, err)
	require.True(t, stat.Assigned)
	require.Equal(t, workerID, stat.WorkerID)
	require.False(t, stat.Done)
	require.False(t, stat.Failed)

	// Mark done
	manifest := cluster.ShardManifest{OutputPath: "/tmp/shard0.jsonl"}
	require.NoError(t, cl.ReportShardDone(ctx, jobID, 0, manifest))
	stat, err = cl.GetShardStatus(ctx, jobID, 0)
	require.NoError(t, err)
	require.True(t, stat.Done)
	require.Equal(t, "/tmp/shard0.jsonl", stat.OutputPath)

	// Assign and fail the other shard (with retries/backoff)
	require.NoError(t, cl.AssignShard(ctx, jobID, 1, "worker2"))
	for i := 0; i < 4; i++ {
		err := cl.ReportShardFailed(ctx, jobID, 1)
		if i < 3 {
			require.NoError(t, err)
			s, _ := cl.GetShardStatus(ctx, jobID, 1)
			require.Equal(t, i+1, s.Retries)
			require.False(t, s.Failed)
			require.False(t, s.Done)
		} else {
			require.NoError(t, err)
			s, _ := cl.GetShardStatus(ctx, jobID, 1)
			require.True(t, s.Failed, "should be permanently failed after max retries")
			require.True(t, s.Done)
		}
	}
}
