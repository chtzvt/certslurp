package cluster_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

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

func TestRequestShardSplit(t *testing.T) {
	cl, cleanup := setupEtcdCluster(t)
	defer cleanup()
	ctx := context.Background()
	jobID := "splitjob"
	require.NoError(t, cl.BulkCreateShards(ctx, jobID, []cluster.ShardRange{
		{ShardID: 10, IndexFrom: 0, IndexTo: 10000},
	}))
	err := cl.RequestShardSplit(ctx, jobID, 10, []cluster.ShardRange{
		{ShardID: 11, IndexFrom: 0, IndexTo: 5000},
		{ShardID: 12, IndexFrom: 5000, IndexTo: 10000},
	})
	require.NoError(t, err)

	// Check the split flag
	prefix := "/ctsnarf_test/jobs/splitjob/shards/10/split"
	resp, err := cl.Client().Get(ctx, prefix)
	require.NoError(t, err)
	require.NotEmpty(t, resp.Kvs)

	// Check new shards
	statusMap, err := cl.GetShardAssignments(ctx, jobID)
	require.NoError(t, err)
	require.Contains(t, statusMap, 11)
	require.Contains(t, statusMap, 12)
}

func TestReassignOrphanedShards(t *testing.T) {
	cl, cleanup := setupEtcdCluster(t)
	defer cleanup()
	ctx := context.Background()
	jobID := "orphanjob"
	require.NoError(t, cl.BulkCreateShards(ctx, jobID, []cluster.ShardRange{{ShardID: 0, IndexFrom: 0, IndexTo: 100}}))

	// Simulate orphan by assigning and then "expiring" lease
	_ = cl.AssignShard(ctx, jobID, 0, "deadworker")
	// Manually set lease expiry in the past
	prefix := "/ctsnarf_test/jobs/orphanjob/shards/0/assignment"
	resp, err := cl.Client().Get(ctx, prefix)
	require.NoError(t, err)
	if len(resp.Kvs) > 0 {
		var a cluster.ShardAssignment
		_ = json.Unmarshal(resp.Kvs[0].Value, &a)
		a.LeaseExpiry = time.Now().Add(-10 * time.Minute)
		b, _ := json.Marshal(a)
		cl.Client().Put(ctx, prefix, string(b))
	}

	orphans, err := cl.ReassignOrphanedShards(ctx, jobID, "newworker")
	require.NoError(t, err)
	require.Contains(t, orphans, 0)
}
