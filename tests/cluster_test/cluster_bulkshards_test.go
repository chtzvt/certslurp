package cluster_test

import (
	"context"
	"testing"

	"github.com/chtzvt/certslurp/internal/cluster"
	"github.com/chtzvt/certslurp/internal/testcluster"
	"github.com/stretchr/testify/require"
)

func TestBulkCreateAndGetShardStatus(t *testing.T) {
	// Start a fresh etcd (details omitted, see previous test example)
	c, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()

	jobID := "testjob"
	shards := []cluster.ShardRange{
		{ShardID: 0, IndexFrom: 0, IndexTo: 1000},
		{ShardID: 1, IndexFrom: 1000, IndexTo: 2000},
	}

	ctx := context.Background()
	if err := c.BulkCreateShards(ctx, jobID, shards); err != nil {
		t.Fatalf("BulkCreateShards: %v", err)
	}

	// Should be idempotent
	if err := c.BulkCreateShards(ctx, jobID, shards); err != nil {
		t.Fatalf("BulkCreateShards 2nd call: %v", err)
	}

	for _, s := range shards {
		status, err := c.GetShardStatus(ctx, jobID, s.ShardID)
		if err != nil {
			t.Errorf("GetShardStatus(%d): %v", s.ShardID, err)
		}
		if status.Assigned {
			t.Errorf("Shard %d should not be assigned yet", s.ShardID)
		}
		if status.Done || status.Failed {
			t.Errorf("Shard %d should not be done/failed yet", s.ShardID)
		}
	}
}

func TestBulkCreateShards_SetsShardCountOnce(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	ctx := context.Background()
	jobID := "countjob"
	shards := []cluster.ShardRange{
		{ShardID: 0, IndexFrom: 0, IndexTo: 100},
		{ShardID: 1, IndexFrom: 100, IndexTo: 200},
		{ShardID: 2, IndexFrom: 200, IndexTo: 300},
	}
	// First call: should store shard count
	require.NoError(t, cl.BulkCreateShards(ctx, jobID, shards))
	count, err := cl.GetShardCount(ctx, jobID)
	require.NoError(t, err)
	require.Equal(t, 3, count)

	// Second call: count should NOT change, even if you try with fewer shards (should remain at first set)
	lessShards := []cluster.ShardRange{
		{ShardID: 0, IndexFrom: 0, IndexTo: 100},
	}
	require.NoError(t, cl.BulkCreateShards(ctx, jobID, lessShards))
	count2, err := cl.GetShardCount(ctx, jobID)
	require.NoError(t, err)
	require.Equal(t, 3, count2)
}
