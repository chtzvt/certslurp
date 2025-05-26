package cluster_test

import (
	"context"
	"testing"

	"github.com/chtzvt/ctsnarf/internal/cluster"
	"github.com/chtzvt/ctsnarf/internal/testutil"
)

func TestBulkCreateAndGetShardStatus(t *testing.T) {
	// Start a fresh etcd (details omitted, see previous test example)
	c, cleanup := testutil.SetupEtcdCluster(t)
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
