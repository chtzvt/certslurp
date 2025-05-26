package testutil

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/chtzvt/ctsnarf/internal/cluster"
	"github.com/stretchr/testify/require"
)

// ExpireShardLease forcibly expires a shard lease for the given job/shard.
func ExpireShardLease(t *testing.T, cl cluster.Cluster, jobID string, shardID int) {
	t.Helper()
	ctx := context.Background()

	// Get etcd key for the assignment
	prefix := cl.Prefix() + "/jobs/" + jobID + "/shards/" + itoa(shardID) + "/assignment"
	resp, err := cl.Client().Get(ctx, prefix)
	require.NoError(t, err)
	require.NotEmpty(t, resp.Kvs, "no assignment found for shard %d", shardID)

	var a cluster.ShardAssignment
	err = json.Unmarshal(resp.Kvs[0].Value, &a)
	require.NoError(t, err)

	// Set lease expiry in the past
	a.LeaseExpiry = time.Now().Add(-10 * time.Minute)
	b, err := json.Marshal(a)
	require.NoError(t, err)

	_, err = cl.Client().Put(ctx, prefix, string(b))
	require.NoError(t, err)
}

// Helper: convert int to string
func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}
