package testcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/chtzvt/ctsnarf/internal/cluster"
	"github.com/chtzvt/ctsnarf/internal/job"
	"github.com/chtzvt/ctsnarf/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
)

// Start an embedded etcd cluster for test, return cluster + cleanup
func SetupEtcdCluster(t *testing.T) (cluster.Cluster, func()) {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.Logger = "zap"
	cfg.LogLevel = "error"
	e, err := embed.StartEtcd(cfg)
	require.NoError(t, err)

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		t.Fatal("etcd server did not become ready in time")
	}

	cl, err := cluster.NewEtcdCluster(cluster.EtcdConfig{
		Endpoints:   []string{e.Clients[0].Addr().String()},
		DialTimeout: 2 * time.Second,
		Prefix:      "/ctsnarf_test_" + testutil.RandString(5),
	})
	require.NoError(t, err)

	cleanup := func() {
		_ = cl.Close()
		e.Close()
	}
	return cl, cleanup
}

func DefaultTestJobOptions() job.JobOptions {
	return job.JobOptions{
		Output: job.OutputOptions{
			Extractor:   "dummy",
			Transformer: "dummy",
			Sink:        "null",
		},
	}
}

// SubmitTestJob creates a job using test-safe plugins if not provided.
func SubmitTestJob(t *testing.T, cl cluster.Cluster, logURI string, numShards int, opts ...job.JobOptions) string {
	t.Helper()
	options := DefaultTestJobOptions()
	if len(opts) > 0 {
		options = opts[0]
	}
	spec := &job.JobSpec{
		Version: "0.1.0",
		LogURI:  logURI,
		Options: options,
	}
	ctx := context.Background()
	jobID, err := cl.SubmitJob(ctx, spec)
	require.NoError(t, err)

	// Default: shards cover indices 0 .. numShards*100
	ranges := make([]cluster.ShardRange, numShards)
	for i := 0; i < numShards; i++ {
		ranges[i] = cluster.ShardRange{
			ShardID:   i,
			IndexFrom: int64(i * 100),
			IndexTo:   int64((i + 1) * 100),
		}
	}
	require.NoError(t, cl.BulkCreateShards(ctx, jobID, ranges))
	return jobID
}

// Wait until all shards are done for a jobID, with a timeout
func AllShardsDone(t *testing.T, cl cluster.Cluster, jobID string) bool {
	t.Helper()
	ctx := context.Background()
	assignments, err := cl.GetShardAssignments(ctx, jobID)
	require.NoError(t, err)
	for _, stat := range assignments {
		if !stat.Done && !stat.Failed {
			return false
		}
	}
	return true
}

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
