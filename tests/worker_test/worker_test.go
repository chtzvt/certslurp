package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/chtzvt/ctsnarf/internal/cluster"
	"github.com/chtzvt/ctsnarf/internal/job"
	"github.com/chtzvt/ctsnarf/internal/worker"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
)

func setupEtcdCluster(t *testing.T) (cluster.Cluster, func()) {
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.Logger = "zap"
	cfg.LogLevel = "error"
	e, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		t.Fatal("etcd not ready")
	}
	cl, err := cluster.NewEtcdCluster(cluster.EtcdConfig{
		Endpoints:   []string{e.Clients[0].Addr().String()},
		DialTimeout: 2 * time.Second,
		Prefix:      "/ctsnarf_worker_test",
	})
	require.NoError(t, err)
	cleanup := func() {
		cl.Close()
		e.Close()
	}
	return cl, cleanup
}

func TestWorkerE2EJobCompletion(t *testing.T) {
	cl, cleanup := setupEtcdCluster(t)
	defer cleanup()

	ctx := context.Background()
	spec := &job.JobSpec{
		Version: "0.1.0",
		LogURI:  "https://ct.googleapis.com/aviator",
		Options: job.JobOptions{},
	}
	jobID, err := cl.SubmitJob(ctx, spec)
	require.NoError(t, err)
	require.NotEmpty(t, jobID)

	// Create 5 shards for the job
	numShards := 5
	ranges := make([]cluster.ShardRange, numShards)
	for i := 0; i < numShards; i++ {
		ranges[i] = cluster.ShardRange{
			ShardID:   i,
			IndexFrom: int64(i * 100),
			IndexTo:   int64((i + 1) * 100),
		}
	}
	require.NoError(t, cl.BulkCreateShards(ctx, jobID, ranges))

	// Start 2 workers (goroutines)
	workerCount := 2
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	for i := 0; i < workerCount; i++ {
		go func(i int) {
			w, err := worker.New(worker.WorkerConfig{
				Cluster:    cl,
				WorkerInfo: cluster.WorkerInfo{Host: "workerhost"},
				JobID:      jobID,
				PollDelay:  100 * time.Millisecond,
				LeaseTTL:   2 * time.Second,
				Logf: func(format string, args ...interface{}) {
					// t.Logf(format, args...) // Uncomment for debug output
				},
			})
			require.NoError(t, err)
			_ = w.Run() // Runs until all shards are processed or test ends
			doneCh <- struct{}{}
		}(i)
	}

	// Wait for job to complete or timeout
	timeout := time.After(10 * time.Second)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
loop:
	for {
		select {
		case <-timeout:
			t.Fatalf("timeout waiting for job to complete")
		case <-tick.C:
			assignments, err := cl.GetShardAssignments(ctx, jobID)
			require.NoError(t, err)
			done := 0
			for _, stat := range assignments {
				if stat.Done || stat.Failed {
					done++
				}
			}
			if done == numShards {
				break loop
			}
		}
	}

	// Stop workers
	close(stopCh)
	for i := 0; i < workerCount; i++ {
		<-doneCh
	}

	// Final check: all shards should be done (not failed)
	assignments, err := cl.GetShardAssignments(ctx, jobID)
	require.NoError(t, err)
	for shardID, stat := range assignments {
		require.True(t, stat.Done, "shard %d not done", shardID)
		require.False(t, stat.Failed, "shard %d failed", shardID)
	}
}
