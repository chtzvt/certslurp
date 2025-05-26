package worker_test

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/chtzvt/ctsnarf/internal/cluster"
	"github.com/chtzvt/ctsnarf/internal/job"
	"github.com/chtzvt/ctsnarf/internal/testutil"
	"github.com/chtzvt/ctsnarf/internal/worker"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
)

func setupEtcdCluster(t *testing.T) (cluster.Cluster, func()) {
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
		Prefix:      "/ctsnarf_test",
	})
	require.NoError(t, err)

	cleanup := func() {
		_ = cl.Close()
		e.Close()
	}
	return cl, cleanup
}

func TestWorkerE2EJobCompletion(t *testing.T) {
	ts := testutil.NewStubCTLogServer(t, testutil.CTLogFourEntrySTH, testutil.CTLogFourEntries)
	defer ts.Close()

	cl, cleanup := setupEtcdCluster(t)
	defer cleanup()

	ctx := context.Background()
	spec := &job.JobSpec{
		Version: "0.1.0",
		LogURI:  ts.URL,
		Options: job.JobOptions{
			Fetch: job.FetchConfig{
				BatchSize:  8,
				Workers:    1,
				IndexStart: 0,
				IndexEnd:   4,
			},
		},
	}
	jobID, err := cl.SubmitJob(ctx, spec)
	require.NoError(t, err)
	require.NotEmpty(t, jobID)

	// Create 5 shards for the job (update as appropriate for the stub data)
	numShards := 2
	ranges := make([]cluster.ShardRange, numShards)
	for i := 0; i < numShards; i++ {
		ranges[i] = cluster.ShardRange{
			ShardID:   i,
			IndexFrom: int64(i * 2),
			IndexTo:   int64((i + 1) * 2),
		}
	}
	require.NoError(t, cl.BulkCreateShards(ctx, jobID, ranges))

	// Start 2 workers
	workerCount := 2
	doneCh := make(chan struct{}, workerCount)
	workers := make([]*worker.Worker, workerCount)
	var logger = log.New(os.Stderr, "[worker] ", log.LstdFlags)

	for i := 0; i < workerCount; i++ {
		id := "worker-" + string(rune('A'+i))
		w := worker.NewWorker(cl, jobID, id, logger)
		workers[i] = w
		go func(w *worker.Worker) {
			_ = w.Run(ctx)
			doneCh <- struct{}{}
		}(w)
	}

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
	for _, w := range workers {
		w.Stop()
	}
	for i := 0; i < workerCount; i++ {
		<-doneCh
	}
	assignments, err := cl.GetShardAssignments(ctx, jobID)
	require.NoError(t, err)
	for shardID, stat := range assignments {
		require.True(t, stat.Done, "shard %d not done", shardID)
		require.False(t, stat.Failed, "shard %d failed", shardID)
	}
}
