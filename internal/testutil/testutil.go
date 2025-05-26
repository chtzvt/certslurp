package testutil

import (
	"context"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/chtzvt/ctsnarf/internal/cluster"
	"github.com/chtzvt/ctsnarf/internal/job"
	"github.com/chtzvt/ctsnarf/internal/worker"
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
		Prefix:      "/ctsnarf_test_" + RandString(5),
	})
	require.NoError(t, err)

	cleanup := func() {
		_ = cl.Close()
		e.Close()
	}
	return cl, cleanup
}

// Random string for unique prefixes
func RandString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyz0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[time.Now().UnixNano()%int64(len(letters))]
	}
	return string(b)
}

// Submit a job and create shards
func SubmitTestJob(t *testing.T, cl cluster.Cluster, logURI string, numShards int, opts ...job.JobOptions) string {
	t.Helper()
	options := job.JobOptions{}
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

// Run N workers in parallel; returns a slice of workers for further control
func RunWorkers(ctx context.Context, t *testing.T, cl cluster.Cluster, jobID string, workerCount int, logger *log.Logger) []*worker.Worker {
	t.Helper()
	var wg sync.WaitGroup
	workers := make([]*worker.Worker, workerCount)
	for i := 0; i < workerCount; i++ {
		id := "worker-" + RandString(5)
		w := worker.NewWorker(cl, jobID, id, logger)
		workers[i] = w
		wg.Add(1)
		go func(w *worker.Worker) {
			defer wg.Done()
			_ = w.Run(ctx)
		}(w)
	}
	// NOTE: You can wait on wg.Wait() if you want to wait for all workers to finish
	return workers
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

// Utility: Wait for a condition or timeout
func WaitFor(t *testing.T, cond func() bool, timeout time.Duration, tick time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(tick)
	}
	t.Fatalf("WaitFor timeout: %s", msg)
}

// Helper for creating a test logger that discards or logs as needed
func NewTestLogger(discard bool) *log.Logger {
	if discard {
		return log.New(os.Stderr, "[worker] ", log.LstdFlags)
	}
	return log.New(os.Stdout, "[worker] ", log.LstdFlags)
}
