package cluster_test

import (
	"context"
	"testing"
	"time"

	"github.com/chtzvt/ctsnarf/internal/cluster"
	"github.com/chtzvt/ctsnarf/internal/job"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
)

func setupTestEtcd(t *testing.T) (*embed.Etcd, cluster.Cluster) {
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

	// Small dial timeout for tests
	c, err := cluster.NewEtcdCluster(cluster.EtcdConfig{
		Endpoints:   []string{e.Clients[0].Addr().String()},
		DialTimeout: 2 * time.Second,
		Prefix:      "/ctsnarf_test",
	})
	require.NoError(t, err)
	return e, c
}

func TestJobLifecycle(t *testing.T) {
	etcd, cl := setupTestEtcd(t)
	defer etcd.Close()
	defer cl.Close()

	ctx := context.Background()
	spec := &job.JobSpec{
		Version: "0.1.0",
		LogURI:  "https://ct.googleapis.com/aviator",
		Options: job.JobOptions{ /* ...minimal fields... */ },
	}
	jobID, err := cl.SubmitJob(ctx, spec)
	require.NoError(t, err)
	require.NotEmpty(t, jobID)

	// Can retrieve same job
	got, err := cl.GetJob(ctx, jobID)
	require.NoError(t, err)
	require.Equal(t, spec.Version, got.Version)
}

func TestWorkerLifecycle(t *testing.T) {
	etcd, cl := setupTestEtcd(t)
	defer etcd.Close()
	defer cl.Close()

	ctx := context.Background()
	worker := cluster.WorkerInfo{Host: "testhost"}
	workerID, err := cl.RegisterWorker(ctx, worker)
	require.NoError(t, err)
	require.NotEmpty(t, workerID)

	workers, err := cl.ListWorkers(ctx)
	require.NoError(t, err)
	require.Len(t, workers, 1)

	// Heartbeat works
	err = cl.HeartbeatWorker(ctx, workerID)
	require.NoError(t, err)
}
