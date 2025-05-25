package cluster_test

import (
	"context"
	"testing"

	"github.com/chtzvt/ctsnarf/internal/cluster"
	"github.com/stretchr/testify/require"
)

func TestWorkerLifecycle(t *testing.T) {
	cl, cleanup := setupEtcdCluster(t)
	defer cleanup()

	ctx := context.Background()
	worker := cluster.WorkerInfo{Host: "testhost"}
	workerID, err := cl.RegisterWorker(ctx, worker)
	require.NoError(t, err)
	require.NotEmpty(t, workerID)

	workers, err := cl.ListWorkers(ctx)
	require.NoError(t, err)
	require.Len(t, workers, 1)
	require.Equal(t, workerID, workers[0].ID)

	// Heartbeat works
	require.NoError(t, cl.HeartbeatWorker(ctx, workerID))
}
