package cluster_test

import (
	"context"
	"testing"

	"github.com/chtzvt/ctsnarf/internal/job"
	"github.com/stretchr/testify/require"
)

func TestJobLifecycle(t *testing.T) {
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

	// Can retrieve same job
	got, err := cl.GetJob(ctx, jobID)
	require.NoError(t, err)
	require.Equal(t, spec.Version, got.Version)

	jobs, err := cl.ListJobs(ctx)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, jobID, jobs[0].ID)
}
