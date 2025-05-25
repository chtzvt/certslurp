package cluster_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/chtzvt/ctsnarf/internal/cluster"
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

	// Immediately after submit
	job, err := cl.GetJob(ctx, jobID)
	require.NoError(t, err)
	require.Equal(t, spec.Version, job.Spec.Version)
	require.Equal(t, "pending", job.Status)
	require.False(t, job.Submitted.IsZero())

	// Mark started
	require.NoError(t, cl.MarkJobStarted(ctx, jobID))
	job, err = cl.GetJob(ctx, jobID)
	require.NoError(t, err)
	require.False(t, job.Started.IsZero())

	// Update status to running
	require.NoError(t, cl.UpdateJobStatus(ctx, jobID, "running"))
	job, err = cl.GetJob(ctx, jobID)
	require.NoError(t, err)
	require.Equal(t, "running", job.Status)

	// Mark completed
	require.NoError(t, cl.MarkJobCompleted(ctx, jobID))
	job, err = cl.GetJob(ctx, jobID)
	require.NoError(t, err)
	require.False(t, job.Completed.IsZero())

	// Cancel job
	require.NoError(t, cl.CancelJob(ctx, jobID))
	job, err = cl.GetJob(ctx, jobID)
	require.NoError(t, err)
	require.False(t, job.Cancelled.IsZero())
}

func TestCancelJobErrors(t *testing.T) {
	cl, cleanup := setupEtcdCluster(t)
	defer cleanup()
	ctx := context.Background()
	_, err := cl.GetJob(ctx, "doesnotexist")
	require.Error(t, err)

	err = cl.CancelJob(ctx, "doesnotexist")
	require.Error(t, err) // Or require.NoError(t, err) if you want idempotency

	// Create, cancel, cancel again
	spec := &job.JobSpec{Version: "x", LogURI: "u", Options: job.JobOptions{}}
	jobID, _ := cl.SubmitJob(ctx, spec)
	require.NoError(t, cl.CancelJob(ctx, jobID))
	require.NoError(t, cl.CancelJob(ctx, jobID)) // Should not error if idempotent
}

func TestListJobs_AllFields(t *testing.T) {
	cl, cleanup := setupEtcdCluster(t)
	defer cleanup()
	ctx := context.Background()

	// Submit first job and advance it through the full lifecycle.
	spec1 := &job.JobSpec{Version: "v1", LogURI: "log1", Options: job.JobOptions{}}
	jobID1, err := cl.SubmitJob(ctx, spec1)
	require.NoError(t, err)
	require.NoError(t, cl.MarkJobStarted(ctx, jobID1))
	require.NoError(t, cl.UpdateJobStatus(ctx, jobID1, "running"))
	require.NoError(t, cl.MarkJobCompleted(ctx, jobID1))
	require.NoError(t, cl.CancelJob(ctx, jobID1))

	// Submit a second, simpler job
	spec2 := &job.JobSpec{Version: "v2", LogURI: "log2", Options: job.JobOptions{}}
	jobID2, err := cl.SubmitJob(ctx, spec2)
	require.NoError(t, err)

	// List jobs and validate fields
	jobs, err := cl.ListJobs(ctx)
	require.NoError(t, err)
	require.Len(t, jobs, 2)

	var found1, found2 *cluster.JobInfo
	for i := range jobs {
		switch jobs[i].ID {
		case jobID1:
			found1 = &jobs[i]
		case jobID2:
			found2 = &jobs[i]
		}
	}
	require.NotNil(t, found1)
	require.NotNil(t, found2)

	require.Equal(t, spec1.Version, found1.Spec.Version)
	require.Equal(t, "running", found1.Status) // The last status set
	require.False(t, found1.Submitted.IsZero())
	require.False(t, found1.Started.IsZero())
	require.False(t, found1.Completed.IsZero())
	require.False(t, found1.Cancelled.IsZero())

	require.Equal(t, spec2.Version, found2.Spec.Version)
	require.Equal(t, "pending", found2.Status)
	require.False(t, found2.Submitted.IsZero())
	require.True(t, found2.Started.IsZero())
	require.True(t, found2.Completed.IsZero())
	require.True(t, found2.Cancelled.IsZero())
}

func TestJobInfo_JSONRoundtrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	info := cluster.JobInfo{
		ID:        "job123",
		Spec:      &job.JobSpec{Version: "v1", LogURI: "log", Options: job.JobOptions{}},
		Submitted: now,
		Started:   now.Add(10 * time.Second),
		Completed: now.Add(20 * time.Second),
		Status:    "completed",
		Cancelled: now.Add(30 * time.Second),
	}
	b, err := json.Marshal(info)
	require.NoError(t, err)

	var out cluster.JobInfo
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, info.ID, out.ID)
	require.Equal(t, info.Spec.Version, out.Spec.Version)
	require.Equal(t, info.Status, out.Status)
	require.WithinDuration(t, info.Submitted, out.Submitted, time.Millisecond)
	require.WithinDuration(t, info.Started, out.Started, time.Millisecond)
	require.WithinDuration(t, info.Completed, out.Completed, time.Millisecond)
	require.WithinDuration(t, info.Cancelled, out.Cancelled, time.Millisecond)
}
