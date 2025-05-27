package worker_test

import (
	"context"
	"testing"

	"github.com/chtzvt/ctsnarf/internal/job"
	"github.com/chtzvt/ctsnarf/internal/testcluster"
	"github.com/chtzvt/ctsnarf/internal/testutil"
	"github.com/chtzvt/ctsnarf/internal/worker"
	"github.com/stretchr/testify/require"
)

func TestWorker_ScanShard_StubbedCTLog(t *testing.T) {
	ts := testutil.NewStubCTLogServer(t, testutil.CTLogFourEntrySTH, testutil.CTLogFourEntries)
	defer ts.Close()

	spec := job.JobSpec{
		LogURI: ts.URL,
		Options: job.JobOptions{
			Fetch: job.FetchConfig{
				BatchSize:  2,
				Workers:    1,
				IndexStart: 0,
				IndexEnd:   4,
			},
			Match: job.MatchConfig{
				SubjectRegex: "mail.google.com",
			},
			// Don't need Output for ScanShard-only test
		},
	}

	cluster, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()

	w := worker.NewWorker(cluster, "testjob", "worker-1", nil)

	ctx := context.Background()
	results, err := w.ScanShard(ctx, spec, int64(0), int64(4))
	require.NoError(t, err)

	// Should find the mail.google.com cert
	found := false
	for _, entry := range results {
		parsed, _ := entry.ToLogEntry()
		if parsed.X509Cert != nil && parsed.X509Cert.Subject.CommonName == "mail.google.com" {
			found = true
		}
	}
	require.True(t, found, "Expected to find mail.google.com cert, did not")
}
