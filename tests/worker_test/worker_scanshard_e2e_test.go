package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chtzvt/ctsnarf/internal/job"
	"github.com/chtzvt/ctsnarf/internal/testcluster"
	"github.com/chtzvt/ctsnarf/internal/testutil"
	"github.com/chtzvt/ctsnarf/internal/testworkers"
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

func TestWorkerE2E_ExtractsExpectedSubjects(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	ts := testutil.NewStubCTLogServer(t, testutil.CTLogFourEntrySTH, testutil.CTLogFourEntries)
	defer ts.Close()

	outputDir := t.TempDir() // or custom temp dir

	opts := job.JobOptions{
		Fetch: job.FetchConfig{
			BatchSize:  2,
			Workers:    1,
			IndexStart: 0,
			IndexEnd:   4,
		},
		Match: job.MatchConfig{
			SubjectRegex: "mail.google.com",
		},
		Output: job.OutputOptions{
			Extractor:   "cert_fields",
			Transformer: "jsonl",
			Sink:        "disk",
			SinkOptions: map[string]interface{}{"path": outputDir},
		},
	}

	jobID := testcluster.SubmitTestJob(t, cl, ts.URL, 1, opts)
	logger := testutil.NewTestLogger(true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	workers := testworkers.RunWorkers(ctx, t, cl, jobID, 1, logger)

	testutil.WaitFor(t, func() bool {
		return testcluster.AllShardsDone(t, cl, jobID)
	}, 5*time.Second, 100*time.Millisecond, "job should complete")

	for _, w := range workers {
		w.Stop()
	}

	// Find files in outputDir, parse results, and assert the output
	files, err := os.ReadDir(outputDir)
	require.NoError(t, err)
	require.NotEmpty(t, files)

	var found bool
	for _, f := range files {
		data, err := os.ReadFile(filepath.Join(outputDir, f.Name()))
		fmt.Printf(">>> CONTENT: %s", data)
		require.NoError(t, err)
		lines := bytes.Split(data, []byte("\n"))
		for _, line := range lines {
			if len(line) == 0 {
				continue
			}
			var rec map[string]interface{}
			require.NoError(t, json.Unmarshal(line, &rec))
			// e.g., check for a particular CN
			if subj, ok := rec["subject"].(map[string]interface{}); ok {
				if cn, ok := subj["CommonName"].(string); ok && cn == "mail.google.com" {
					found = true
				}
			}
		}
	}
	require.True(t, found, "Expected to find subject CN mail.google.com in ETL output")
}
