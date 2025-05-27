package worker_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/chtzvt/ctsnarf/internal/job"
	"github.com/chtzvt/ctsnarf/internal/testcluster"
	"github.com/chtzvt/ctsnarf/internal/testutil"
	"github.com/chtzvt/ctsnarf/internal/testworkers"
	"github.com/chtzvt/ctsnarf/internal/worker"
	ct "github.com/google/certificate-transparency-go"
	"github.com/google/certificate-transparency-go/x509"
	"github.com/stretchr/testify/require"
)

func TestWorker_StreamShard_StubbedCTLog(t *testing.T) {
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
			// Output not needed for StreamShard-only test
		},
	}

	cluster, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()

	w := worker.NewWorker(cluster, "worker-1", nil)

	ctx := context.Background()
	entriesCh := make(chan *ct.RawLogEntry, 10)
	var results []*ct.RawLogEntry

	// Drain channel in a goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for entry := range entriesCh {
			results = append(results, entry)
		}
	}()

	// StreamShard will close entriesCh when done
	err := w.StreamShard(ctx, spec, int64(0), int64(4), entriesCh)
	require.NoError(t, err)
	wg.Wait()

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

func TestWorkerE2E_ExtractsExpectedCerts(t *testing.T) {
	cl, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()
	ts := testutil.NewStubCTLogServer(t, testutil.CTLogFourEntrySTH, testutil.CTLogFourEntries)
	defer ts.Close()

	outputDir := t.TempDir()

	opts := job.JobOptions{
		Fetch: job.FetchConfig{
			BatchSize:  2,
			Workers:    1,
			IndexStart: 0,
			IndexEnd:   1, // Get first cert (testutil.CTLogEntry0)
		},
		Match: job.MatchConfig{
			SubjectRegex: "mail.google.com",
		},
		Output: job.OutputOptions{
			Extractor:   "raw",
			Transformer: "passthrough",
			Sink:        "disk", // Writes raw ASN.1 data to disk from the extracted ct.RawLogEntry
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
		require.NoError(t, err)

		cert, err := x509.ParseCertificates(data)
		require.NoError(t, err)

		if cert[0].DNSNames[0] == "mail.google.com" {
			found = true
		}
	}

	require.True(t, found, "Expected to find subject CN mail.google.com in ETL output")
}
