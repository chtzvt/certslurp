package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/chtzvt/ctsnarf/internal/job"
	"github.com/chtzvt/ctsnarf/internal/testcluster"
	"github.com/chtzvt/ctsnarf/internal/testutil"
	"github.com/chtzvt/ctsnarf/internal/worker"
)

func TestWorker_ScanShard_StubbedCTLog(t *testing.T) {
	ts := testutil.NewStubCTLogServer(t, testutil.CTLogFourEntrySTH, testutil.CTLogFourEntries)
	defer ts.Close()

	// Build JobSpec for your worker to target this CT log.
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
			Output: job.OutputOptions{},
		},
	}

	cluster, cleanup := testcluster.SetupEtcdCluster(t)
	defer cleanup()

	w := worker.NewWorker(cluster, "testjob", "worker-1", nil)

	ctx := context.Background()
	results, err := w.ScanShard(ctx, spec, int64(0), int64(4))
	if err != nil {
		t.Fatalf("ScanShard: %v", err)
	}

	// Should find the mail.google.com cert (see FourEntries above).
	if len(results) == 0 {
		t.Fatalf("Expected at least one result")
	}
	found := false
	for _, entry := range results {
		parsed, _ := entry.ToLogEntry()
		if parsed.X509Cert != nil && parsed.X509Cert.Subject.CommonName == "mail.google.com" {
			found = true
		}
	}
	if !found {
		t.Error("Expected to find mail.google.com cert, did not")
	}

	fname, err := w.WriteOutput(results, 0)
	if err != nil {
		t.Fatalf("WriteOutput: %v", err)
	}

	data, _ := os.ReadFile(fname)
	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var out map[string]interface{}
		_ = json.Unmarshal(line, &out)
		if subj, ok := out["subject"].(map[string]interface{}); ok {
			if cn, ok := subj["CommonName"].(string); ok && cn == "mail.google.com" {
				return // success!
			}
		}
	}
	t.Error("No JSONL output found with CN mail.google.com")
}
