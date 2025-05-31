package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/chtzvt/certslurp/internal/extractor"
	"github.com/dsnet/compress/bzip2"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

/*
To run the integration tests, you’ll need a running PostgreSQL instance.
The easiest way is to use Docker to spin up a disposable container:

    docker run --rm \
      -e POSTGRES_USER=slurper \
      -e POSTGRES_PASSWORD=slurper \
      -e POSTGRES_DB=slurper_test \
      -p 5433:5432 \
      --name slurper-test-postgres \
      postgres:latest

This starts a PostgreSQL server on localhost:5433 with the username `slurper`, password `slurper`, and database `slurper_test`.
You can stop it any time with:

    docker stop slurper-test-postgres

In a separate shell, set the following environment variable so your Go tests connect to this instance:

    export TEST_DATABASE_DSN="host=localhost port=5433 user=slurper password=slurper dbname=slurper_test sslmode=disable"

Now just run your tests as usual:

    go test ./...

Note: Adjust the port if 5433 is taken, but make sure it matches in your DSN.
*/

func setupTestDB(t *testing.T) *sql.DB {
	dsn := os.Getenv("TEST_DATABASE_DSN")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_DSN not set")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	require.NoError(t, runInitDB(db))

	fqdnCache, err = initFQDNLRUCache(1000)
	require.NoError(t, err)

	return db
}

func teardownTestDB(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`TRUNCATE root_domains, subdomains, certificates, subdomain_certificates RESTART IDENTITY CASCADE`)
	require.NoError(t, err)
	db.Close()
	fqdnCache = nil
}

func compressGzip(data []byte) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, _ = w.Write(data)
	w.Close()
	return buf.Bytes()
}

func compressBzip2(data []byte) []byte {
	var buf bytes.Buffer
	w, _ := bzip2.NewWriter(&buf, &bzip2.WriterConfig{Level: bzip2.BestCompression})
	_, _ = w.Write(data)
	w.Close()
	return buf.Bytes()
}

func writeTestFile(t *testing.T, dir, ext, data string) string {
	path := filepath.Join(dir, "test"+ext)
	switch ext {
	case ".jsonl":
		require.NoError(t, os.WriteFile(path, []byte(data), 0644))
	case ".jsonl.gz":
		f, err := os.Create(path)
		require.NoError(t, err)
		gz := gzip.NewWriter(f)
		_, err = gz.Write([]byte(data))
		require.NoError(t, err)
		require.NoError(t, gz.Close())
		require.NoError(t, f.Close())
	case ".jsonl.bz2":
		f, err := os.Create(path)
		require.NoError(t, err)
		bz, err := bzip2.NewWriter(f, nil)
		require.NoError(t, err)
		_, err = bz.Write([]byte(data))
		require.NoError(t, err)
		require.NoError(t, bz.Close())
		require.NoError(t, f.Close())
	}
	return path
}

func TestInsertBatch(t *testing.T) {
	db := setupTestDB(t)
	defer teardownTestDB(t, db)

	cert := extractor.CertFieldsExtractorOutput{
		CommonName:         "www.example.com",
		DNSNames:           []string{"www.example.com"},
		OrganizationalUnit: []string{"IT"},
		Organization:       []string{"ExampleCorp"},
		Locality:           []string{"Mountain View"},
		Country:            []string{"US"},
		NotBefore:          time.Now().Add(-1 * time.Hour),
		NotAfter:           time.Now().Add(365 * 24 * time.Hour),
		Subject:            "CN=www.example.com,O=ExampleCorp",
		LogIndex:           42,
	}

	metrics := NewSlurploadMetrics()
	metrics.Start()

	err := insertBatch(
		context.Background(), db,
		[]extractor.CertFieldsExtractorOutput{cert},
		0, metrics)
	require.NoError(t, err)

	// Query for inserted certificate
	var cn string
	err = db.QueryRow(`SELECT cn FROM certificates WHERE cn = $1`, "www.example.com").Scan(&cn)
	require.NoError(t, err)
	require.Equal(t, "www.example.com", cn)
}

func TestProcessFileJob_Plain_Gz_Bz2(t *testing.T) {
	dir := t.TempDir()
	for _, ext := range []string{".jsonl", ".jsonl.gz", ".jsonl.bz2"} {
		t.Run(ext, func(t *testing.T) {
			db := setupTestDB(t)
			defer teardownTestDB(t, db)
			path := writeTestFile(t, dir, ext, testData)
			metrics := NewSlurploadMetrics()
			metrics.Start()
			job := InsertJob{Name: filepath.Base(path), Path: path}
			err := processFileJob(context.Background(), db, job, 10, 0, metrics)
			require.NoError(t, err)
			var count int
			require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM certificates`).Scan(&count))
			require.Equal(t, 1, count)
		})
	}
}

const testData string = `{"cn":"www.example.com","dns":["www.example.com"],"ou":["IT"],"o":["ExampleCorp"],"l":["Mountain View"],"c":["US"],"sub":"CN=www.example.com,O=ExampleCorp","nbf":"2023-01-01T00:00:00Z","naf":"2024-01-01T00:00:00Z","en":1}`

func TestHTTPEndpoint(t *testing.T) {
	db := setupTestDB(t)
	defer teardownTestDB(t, db)

	inboxDir := t.TempDir()
	stop := make(chan struct{})
	jobs := make(chan InsertJob, 2)

	cfg := NewWatcherConfig(inboxDir, "", []string{"*.jsonl"}, 50*time.Millisecond)
	go StartInboxWatcher(cfg, jobs, stop)

	srv := httptest.NewUnstartedServer(uploadHandler(inboxDir))
	srv.Start()
	defer srv.Close()

	// POST test data (plain JSONL)
	resp, err := http.Post(srv.URL+"/upload", "application/json", bytes.NewReader([]byte(testData)))
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	// Wait for watcher to see and enqueue the job
	var job InsertJob
	select {
	case job = <-jobs:
	case <-time.After(3 * time.Second):
		close(stop)
		t.Fatal("timed out waiting for watcher to enqueue job")
	}
	close(stop) // stop watcher

	// File should exist
	_, err = os.Stat(job.Path)
	require.NoError(t, err)

	// Process the file
	metrics := NewSlurploadMetrics()
	metrics.Start()
	err = processFileJob(context.Background(), db, job, 10, 0, metrics)
	require.NoError(t, err)

	// Assert DB content
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM certificates`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestUploadHandler_Methods(t *testing.T) {
	inboxDir := t.TempDir()
	handler := uploadHandler(inboxDir)

	cases := []struct {
		method     string
		wantStatus int
	}{
		{"POST", http.StatusNoContent},
		{"PUT", http.StatusNoContent},
		{"GET", http.StatusMethodNotAllowed},
		{"DELETE", http.StatusMethodNotAllowed},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(tc.method, "/upload", bytes.NewReader([]byte(testData)))
		w := httptest.NewRecorder()
		handler(w, req)
		resp := w.Result()
		require.Equal(t, tc.wantStatus, resp.StatusCode)
	}
}

func TestInboxWatcher_Workers_E2E(t *testing.T) {
	db := setupTestDB(t)
	defer teardownTestDB(t, db)

	inboxDir := t.TempDir()
	writeTestFile(t, inboxDir, ".jsonl", testData+"\n")

	jobs := make(chan InsertJob, 2)
	stop := make(chan struct{})

	cfg := NewWatcherConfig(inboxDir, "", []string{"*.jsonl"}, 100*time.Millisecond)

	// Start watcher
	go StartInboxWatcher(cfg, jobs, stop)

	metrics := NewSlurploadMetrics()
	metrics.Start()

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				_ = processFileJob(context.Background(), db, job, 10, 0, metrics)
			}
		}()
	}

	// Shutdown
	time.Sleep(200 * time.Millisecond) // let watcher find files
	close(stop)                        // stop watcher
	time.Sleep(100 * time.Millisecond) // let jobs channel fill
	close(jobs)                        // let workers drain jobs
	wg.Wait()

	// Assert DB content
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM certificates`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestHTTPEndpoint_Compressed(t *testing.T) {
	inboxDir := t.TempDir()
	stop := make(chan struct{})
	jobs := make(chan InsertJob, 2)
	cfg := NewWatcherConfig(inboxDir, "", []string{"*.jsonl", "*.jsonl.gz", "*.jsonl.bz2"}, 50*time.Millisecond)
	go StartInboxWatcher(cfg, jobs, stop)

	srv := httptest.NewUnstartedServer(uploadHandler(inboxDir))
	srv.Start()
	defer srv.Close()

	type testCase struct {
		name     string
		content  []byte
		ct       string
		encoding string
		ext      string
	}
	tests := []testCase{
		{
			name:     "plain",
			content:  []byte(testData),
			ct:       "application/json",
			encoding: "",
			ext:      ".jsonl",
		},
		{
			name:     "gzip",
			content:  compressGzip([]byte(testData)),
			ct:       "application/json",
			encoding: "gzip",
			ext:      ".jsonl.gz",
		},
		{
			name:     "bzip2",
			content:  compressBzip2([]byte(testData)),
			ct:       "application/json",
			encoding: "bzip2",
			ext:      ".jsonl.bz2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := setupTestDB(t)
			defer teardownTestDB(t, db)

			req, err := http.NewRequest("POST", srv.URL+"/upload", bytes.NewReader(tc.content))
			require.NoError(t, err)
			req.Header.Set("Content-Type", tc.ct)
			if tc.encoding != "" {
				req.Header.Set("Content-Encoding", tc.encoding)
			}
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusNoContent, resp.StatusCode)

			var job InsertJob
			select {
			case job = <-jobs:
			case <-time.After(3 * time.Second):
				close(stop)
				t.Fatalf("timed out waiting for watcher to enqueue job for %s", tc.name)
			}

			metrics := NewSlurploadMetrics()
			metrics.Start()

			err = processFileJob(context.Background(), db, job, 10, 0, metrics)
			require.NoError(t, err)

			var count int
			require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM certificates`).Scan(&count))
			require.Equal(t, 1, count)
		})
	}

	close(stop)
}

func TestWatcherMovesToDoneDir(t *testing.T) {
	db := setupTestDB(t)
	defer teardownTestDB(t, db)

	inboxDir := t.TempDir()
	doneDir := t.TempDir()

	// Place a file in the inbox
	_ = writeTestFile(t, inboxDir, ".jsonl", testData+"\n")

	jobs := make(chan InsertJob, 1)
	stop := make(chan struct{})

	cfg := NewWatcherConfig(inboxDir, doneDir, []string{"*.jsonl"}, 200*time.Millisecond)

	go StartInboxWatcher(cfg, jobs, stop)

	// Get the job
	var job InsertJob
	select {
	case job = <-jobs:
	case <-time.After(2 * time.Second):
		close(stop)
		t.Fatal("timed out waiting for watcher")
	}
	close(stop)

	// Run the worker
	metrics := NewSlurploadMetrics()
	metrics.Start()
	err := processFileJob(context.Background(), db, job, 10, 0, metrics)
	require.NoError(t, err)

	// Move file (simulate worker cleanup)
	dest := filepath.Join(doneDir, filepath.Base(job.Path))
	err = os.Rename(job.Path, dest)
	require.NoError(t, err)

	// File should not exist in inbox, but must exist in doneDir
	_, err = os.Stat(job.Path)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(dest)
	require.NoError(t, err)
}

func TestLoadConfig_YAML(t *testing.T) {
	// Create a temp YAML file
	yamlContent := `
database:
  host: "localhost"
  port: 5432
  username: "test"
  password: "s3cr3t"
  database: "certs"
  ssl_mode: "disable"
  max_conns: 10
  batch_size: 50
  cache_size: 12345
server:
  listen_addr: ":8081"
processing:
  inbox_dir: "/tmp/inbox"
  done_dir: "/tmp/done"
  inbox_patterns: "*.jsonl,*.gz"
  inbox_poll: 1s
  enable_watcher: true
metrics:
  log_stat_every: 17
`
	f, err := os.CreateTemp("", "slurpload-config-*.yaml")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	_, err = f.Write([]byte(yamlContent))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	cfg, err := loadConfig(f.Name())
	require.NoError(t, err)

	// Check all fields
	require.Equal(t, "localhost", cfg.Database.Host)
	require.Equal(t, 5432, cfg.Database.Port)
	require.Equal(t, "test", cfg.Database.Username)
	require.Equal(t, "s3cr3t", cfg.Database.Password)
	require.Equal(t, "certs", cfg.Database.DatabaseName)
	require.Equal(t, "disable", cfg.Database.SSLMode)
	require.Equal(t, 10, cfg.Database.MaxConns)
	require.Equal(t, 50, cfg.Database.BatchSize)
	require.Equal(t, 12345, cfg.Database.CacheSize)
	require.Equal(t, ":8081", cfg.Server.ListenAddr)
	require.Equal(t, "/tmp/inbox", cfg.Processing.InboxDir)
	require.Equal(t, "/tmp/done", cfg.Processing.DoneDir)
	require.Equal(t, "*.jsonl,*.gz", cfg.Processing.InboxPatterns)
	require.Equal(t, true, cfg.Processing.EnableWatcher)
	require.Equal(t, int64(17), cfg.Metrics.LogStatEvery)
	require.Equal(t, 1*time.Second, cfg.Processing.InboxPollInterval)
}

func TestLoadConfig_Validation(t *testing.T) {
	// Minimal config with missing required fields
	yamlContent := `
database:
  port: 5432
`
	f, err := os.CreateTemp("", "slurpload-config-*.yaml")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	_, err = f.Write([]byte(yamlContent))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = loadConfig(f.Name())
	require.Error(t, err)
	require.Contains(t, err.Error(), "database.host")
}

func TestMetricsHandler(t *testing.T) {
	metrics := NewSlurploadMetrics()
	metrics.Start()
	metrics.IncProcessed()
	metrics.IncFailed()

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()

	handler := metricsHandler(metrics)
	handler(w, req)

	resp := w.Result()
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	// Check content type and body
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	require.Contains(t, string(body), `"processed":1`)
	require.Contains(t, string(body), `"failed":1`)
}
