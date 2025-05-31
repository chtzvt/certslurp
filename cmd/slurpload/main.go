package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

func main() {
	var (
		configPath   string
		maxDBConns   int
		batchSize    int
		logStatEvery int64
		cacheSize    int
	)

	rootCmd := &cobra.Command{
		Use:   "slurpload",
		Short: "certslurp ingester for PostgreSQL",
	}

	rootCmd.PersistentFlags().StringVar(&configPath, "config", "", "Path to DB config JSON (required)")
	rootCmd.PersistentFlags().IntVar(&maxDBConns, "max-db-conns", 8, "Number of concurrent DB workers")
	rootCmd.PersistentFlags().IntVar(&batchSize, "batch-size", 100, "Number of records to insert per transaction/batch")
	rootCmd.PersistentFlags().IntVar(&cacheSize, "cache-size", 250_000, "FQDN cache size (default: 250,000)")
	rootCmd.PersistentFlags().Int64Var(&logStatEvery, "logstat", 1000, "Emit stats every N records processed (0 disables)")
	rootCmd.MarkPersistentFlagRequired("config")

	// ----- init-db command -----
	initCmd := &cobra.Command{
		Use:   "init-db",
		Short: "Initialize database schema",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig(configPath)
			if err != nil {
				return err
			}
			db, err := openDatabase(buildDSN(cfg), maxDBConns)
			if err != nil {
				return err
			}
			defer db.Close()
			if err := runInitDB(db); err != nil {
				return err
			}
			fmt.Println("Database schema created.")
			return nil
		},
	}

	// ----- load command -----
	var archivePath string
	var useGzip, useBzip2 bool

	loadCmd := &cobra.Command{
		Use:   "load",
		Short: "One-shot ingest of archive or file (stdin or disk)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig(configPath)
			if err != nil {
				return err
			}
			db, err := openDatabase(buildDSN(cfg), maxDBConns)
			if err != nil {
				return err
			}
			defer db.Close()

			fqdnCache, err = initFQDNLRUCache(cacheSize)
			if err != nil {
				return err
			}

			reader, err := getReader(archivePath, useGzip, useBzip2)
			if err != nil {
				return err
			}
			ctx := context.Background()
			jobs := make(chan InsertJob, batchSize*maxDBConns)
			var wg sync.WaitGroup

			var processedRecords int64 = 0
			var errorCount int64 = 0
			startTime := time.Now()

			watcherCfg := NewWatcherConfig("", "", []string{}, 0*time.Second)

			for i := 0; i < maxDBConns; i++ {
				wg.Add(1)
				go fileWorker(ctx, db, jobs, batchSize, &wg, logStatEvery, &errorCount, &processedRecords, startTime, "", watcherCfg)
			}

			// Save stdin/archive to temp file for file-based batching
			tmp, err := os.CreateTemp("", "slurpload-*.jsonl")
			if err != nil {
				return err
			}
			defer os.Remove(tmp.Name())
			_, err = bufio.NewReader(reader).WriteTo(tmp)
			if err != nil {
				return err
			}
			tmp.Close()

			jobs <- InsertJob{Name: filepath.Base(tmp.Name()), Path: tmp.Name()}
			close(jobs)
			wg.Wait()
			log.Printf("Done. %d records processed. %d errors encountered. Elapsed: %v", processedRecords, errorCount, time.Since(startTime).Truncate(time.Second))
			return nil
		},
	}
	loadCmd.Flags().StringVar(&archivePath, "archive", "", "Input archive file (or '-' for stdin)")
	loadCmd.Flags().BoolVar(&useGzip, "gzip", false, "Decompress gzip input")
	loadCmd.Flags().BoolVar(&useBzip2, "bzip2", false, "Decompress bzip2 input")
	loadCmd.MarkFlagRequired("archive")

	// ----- serve command -----
	var httpAddr string
	var inboxDir string
	var doneDir string
	var pollInterval time.Duration
	var filePatterns string
	var enableWatcher bool

	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Run HTTP upload server, inbox watcher, or both (continuous ingestion mode)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig(configPath)
			if err != nil {
				return err
			}
			db, err := openDatabase(buildDSN(cfg), maxDBConns)
			if err != nil {
				return err
			}
			defer db.Close()

			fqdnCache, err = initFQDNLRUCache(cacheSize)
			if err != nil {
				return err
			}

			var processedRecords int64 = 0
			var errorCount int64 = 0
			startTime := time.Now()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			jobs := make(chan InsertJob, 32*maxDBConns)
			var wg sync.WaitGroup

			patterns := strings.Split(filePatterns, ",")
			watcherCfg := NewWatcherConfig(inboxDir, doneDir, patterns, pollInterval)

			// Start workers
			for i := 0; i < maxDBConns; i++ {
				wg.Add(1)
				go fileWorker(ctx, db, jobs, batchSize, &wg, logStatEvery, &errorCount, &processedRecords, startTime, doneDir, watcherCfg)
			}

			stop := make(chan struct{})

			if enableWatcher && inboxDir != "" {
				go StartInboxWatcher(watcherCfg, jobs, stop)
				log.Printf("Inbox watcher started on %s", inboxDir)
			}
			if httpAddr != "" && inboxDir != "" {
				go StartHTTPServer(httpAddr, inboxDir)
				log.Printf("HTTP server started at %s, uploads go to %s", httpAddr, inboxDir)
			}

			// Graceful shutdown on SIGINT/SIGTERM
			sig := make(chan os.Signal, 1)
			signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
			select {
			case <-sig:
				log.Println("Signal received, shutting down...")
				close(stop)
			}
			close(jobs)
			wg.Wait()
			log.Printf("Done. %d records processed. %d errors encountered. Elapsed: %v", processedRecords, errorCount, time.Since(startTime).Truncate(time.Second))
			return nil
		},
	}
	serveCmd.Flags().StringVar(&httpAddr, "http", "", "Serve HTTP upload endpoint at this address")
	serveCmd.Flags().StringVar(&inboxDir, "inbox", "", "Inbox directory to watch for uploads")
	serveCmd.Flags().StringVar(&doneDir, "done", "", "Directory to move processed files to (default: delete after processing)")
	serveCmd.Flags().DurationVar(&pollInterval, "poll", 2*time.Second, "Inbox watcher poll interval")
	serveCmd.Flags().StringVar(&filePatterns, "patterns", "*.jsonl,*.jsonl.gz,*.jsonl.bz2", "Comma-separated file patterns for inbox watcher")
	serveCmd.Flags().BoolVar(&enableWatcher, "watch-inbox", true, "Enable inbox directory watcher")

	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(loadCmd)
	rootCmd.AddCommand(serveCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("slurpload error: %v", err)
	}
}
