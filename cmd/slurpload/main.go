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
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

func main() {

	rootCmd := &cobra.Command{
		Use:   "slurpload",
		Short: "certslurp ingester for PostgreSQL",
	}

	rootCmd.PersistentFlags().String("config", "", "Path to config file")
	viper.BindPFlag("config", rootCmd.PersistentFlags().Lookup("config"))
	rootCmd.MarkPersistentFlagRequired("config")

	rootCmd.PersistentFlags().Int("max-db-conns", 8, "Number of concurrent DB workers")
	viper.BindPFlag("database.max_conns", rootCmd.PersistentFlags().Lookup("max-db-conns"))

	rootCmd.PersistentFlags().Int("batch-size", 100, "Number of records to insert per transaction/batch")
	viper.BindPFlag("database.batch_size", rootCmd.PersistentFlags().Lookup("batch-size"))

	rootCmd.PersistentFlags().Int64("logstat", 1000, "Emit stats every N records processed (0 disables)")
	viper.BindPFlag("metrics.log_stat_every", rootCmd.PersistentFlags().Lookup("logstat"))

	var cfg *SlurploadConfig

	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		loadedConfig, err := loadConfig(viper.GetString("config"))
		if err != nil {
			return err
		}
		cfg = loadedConfig
		return nil
	}

	// ----- init-db command -----
	initCmd := &cobra.Command{
		Use:   "init-db",
		Short: "Initialize database schema",
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := openDatabase(cfg)
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
			db, err := openDatabase(cfg)
			if err != nil {
				return err
			}
			defer db.Close()

			reader, err := getReader(archivePath, useGzip, useBzip2)
			if err != nil {
				return err
			}
			ctx := context.Background()
			jobs := make(chan InsertJob, cfg.Database.BatchSize*cfg.Database.MaxConns)
			var wg sync.WaitGroup

			metrics := NewSlurploadMetrics()
			metrics.Start()

			watcherCfg := NewWatcherConfig("", "", []string{}, 0*time.Second)

			for i := 0; i < cfg.Database.MaxConns; i++ {
				wg.Add(1)
				go fileWorker(ctx, db, jobs, cfg.Database.BatchSize, &wg, cfg.Metrics.LogStatEvery, metrics, "", watcherCfg)
			}

			go RunFlusher(ctx, db, cfg, metrics)

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
			log.Printf("Done. %s", metrics)
			return nil
		},
	}
	loadCmd.Flags().StringVar(&archivePath, "archive", "", "Input archive file (or '-' for stdin)")
	loadCmd.Flags().BoolVar(&useGzip, "gzip", false, "Decompress gzip input")
	loadCmd.Flags().BoolVar(&useBzip2, "bzip2", false, "Decompress bzip2 input")
	loadCmd.MarkFlagRequired("archive")

	// ----- serve command -----
	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Run HTTP upload server, inbox watcher, or both (continuous ingestion mode)",
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := openDatabase(cfg)
			if err != nil {
				return err
			}
			defer db.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			jobs := make(chan InsertJob, 32*cfg.Database.MaxConns)
			var wg sync.WaitGroup

			metrics := NewSlurploadMetrics()
			metrics.Start()

			patterns := strings.Split(cfg.Processing.InboxPatterns, ",")
			watcherCfg := NewWatcherConfig(cfg.Processing.InboxDir, cfg.Processing.DoneDir, patterns, cfg.Processing.InboxPollInterval)

			// Start workers
			for i := 0; i < cfg.Database.MaxConns; i++ {
				wg.Add(1)
				go fileWorker(ctx, db, jobs, cfg.Database.BatchSize, &wg, cfg.Metrics.LogStatEvery, metrics, cfg.Processing.DoneDir, watcherCfg)
			}

			go RunFlusher(ctx, db, cfg, metrics)

			stop := make(chan struct{})

			if cfg.Processing.EnableWatcher && cfg.Processing.InboxDir != "" {
				go StartInboxWatcher(watcherCfg, jobs, stop)
				log.Printf("Inbox watcher started on %s", cfg.Processing.InboxDir)
			}

			if cfg.Server.ListenAddr != "" && cfg.Processing.InboxDir != "" {
				go StartHTTPServer(ctx, cfg, metrics)
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
			FlushIfNeeded(db, cfg, metrics)
			log.Printf("Done. %s", metrics)
			return nil
		},
	}

	serveCmd.Flags().String("http", "", "Serve HTTP upload endpoint at this address")
	viper.BindPFlag("server.listen_addr", serveCmd.Flags().Lookup("http"))

	serveCmd.Flags().String("inbox", "", "Inbox directory to watch for uploads")
	viper.BindPFlag("processing.inbox_dir", serveCmd.Flags().Lookup("inbox"))

	serveCmd.Flags().String("done", "", "Directory to move processed files to")
	viper.BindPFlag("processing.done_dir", serveCmd.Flags().Lookup("done"))

	serveCmd.Flags().Duration("poll", 2*time.Second, "Inbox watcher poll interval")
	viper.BindPFlag("processing.inbox_poll", serveCmd.Flags().Lookup("poll"))

	serveCmd.Flags().String("patterns", "*.jsonl,*.jsonl.gz,*.jsonl.bz2", "Inbox file patterns")
	viper.BindPFlag("processing.inbox_patterns", serveCmd.Flags().Lookup("patterns"))

	serveCmd.Flags().Bool("watch-inbox", true, "Enable inbox directory watcher")
	viper.BindPFlag("processing.enable_watcher", serveCmd.Flags().Lookup("watch-inbox"))

	configCmd := &cobra.Command{
		Use:   "config",
		Short: "Print effective configuration",
		Run: func(cmd *cobra.Command, args []string) {
			loadedConfig, err := loadConfig(viper.GetString("config"))
			if err != nil {
				log.Fatalf("Config error: %v", err)
			}
			b, _ := yaml.Marshal(loadedConfig)
			fmt.Println(string(b))
		},
	}
	rootCmd.AddCommand(configCmd)

	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(loadCmd)
	rootCmd.AddCommand(serveCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("slurpload error: %v", err)
	}
}
