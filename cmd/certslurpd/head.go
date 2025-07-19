package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/chtzvt/certslurp/cmd/certslurpd/config"
	"github.com/chtzvt/certslurp/internal/api"
	"github.com/chtzvt/certslurp/internal/cluster"
	"github.com/spf13/cobra"
)

var headCmd = &cobra.Command{
	Use:   "head",
	Short: "Run as cluster head node (API, metrics, management)",
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.LoadConfig(cfgFile)
		if err != nil {
			return fmt.Errorf("config error: %w", err)
		}
		return runHead(cfg)
	},
}

func runHead(cfg *config.ClusterConfig) error {
	ctx := cmdContext()

	cl, err := newCluster(cfg)
	if err != nil {
		return fmt.Errorf("boot failure: %w", err)
	}
	defer cl.Close()

	logger := log.New(os.Stdout, "[api] ", log.LstdFlags)
	apiServer := api.NewServer(cl, cfg.Api, logger)

	if cfg.Secrets.ClusterKey == "" {
		return fmt.Errorf("cluster_key is required in the secrets configuration when starting in head node mode")
	}

	err = selfBootstrap(ctx, cl, cfg, logger)
	if err != nil {
		return err
	}

	go headMonitorLoop(ctx, cl, 30*time.Second, logger)

	logger.Printf("Starting API server on %s", cfg.Api.ListenAddr)
	return apiServer.Start(ctx)
}

func isShardEffectivelyDone(shard cluster.ShardAssignmentStatus) bool {
	// A shard is considered "done" if:
	//   - It's marked Done,
	//   - Or, it's permanently failed
	return shard.Done || shard.Failed
}

func headMonitorLoop(ctx context.Context, cl cluster.Cluster, pollInterval time.Duration, logger *log.Logger) {
	basePoll := jitterDuration() + pollInterval

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(basePoll + jitterDuration()):
			maybeSleep()
			jobs, err := cl.ListJobs(ctx)
			if err != nil {
				logger.Printf("Error listing jobs: %v", err)
				continue
			}

			for _, job := range jobs {
				if job.Status == cluster.JobStateCompleted || job.Status == cluster.JobStateCancelled {
					continue
				}

				maybeSleep()
				shardMap, err := cl.GetShardAssignments(ctx, job.ID)
				if err != nil {
					logger.Printf("Error getting shards for job %s: %v", job.ID, err)
					continue
				}

				if len(shardMap) == 0 {
					continue
				}

				allDone := true
				hasPermanentFailure := false

				for _, shard := range shardMap {
					if !isShardEffectivelyDone(shard) {
						allDone = false
					}
					if !shard.Failed && shard.Retries >= cluster.MaxShardRetries {
						hasPermanentFailure = true
					}
				}

				if allDone {
					maybeSleep()
					if err := cl.MarkJobCompleted(ctx, job.ID); err != nil {
						logger.Printf("Failed to mark job %s completed: %v", job.ID, err)
					} else {
						logger.Printf("Job %s completed!", job.ID)
					}
				} else if hasPermanentFailure {
					logger.Printf("Job %s has at least one permanently failed shard; marking failed", job.ID)
					if err := cl.UpdateJobStatus(ctx, job.ID, cluster.JobStateFailed); err != nil {
						logger.Printf("Failed to mark job %s failed: %v", job.ID, err)
					}
				}
			}
		}
	}
}
