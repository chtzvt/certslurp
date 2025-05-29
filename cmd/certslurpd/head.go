package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"strings"
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cl, err := newCluster(cfg)
	if err != nil {
		return fmt.Errorf("failed to connect to etcd: %w", err)
	}
	defer cl.Close()

	logger := log.New(os.Stdout, "[api] ", log.LstdFlags)
	apiServer := api.NewServer(cl, cfg.Api, logger)

	if cfg.Secrets.ClusterKey == "" {
		return fmt.Errorf("cluster_key is required in the secrets configuration when starting in head node mode")
	}

	err = registerHead(ctx, cl, cfg, logger)
	if err != nil {
		return err
	}

	go headMonitorLoop(ctx, cl, 30*time.Second, logger)

	logger.Printf("Starting API server on %s", cfg.Api.ListenAddr)
	return apiServer.Start(ctx)
}

func approveSelf(cl cluster.Cluster, ctx context.Context, clusterKeyB64 string) error {
	var clusterKey [32]byte

	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(clusterKeyB64)))
	if err != nil {
		return err
	}

	if len(raw) != 32 {
		return fmt.Errorf("invalid cluster key length: got %d, want 32", len(raw))
	}

	copy(clusterKey[:], raw)
	cl.Secrets().SetClusterKey(clusterKey)

	return cl.Secrets().ApproveNode(ctx, cl.Secrets().NodeId())
}

func registerHead(ctx context.Context, cl cluster.Cluster, cfg *config.ClusterConfig, logger *log.Logger) error {
	registrationDone := make(chan struct{})
	registrationFailed := make(chan error, 2)

	go func() {
		if err := cl.Secrets().RegisterAndWaitForClusterKey(context.Background()); err != nil {
			registrationFailed <- err
			return
		}
		logger.Println("Head node registration complete.")
		close(registrationDone)
	}()

	approved := false
	for i := 0; i < 10; i++ {
		select {
		case err := <-registrationFailed:
			return fmt.Errorf("Head node registration failed: %v", err)
		case <-registrationDone:
			approved = true
			break
		default:
			pending, err := cl.Secrets().ListPendingRegistrations(ctx)
			if err != nil {
				logger.Printf("Could not query pending registrations: %v", err)
				break
			}
			for _, reg := range pending {
				if reg.NodeID == cl.Secrets().NodeId() {
					if err := approveSelf(cl, ctx, cfg.Secrets.ClusterKey); err != nil {
						logger.Fatalf("head node bootstrap failed: %v", err)
					}
					logger.Println("head node bootstrapped successfully")
					approved = true
					goto done
				}
			}
		}
		if approved {
			goto done
		}
		time.Sleep(300 * time.Millisecond)
	}
done:

	if !approved {
		return fmt.Errorf("Head node failed to register after 3s")
	}

	return nil
}

func isShardEffectivelyDone(shard cluster.ShardAssignmentStatus) bool {
	// A shard is considered "done" if:
	//   - It's marked Done,
	//   - Or, it's permanently failed (Failed && Retries > MaxShardRetries)
	return shard.Done || (shard.Failed && shard.Retries > cluster.MaxShardRetries)
}

func headMonitorLoop(ctx context.Context, cl cluster.Cluster, pollInterval time.Duration, logger *log.Logger) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			jobs, err := cl.ListJobs(ctx)
			if err != nil {
				logger.Printf("Error listing jobs: %v", err)
				continue
			}

			for _, job := range jobs {
				if job.Status == cluster.JobStateCompleted || job.Status == cluster.JobStateCancelled {
					continue
				}

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
					if shard.Failed && shard.Retries > cluster.MaxShardRetries {
						hasPermanentFailure = true
					}
				}

				if allDone {
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
