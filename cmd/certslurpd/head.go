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

	go func() {
		if err := cl.Secrets().RegisterAndWaitForClusterKey(context.Background()); err != nil {
			logger.Fatalf("Head node registration failed: %v", err)
		}
		logger.Println("Head node registration complete.")
	}()

	time.Sleep(1 * time.Second)

	approved := false
	for i := 0; i < 10; i++ {
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
				break
			}
		}
		if approved {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if !approved {
		return fmt.Errorf("Head node was failed to register after 3s")
	}

	logger.Printf("Starting API server on %s", cfg.Api.ListenAddr)
	return apiServer.Start(ctx)
}

func approveSelf(cl cluster.Cluster, ctx context.Context, clusterKeyB64 string) error {
	clusterKey, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(clusterKeyB64)))
	if err != nil {
		return err
	}

	if len(clusterKey) != 32 {
		return fmt.Errorf("invalid cluster key length: got %d, want 32", len(clusterKey))
	}

	return cl.Secrets().ApproveNode(ctx, cl.Secrets().NodeId())
}
