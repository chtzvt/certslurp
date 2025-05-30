package main

import (
	"fmt"
	"log"
	"os"

	"github.com/chtzvt/certslurp/cmd/certslurpd/config"
	"github.com/chtzvt/certslurp/internal/worker"
	"github.com/spf13/cobra"
)

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Run as worker node (shard processor)",
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.LoadConfig(cfgFile)
		if err != nil {
			return fmt.Errorf("config error: %w", err)
		}
		return runWorker(cfg)
	},
}

// Worker node: connect to etcd, launch worker main loop
func runWorker(cfg *config.ClusterConfig) error {
	ctx := cmdContext()

	fmt.Printf("Starting worker node: %s\n", cfg.Node.ID)
	cl, err := newCluster(cfg)
	if err != nil {
		return fmt.Errorf("boot failure: %w", err)
	}
	defer cl.Close()

	logger := log.New(os.Stdout, "[worker] ", log.LstdFlags)

	if cfg.Secrets.ClusterKey != "" {
		err = selfBootstrap(ctx, cl, cfg, logger)
		if err != nil {
			return err
		}
	} else {
		logger.Println("Registering worker and waiting for admin to approve secrets...")
		cl.Secrets().RegisterAndWaitForClusterKey(ctx)
		logger.Println("Registration complete. Starting...")
	}

	w := worker.NewWorker(cl, cfg.Node.ID, logger)
	return w.Run(cmdContext())
}
