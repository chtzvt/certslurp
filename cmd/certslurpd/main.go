package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chtzvt/certslurp/cmd/certslurpd/config"
	"github.com/chtzvt/certslurp/internal/api"
	"github.com/chtzvt/certslurp/internal/cluster"
	"github.com/chtzvt/certslurp/internal/worker"
	"github.com/spf13/cobra"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "certslurpd",
	Short: "certslurpd is a distributed CT log fetcher and processor (head/worker)",
}

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

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $PWD/certslurpd.yaml)")
	rootCmd.AddCommand(headCmd)
	rootCmd.AddCommand(workerCmd)
}

func newCluster(cfg *config.ClusterConfig) (cluster.Cluster, error) {
	hostname, _ := os.Hostname()
	if cfg.Node.ID == "" {
		cfg.Node.ID = hostname
	}

	etcdCfg := cluster.EtcdConfig{
		Endpoints:    cfg.Etcd.Endpoints,
		Username:     cfg.Etcd.Username,
		Password:     cfg.Etcd.Password,
		Prefix:       cfg.Etcd.Prefix,
		DialTimeout:  5 * time.Second,
		KeychainFile: cfg.Secrets.KeychainFile,
	}

	cl, err := cluster.NewEtcdCluster(etcdCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}

	return cl, nil
}

// Worker node: connect to etcd, launch worker main loop
func runWorker(cfg *config.ClusterConfig) error {
	fmt.Printf("Starting worker node: %s\n", cfg.Node.ID)
	cl, err := newCluster(cfg)
	if err != nil {
		return fmt.Errorf("failed to connect to etcd: %w", err)
	}

	defer cl.Close()

	logger := log.New(os.Stdout, "[worker] ", log.LstdFlags)
	w := worker.NewWorker(cl, cfg.Node.ID, logger)
	return w.Run(cmdContext())
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
	return apiServer.Start(ctx)
}

func cmdContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		cancel()
	}()
	return ctx
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
