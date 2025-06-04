package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chtzvt/certslurp/cmd/certslurpd/config"
	"github.com/chtzvt/certslurp/internal/cluster"
)

func newCluster(cfg *config.ClusterConfig) (cluster.Cluster, error) {
	hostname, _ := os.Hostname()
	if cfg.Node.ID == "" {
		cfg.Node.ID = hostname
	}

	// Create a unique temporary path, but remove the file so the secrets package
	// can create and initialize it later.
	keychainFile := cfg.Secrets.KeychainFile
	if keychainFile == "" {
		tmpFile, err := os.CreateTemp("", "certslurpd-keychain-*.bin")
		if err != nil {
			return nil, fmt.Errorf("unable to create temporary keychain file: %w", err)
		}
		keychainFile = tmpFile.Name()
		tmpFile.Close()
		// Remove the empty file so the secrets package can safely create it
		os.Remove(keychainFile)
	}

	var etcdPrefix string
	if cfg.Etcd.Prefix == "" {
		etcdPrefix = "/certslurp"
	} else {
		etcdPrefix = cfg.Etcd.Prefix
	}

	etcdCfg := cluster.EtcdConfig{
		Endpoints:    cfg.Etcd.Endpoints,
		Username:     cfg.Etcd.Username,
		Password:     cfg.Etcd.Password,
		Prefix:       etcdPrefix,
		DialTimeout:  5 * time.Second,
		KeychainFile: keychainFile,
	}

	cl, err := cluster.NewEtcdCluster(etcdCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}

	return cl, nil
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

func jitterDuration() time.Duration {
	min := 100 * time.Millisecond
	max := 2 * time.Second

	return min + time.Duration(rand.Int63n(int64(max-min)))
}

func maybeSleep() {
	if rand.Float64() < 0.05 { // 5% of the time
		time.Sleep(jitterDuration())
	}
}
