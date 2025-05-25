package cluster_test

import (
	"testing"
	"time"

	"github.com/chtzvt/ctsnarf/internal/cluster"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
)

func setupEtcdCluster(t *testing.T) (cluster.Cluster, func()) {
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.Logger = "zap"
	cfg.LogLevel = "error"
	e, err := embed.StartEtcd(cfg)
	require.NoError(t, err)

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		t.Fatal("etcd not ready")
	}

	cl, err := cluster.NewEtcdCluster(cluster.EtcdConfig{
		Endpoints:   []string{e.Clients[0].Addr().String()},
		DialTimeout: 2 * time.Second,
		Prefix:      "/ctsnarf_test",
	})
	require.NoError(t, err)
	cleanup := func() {
		cl.Close()
		e.Close()
	}
	return cl, cleanup
}
