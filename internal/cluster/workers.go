package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"path"

	"github.com/google/uuid"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func (c *etcdCluster) RegisterWorker(ctx context.Context, info WorkerInfo) (string, error) {
	workerID := info.ID
	if workerID == "" {
		workerID = uuid.New().String()
		info.ID = workerID
	}
	key := path.Join(c.cfg.Prefix, "workers", workerID)
	val, _ := json.Marshal(info)

	// Grant a lease for TTL (worker heartbeat)
	lease, err := c.client.Grant(ctx, 15) // 15 seconds, tune as needed
	if err != nil {
		return "", err
	}
	_, err = c.client.Put(ctx, key, string(val), clientv3.WithLease(lease.ID))
	if err != nil {
		return "", err
	}
	return workerID, nil
}

func (c *etcdCluster) ListWorkers(ctx context.Context) ([]WorkerInfo, error) {
	prefix := path.Join(c.cfg.Prefix, "workers") + "/"
	resp, err := c.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	workers := make([]WorkerInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var info WorkerInfo
		if err := json.Unmarshal(kv.Value, &info); err == nil {
			workers = append(workers, info)
		}
	}
	return workers, nil
}

func (c *etcdCluster) HeartbeatWorker(ctx context.Context, workerID string) error {
	key := path.Join(c.cfg.Prefix, "workers", workerID)
	// Refresh TTL on worker
	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return fmt.Errorf("worker %s not found", workerID)
	}
	// Get lease ID from the worker key
	leaseID := clientv3.LeaseID(resp.Kvs[0].Lease)
	_, err = c.client.KeepAliveOnce(ctx, leaseID)
	return err
}
