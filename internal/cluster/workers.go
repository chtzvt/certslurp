package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"time"

	"github.com/google/uuid"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type WorkerInfo struct {
	ID       string
	Host     string
	LastSeen time.Time
}

func (c *etcdCluster) RegisterWorker(ctx context.Context, info WorkerInfo) (string, error) {
	workerID := info.ID
	if workerID == "" {
		workerID = uuid.New().String()
		info.ID = workerID
	}
	key := path.Join(c.Prefix(), "workers", workerID)
	val, _ := json.Marshal(info)

	lease, err := c.client.Grant(ctx, 15)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	txn := c.client.Txn(ctx).Then(
		clientv3.OpPut(key, string(val), clientv3.WithLease(lease.ID)),
		clientv3.OpPut(key+"/last_seen", now, clientv3.WithLease(lease.ID)),
	)
	_, err = txn.Commit()
	if err != nil {
		return "", err
	}
	return workerID, nil
}

func (c *etcdCluster) ListWorkers(ctx context.Context) ([]WorkerInfo, error) {
	prefix := path.Join(c.Prefix(), "workers") + "/"
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
	key := path.Join(c.Prefix(), "workers", workerID)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return fmt.Errorf("worker %s not found", workerID)
	}
	leaseID := clientv3.LeaseID(resp.Kvs[0].Lease)
	txn := c.client.Txn(ctx).Then(
		clientv3.OpPut(key+"/last_seen", now, clientv3.WithLease(leaseID)),
	)
	_, err = txn.Commit()
	if err != nil {
		return err
	}
	_, err = c.client.KeepAliveOnce(ctx, leaseID)
	return err
}
