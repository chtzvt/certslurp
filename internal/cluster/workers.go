package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
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

	lease, err := c.client.Grant(ctx, 150)
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
	workers := make(map[string]*WorkerInfo)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		rel := key[len(prefix):]
		if rel == "" || rel == "last_seen" || strings.Contains(rel, "/") {
			parts := strings.Split(rel, "/")
			if len(parts) == 2 && parts[1] == "last_seen" {
				workerID := parts[0]
				if worker, ok := workers[workerID]; ok {
					// parse last_seen timestamp
					t, err := time.Parse(time.RFC3339Nano, string(kv.Value))
					if err == nil {
						worker.LastSeen = t
					}
				} else {
					// If we haven't seen the main info yet, create placeholder
					t, err := time.Parse(time.RFC3339Nano, string(kv.Value))
					if err == nil {
						workers[workerID] = &WorkerInfo{
							ID:       workerID,
							LastSeen: t,
						}
					}
				}
			}
			continue
		}

		var info WorkerInfo
		if err := json.Unmarshal(kv.Value, &info); err == nil {
			workers[info.ID] = &info
		}
	}

	result := make([]WorkerInfo, 0, len(workers))
	for _, w := range workers {
		result = append(result, *w)
	}
	return result, nil
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
