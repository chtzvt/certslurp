package cluster

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func (c *etcdCluster) AssignShard(ctx context.Context, jobID string, shardID int, workerID string) error {
	key := path.Join(c.cfg.Prefix, "shards", jobID, fmt.Sprintf("%d", shardID), "assignment")
	val := []byte(workerID)
	_, err := c.client.Put(ctx, key, string(val))
	return err
}

func (c *etcdCluster) GetShardAssignments(ctx context.Context, jobID string) (map[int]string, error) {
	prefix := path.Join(c.cfg.Prefix, "shards", jobID) + "/"
	resp, err := c.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	assignments := make(map[int]string)
	for _, kv := range resp.Kvs {
		keyParts := strings.Split(string(kv.Key), "/")
		// .../shards/<job-id>/<shard-id>/assignment
		if len(keyParts) < 2 {
			continue
		}
		if !strings.HasSuffix(string(kv.Key), "/assignment") {
			continue
		}
		shardStr := keyParts[len(keyParts)-2]
		var shardID int
		fmt.Sscanf(shardStr, "%d", &shardID)
		assignments[shardID] = string(kv.Value)
	}
	return assignments, nil
}

func (c *etcdCluster) ReportShardDone(ctx context.Context, jobID string, shardID int) error {
	key := path.Join(c.cfg.Prefix, "shards", jobID, fmt.Sprintf("%d", shardID), "done")
	val := []byte(time.Now().Format(time.RFC3339))
	_, err := c.client.Put(ctx, key, string(val))
	return err
}
