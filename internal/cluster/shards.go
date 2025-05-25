package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	shardLeaseSeconds = 60
	maxShardRetries   = 3
	shardRetryBackoff = 30 * time.Second
)

type ShardAssignment struct {
	WorkerID    string    `json:"worker_id"`
	AssignedAt  time.Time `json:"assigned_at"`
	LeaseExpiry time.Time `json:"lease_expiry"`
}

// ShardAssignmentStatus captures all assignment state for a shard
type ShardAssignmentStatus struct {
	ShardID      int
	Assigned     bool
	WorkerID     string
	LeaseExpiry  time.Time
	Done         bool
	Failed       bool
	Retries      int
	BackoffUntil time.Time
	OutputPath   string
	IndexFrom    int64
	IndexTo      int64
}

type ShardManifest struct {
	OutputPath   string    `json:"output_path,omitempty"`
	DoneAt       time.Time `json:"done_at"`
	Failed       bool      `json:"failed,omitempty"`
	Retries      int       `json:"retries,omitempty"`
	BackoffUntil time.Time `json:"backoff_until,omitempty"`
}

type ShardStatus struct {
	Assigned     bool
	WorkerID     string
	LeaseExpiry  time.Time
	Done         bool
	Failed       bool
	Retries      int
	BackoffUntil time.Time
	OutputPath   string
	IndexFrom    int64
	IndexTo      int64
}

type ShardRange struct {
	ShardID   int
	IndexFrom int64 // inclusive
	IndexTo   int64 // exclusive
}

// BulkCreateShards creates multiple shard manifests in a single atomic etcd operation.
// If any already exist, they're skipped (idempotent).
func (c *etcdCluster) BulkCreateShards(ctx context.Context, jobID string, ranges []ShardRange) error {
	const batchSize = 128 // etcd transaction limit is 128 ops

	for start := 0; start < len(ranges); start += batchSize {
		end := start + batchSize
		if end > len(ranges) {
			end = len(ranges)
		}
		txn := c.client.Txn(ctx)
		cmps := []clientv3.Cmp{}
		puts := []clientv3.Op{}

		for _, rng := range ranges[start:end] {
			base := fmt.Sprintf("%s/jobs/%s/shards/%d", c.cfg.Prefix, jobID, rng.ShardID)
			rangeKey := base + "/range"
			// Only put if doesn't exist
			cmps = append(cmps, clientv3.Compare(clientv3.Version(rangeKey), "=", 0))
			val, _ := json.Marshal(rng)
			puts = append(puts, clientv3.OpPut(rangeKey, string(val)))
		}
		// Only write shards that don't exist
		txn = txn.If(cmps...).Then(puts...)
		_, err := txn.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

// GetShardAssignments returns a map of all shards (by shardID) to their assignment status.
func (c *etcdCluster) GetShardAssignments(ctx context.Context, jobID string) (map[int]ShardAssignmentStatus, error) {
	prefix := fmt.Sprintf("%s/jobs/%s/shards/", c.cfg.Prefix, jobID)
	resp, err := c.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	statusMap := map[int]ShardAssignmentStatus{}
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		parts := strings.Split(key, "/")
		if len(parts) < 5 {
			continue // Not a shard key
		}
		shardID := 0
		fmt.Sscanf(parts[len(parts)-2], "%d", &shardID)
		subkey := parts[len(parts)-1]
		stat := statusMap[shardID]

		stat.ShardID = shardID

		switch subkey {
		case "assignment":
			stat.Assigned = true
			var assign ShardAssignment
			_ = json.Unmarshal(kv.Value, &assign)
			stat.WorkerID = assign.WorkerID
			stat.LeaseExpiry = assign.LeaseExpiry
		case "done":
			stat.Done = true
			var man ShardManifest
			_ = json.Unmarshal(kv.Value, &man)
			stat.OutputPath = man.OutputPath
			stat.Failed = man.Failed
		case "failed":
			stat.Failed = true
		case "retries":
			fmt.Sscanf(string(kv.Value), "%d", &stat.Retries)
		case "backoff_until":
			t, err := time.Parse(time.RFC3339Nano, string(kv.Value))
			if err == nil {
				stat.BackoffUntil = t
			}
		case "range":
			var rng ShardRange
			if err := json.Unmarshal(kv.Value, &rng); err == nil {
				stat.IndexFrom = rng.IndexFrom
				stat.IndexTo = rng.IndexTo
			}
		}
		statusMap[shardID] = stat
	}
	return statusMap, nil
}

func (c *etcdCluster) AssignShard(ctx context.Context, jobID string, shardID int, workerID string) error {
	shardPrefix := fmt.Sprintf("%s/jobs/%s/shards/%d", c.cfg.Prefix, jobID, shardID)
	assignmentKey := shardPrefix + "/assignment"
	doneKey := shardPrefix + "/done"
	retriesKey := shardPrefix + "/retries"
	backoffKey := shardPrefix + "/backoff_until"

	now := time.Now().UTC()
	leaseExpiry := now.Add(shardLeaseSeconds * time.Second)

	// Get all relevant keys in one go (reduces round trips)
	getOps := []clientv3.Op{
		clientv3.OpGet(assignmentKey),
		clientv3.OpGet(doneKey),
		clientv3.OpGet(retriesKey),
		clientv3.OpGet(backoffKey),
	}
	txn := c.client.Txn(ctx).Then(getOps...)
	txnResp, err := txn.Commit()
	if err != nil {
		return err
	}

	assignExists := len(txnResp.Responses[0].GetResponseRange().Kvs) > 0
	doneExists := len(txnResp.Responses[1].GetResponseRange().Kvs) > 0
	var retries int
	if len(txnResp.Responses[2].GetResponseRange().Kvs) > 0 {
		retriesStr := string(txnResp.Responses[2].GetResponseRange().Kvs[0].Value)
		retries, _ = strconv.Atoi(retriesStr)
	}
	var backoffUntil time.Time
	if len(txnResp.Responses[3].GetResponseRange().Kvs) > 0 {
		backoffUntil.UnmarshalText(txnResp.Responses[3].GetResponseRange().Kvs[0].Value)
	}

	if doneExists {
		return fmt.Errorf("shard %d already completed", shardID)
	}
	if retries >= maxShardRetries {
		return fmt.Errorf("shard %d permanently failed (retries exceeded)", shardID)
	}
	if !backoffUntil.IsZero() && now.Before(backoffUntil) {
		return fmt.Errorf("shard %d in backoff until %v", shardID, backoffUntil)
	}

	assignment := ShardAssignment{
		WorkerID:    workerID,
		AssignedAt:  now,
		LeaseExpiry: leaseExpiry,
	}
	assignmentBytes, _ := json.Marshal(assignment)

	if assignExists {
		var assign ShardAssignment
		json.Unmarshal(txnResp.Responses[0].GetResponseRange().Kvs[0].Value, &assign)
		if assign.LeaseExpiry.After(now) {
			return fmt.Errorf("shard %d already assigned", shardID)
		}
		// Assignment expired: try to claim via CAS
		cmp := clientv3.Compare(clientv3.Value(assignmentKey), "=", string(txnResp.Responses[0].GetResponseRange().Kvs[0].Value))
		txn2 := c.client.Txn(ctx).If(cmp).Then(
			clientv3.OpPut(assignmentKey, string(assignmentBytes)),
			clientv3.OpPut(shardPrefix+"/in_progress", now.Format(time.RFC3339Nano)),
		)
		txn2Resp, err := txn2.Commit()
		if err != nil {
			return err
		}
		if !txn2Resp.Succeeded {
			return fmt.Errorf("shard %d work stealing failed (race)", shardID)
		}
		return nil
	} else {
		// No assignment: normal claim
		cmp := clientv3.Compare(clientv3.Version(assignmentKey), "=", 0)
		txn2 := c.client.Txn(ctx).If(cmp).Then(
			clientv3.OpPut(assignmentKey, string(assignmentBytes)),
			clientv3.OpPut(shardPrefix+"/in_progress", now.Format(time.RFC3339Nano)),
		)
		txn2Resp, err := txn2.Commit()
		if err != nil {
			return err
		}
		if !txn2Resp.Succeeded {
			return fmt.Errorf("shard %d assignment race", shardID)
		}
		return nil
	}
}

func (c *etcdCluster) GetShardStatus(ctx context.Context, jobID string, shardID int) (ShardStatus, error) {
	base := fmt.Sprintf("%s/jobs/%s/shards/%d", c.cfg.Prefix, jobID, shardID)
	keys := []string{
		base + "/assignment",
		base + "/done",
		base + "/failed",
		base + "/retries",
		base + "/backoff_until",
		base + "/range",
	}
	resps := make([]*clientv3.GetResponse, len(keys))

	// Batch get (could optimize with WithPrefix and parsing, but simple for single shard)
	for i, k := range keys {
		resp, err := c.client.Get(ctx, k)
		if err != nil {
			return ShardStatus{}, err
		}
		resps[i] = resp
	}

	status := ShardStatus{}
	// assignment
	if len(resps[0].Kvs) > 0 {
		status.Assigned = true
		var assign ShardAssignment
		if err := json.Unmarshal(resps[0].Kvs[0].Value, &assign); err == nil {
			status.WorkerID = assign.WorkerID
			status.LeaseExpiry = assign.LeaseExpiry
		}
	}
	// done
	if len(resps[1].Kvs) > 0 {
		status.Done = true
		var manifest ShardManifest
		if err := json.Unmarshal(resps[1].Kvs[0].Value, &manifest); err == nil {
			status.OutputPath = manifest.OutputPath
			status.Failed = manifest.Failed
		}
	}
	// failed
	if len(resps[2].Kvs) > 0 {
		status.Failed = true
	}
	// retries
	if len(resps[3].Kvs) > 0 {
		fmt.Sscanf(string(resps[3].Kvs[0].Value), "%d", &status.Retries)
	}
	// backoff_until
	if len(resps[4].Kvs) > 0 {
		ts := string(resps[4].Kvs[0].Value)
		status.BackoffUntil, _ = time.Parse(time.RFC3339Nano, ts)
	}

	if len(resps[5].Kvs) > 0 {
		var rng ShardRange
		if err := json.Unmarshal(resps[5].Kvs[0].Value, &rng); err == nil {
			status.IndexFrom = rng.IndexFrom
			status.IndexTo = rng.IndexTo
		}
	}

	return status, nil
}

func (c *etcdCluster) RequestShardSplit(ctx context.Context, jobID string, shardID int, newRanges []ShardRange) error {
	shardPrefix := fmt.Sprintf("%s/jobs/%s/shards/%d", c.cfg.Prefix, jobID, shardID)
	splitKey := shardPrefix + "/split"

	// Mark the original as "split" (prevents new assignment)
	// Atomically add new shards (idempotent if already exist)
	splitFlag := []byte("1")

	// Store the split flag
	txn := c.client.Txn(ctx)
	txn = txn.Then(clientv3.OpPut(splitKey, string(splitFlag)))
	_, err := txn.Commit()
	if err != nil {
		return err
	}
	// Add the new shards (BulkCreateShards is idempotent)
	return c.BulkCreateShards(ctx, jobID, newRanges)
}

func (c *etcdCluster) ReportShardFailed(ctx context.Context, jobID string, shardID int) error {
	shardPrefix := fmt.Sprintf("%s/jobs/%s/shards/%d", c.cfg.Prefix, jobID, shardID)
	retriesKey := shardPrefix + "/retries"
	backoffKey := shardPrefix + "/backoff_until"
	assignmentKey := shardPrefix + "/assignment"
	inProgressKey := shardPrefix + "/in_progress"
	doneKey := shardPrefix + "/done"

	// Get and increment retries
	var retries int
	resp, err := c.client.Get(ctx, retriesKey)
	if err != nil {
		return err
	}
	if len(resp.Kvs) > 0 {
		retries, _ = strconv.Atoi(string(resp.Kvs[0].Value))
	}
	retries++
	if retries > maxShardRetries {
		// Mark permanently failed
		man := ShardManifest{
			DoneAt:  time.Now().UTC(),
			Failed:  true,
			Retries: retries,
		}
		manBytes, _ := json.Marshal(man)
		_, err := c.client.Txn(ctx).Then(
			clientv3.OpPut(doneKey, string(manBytes)),
			clientv3.OpDelete(assignmentKey),
			clientv3.OpDelete(inProgressKey),
			clientv3.OpDelete(retriesKey),
			clientv3.OpDelete(backoffKey),
		).Commit()
		return err
	}

	// Calculate next backoff (exponential or fixed)
	backoffDuration := shardRetryBackoff * time.Duration(1<<uint(retries-1)) // exponential: 30s, 60s, 120s, ...
	backoffUntil := time.Now().Add(backoffDuration)

	backoffBytes, _ := backoffUntil.MarshalText()
	_, err = c.client.Txn(ctx).Then(
		clientv3.OpPut(retriesKey, fmt.Sprintf("%d", retries)),
		clientv3.OpPut(backoffKey, string(backoffBytes)),
		clientv3.OpDelete(assignmentKey),
		clientv3.OpDelete(inProgressKey),
	).Commit()
	return err
}

func (c *etcdCluster) ReportShardDone(ctx context.Context, jobID string, shardID int, manifest ShardManifest) error {
	shardPrefix := fmt.Sprintf("%s/jobs/%s/shards/%d", c.cfg.Prefix, jobID, shardID)
	assignmentKey := shardPrefix + "/assignment"
	inProgressKey := shardPrefix + "/in_progress"
	doneKey := shardPrefix + "/done"
	retriesKey := shardPrefix + "/retries"
	backoffKey := shardPrefix + "/backoff_until"

	manifest.DoneAt = time.Now().UTC()
	manBytes, _ := json.Marshal(manifest)

	txn := c.client.Txn(ctx).Then(
		clientv3.OpPut(doneKey, string(manBytes)),
		clientv3.OpDelete(assignmentKey),
		clientv3.OpDelete(inProgressKey),
		clientv3.OpDelete(retriesKey),
		clientv3.OpDelete(backoffKey),
	)
	_, err := txn.Commit()
	return err
}

func (c *etcdCluster) FindOrphanedShards(ctx context.Context, jobID string) ([]int, error) {
	shards, err := c.GetShardAssignments(ctx, jobID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	orphaned := []int{}
	for id, s := range shards {
		if s.Assigned && !s.Done && s.LeaseExpiry.Before(now) {
			orphaned = append(orphaned, id)
		}
	}
	return orphaned, nil
}

func (c *etcdCluster) ReassignOrphanedShards(ctx context.Context, jobID string, assignTo string) ([]int, error) {
	orphaned, err := c.FindOrphanedShards(ctx, jobID)
	if err != nil {
		return nil, err
	}
	for _, shardID := range orphaned {
		// Optionally, do not reassign if the shard is split/cancelled
		_ = c.AssignShard(ctx, jobID, shardID, assignTo)
	}
	return orphaned, nil
}
