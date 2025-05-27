package cluster

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type WorkerMetrics struct {
	ShardsProcessed int64 // atomic
	ShardsFailed    int64 // atomic
	processingTime  int64 // nanoseconds, atomic

	mu sync.Mutex
}

func (m *WorkerMetrics) Snapshot() (processed, failed int64, totalTime time.Duration) {
	return atomic.LoadInt64(&m.ShardsProcessed),
		atomic.LoadInt64(&m.ShardsFailed),
		time.Duration(atomic.LoadInt64(&m.processingTime))
}

// Helper methods for atomic increments
func (m *WorkerMetrics) IncProcessed() {
	atomic.AddInt64(&m.ShardsProcessed, 1)
}
func (m *WorkerMetrics) IncFailed() {
	atomic.AddInt64(&m.ShardsFailed, 1)
}
func (m *WorkerMetrics) AddProcessingTime(d time.Duration) {
	atomic.AddInt64(&m.processingTime, d.Nanoseconds())
}
func (m *WorkerMetrics) ProcessingTime() time.Duration {
	return time.Duration(atomic.LoadInt64(&m.processingTime))
}

func (c *etcdCluster) SendMetrics(ctx context.Context, workerID string, metrics *WorkerMetrics) error {
	key := path.Join(c.Prefix(), "workers", workerID)
	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return fmt.Errorf("worker %s not found", workerID)
	}
	leaseID := clientv3.LeaseID(resp.Kvs[0].Lease)

	processed, failed, processingTime := metrics.Snapshot()
	now := time.Now().UTC().Format(time.RFC3339Nano)

	txn := c.client.Txn(ctx).Then(
		clientv3.OpPut(key+"/shards_processed", fmt.Sprintf("%v", processed), clientv3.WithLease(leaseID)),
		clientv3.OpPut(key+"/shards_failed", fmt.Sprintf("%v", failed), clientv3.WithLease(leaseID)),
		clientv3.OpPut(key+"/processing_time_ns", fmt.Sprintf("%v", processingTime.Nanoseconds()), clientv3.WithLease(leaseID)),
		clientv3.OpPut(key+"/last_updated", now, clientv3.WithLease(leaseID)),
	)
	_, err = txn.Commit()
	return err
}

type WorkerMetricsView struct {
	WorkerID         string    `json:"worker_id"`
	ShardsProcessed  int64     `json:"shards_processed"`
	ShardsFailed     int64     `json:"shards_failed"`
	ProcessingTimeNs int64     `json:"processing_time_ns"`
	LastUpdated      time.Time `json:"last_updated"`
}

func (c *etcdCluster) GetWorkerMetrics(ctx context.Context, workerID string) (*WorkerMetricsView, error) {
	keyBase := path.Join(c.Prefix(), "workers", workerID)
	keys := []string{
		keyBase + "/shards_processed",
		keyBase + "/shards_failed",
		keyBase + "/processing_time_ns",
		keyBase + "/last_updated",
	}
	out := WorkerMetricsView{WorkerID: workerID}
	for _, key := range keys {
		resp, err := c.client.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if len(resp.Kvs) == 0 {
			continue
		}
		switch {
		case keyHasSuffix(key, "/shards_processed"):
			out.ShardsProcessed, _ = strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
		case keyHasSuffix(key, "/shards_failed"):
			out.ShardsFailed, _ = strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
		case keyHasSuffix(key, "/processing_time_ns"):
			out.ProcessingTimeNs, _ = strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
		case keyHasSuffix(key, "/last_updated"):
			out.LastUpdated, _ = time.Parse(time.RFC3339Nano, string(resp.Kvs[0].Value))
		}
	}
	return &out, nil
}
