package cluster

import (
	"context"

	"github.com/chtzvt/certslurp/internal/job"
	"github.com/chtzvt/certslurp/internal/secrets"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Cluster interface {
	// Job coordination
	SubmitJob(ctx context.Context, spec *job.JobSpec) (jobID string, err error)
	ListJobs(ctx context.Context) ([]JobInfo, error)
	GetJob(ctx context.Context, jobID string) (*JobInfo, error)
	GetClusterStatus(ctx context.Context) (*ClusterStatus, error)
	UpdateJobStatus(ctx context.Context, jobID string, status JobState) error
	MarkJobStarted(ctx context.Context, jobID string) error
	MarkJobCompleted(ctx context.Context, jobID string) error
	CancelJob(ctx context.Context, jobID string) error
	IsJobCancelled(ctx context.Context, jobID string) (bool, error)

	// Worker management
	RegisterWorker(ctx context.Context, info WorkerInfo) (workerID string, err error)
	ListWorkers(ctx context.Context) ([]WorkerInfo, error)
	HeartbeatWorker(ctx context.Context, workerID string) error
	SendMetrics(ctx context.Context, workerID string, metrics *WorkerMetrics) error
	GetWorkerMetrics(ctx context.Context, workerID string) (*WorkerMetricsView, error)

	// Shard orchestration
	BulkCreateShards(ctx context.Context, jobID string, ranges []ShardRange) error
	GetShardCount(ctx context.Context, jobID string) (int, error)
	AssignShard(ctx context.Context, jobID string, shardID int, workerID string) error
	GetShardAssignments(ctx context.Context, jobID string) (map[int]ShardAssignmentStatus, error)
	GetShardAssignmentsWindow(ctx context.Context, jobID string, start, end int) (map[int]ShardAssignmentStatus, error)
	GetShardStatus(ctx context.Context, jobID string, shardID int) (ShardStatus, error)
	RenewShardLease(ctx context.Context, jobID string, shardID int, workerID string) error
	ReleaseShardLease(ctx context.Context, jobID string, shardID int, workerID string) error
	ReportShardDone(ctx context.Context, jobID string, shardID int, manifest ShardManifest) error
	ReportShardFailed(ctx context.Context, jobID string, shardID int) error
	ResetFailedShards(ctx context.Context, jobID string) ([]int, error)
	ResetFailedShard(ctx context.Context, jobID string, shardID int) error
	RequestShardSplit(ctx context.Context, jobID string, shardID int, newRanges []ShardRange) error
	FindOrphanedShards(ctx context.Context, jobID string) ([]int, error)
	ReassignOrphanedShards(ctx context.Context, jobID string, assignTo string) ([]int, error)
	ShardKey(jobID string, shardID int) string

	Secrets() *secrets.Store

	Prefix() string
	Client() *clientv3.Client
	Close() error
}
