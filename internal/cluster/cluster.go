package cluster

import (
	"context"

	"github.com/chtzvt/ctsnarf/internal/job"
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

	// Shard orchestration
	BulkCreateShards(ctx context.Context, jobID string, ranges []ShardRange) error
	AssignShard(ctx context.Context, jobID string, shardID int, workerID string) error
	GetShardAssignments(ctx context.Context, jobID string) (map[int]ShardAssignmentStatus, error)
	GetShardStatus(ctx context.Context, jobID string, shardID int) (ShardStatus, error)
	ReportShardDone(ctx context.Context, jobID string, shardID int, manifest ShardManifest) error
	ReportShardFailed(ctx context.Context, jobID string, shardID int) error
	RequestShardSplit(ctx context.Context, jobID string, shardID int, newRanges []ShardRange) error
	ReassignOrphanedShards(ctx context.Context, jobID string, assignTo string) ([]int, error)

	Prefix() string
	Client() *clientv3.Client
	Close() error
}
