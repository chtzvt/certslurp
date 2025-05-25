package cluster

import (
	"context"
	"time"

	"github.com/chtzvt/ctsnarf/internal/job"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Cluster interface {
	// Job coordination
	SubmitJob(ctx context.Context, spec *job.JobSpec) (jobID string, err error)
	ListJobs(ctx context.Context) ([]JobInfo, error)
	GetJob(ctx context.Context, jobID string) (*job.JobSpec, error)
	GetClusterStatus(ctx context.Context) (*ClusterStatus, error)
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

	Client() *clientv3.Client
	Close() error
}

type JobInfo struct {
	ID        string
	Spec      *job.JobSpec
	Submitted time.Time
	Status    string
}

type WorkerInfo struct {
	ID       string
	Host     string
	LastSeen time.Time
}
