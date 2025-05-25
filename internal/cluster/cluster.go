// cluster/cluster.go

package cluster

import (
	"context"
	"time"

	"github.com/chtzvt/ctsnarf/internal/job"
)

// Cluster abstracts all distributed coordination and state.
type Cluster interface {
	SubmitJob(ctx context.Context, spec *job.JobSpec) (jobID string, err error)
	ListJobs(ctx context.Context) ([]JobInfo, error)
	GetJob(ctx context.Context, jobID string) (*job.JobSpec, error)

	RegisterWorker(ctx context.Context, info WorkerInfo) (workerID string, err error)
	ListWorkers(ctx context.Context) ([]WorkerInfo, error)
	HeartbeatWorker(ctx context.Context, workerID string) error

	AssignShard(ctx context.Context, jobID string, shardID int, workerID string) error
	GetShardAssignments(ctx context.Context, jobID string) (map[int]string, error)
	ReportShardDone(ctx context.Context, jobID string, shardID int) error

	Close() error
}

// JobInfo: minimal info for listing
type JobInfo struct {
	ID        string
	Spec      *job.JobSpec
	Submitted time.Time
	Status    string // optional
}

type WorkerInfo struct {
	ID       string
	Host     string
	LastSeen time.Time
	// etc: version, running jobs, etc.
}
