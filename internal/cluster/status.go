package cluster

import "context"

type ClusterStatus struct {
	Jobs    []JobStatus
	Workers []WorkerInfo
}
type JobStatus struct {
	Job    JobInfo
	Shards map[int]ShardAssignmentStatus
}

// GetClusterStatus summarizes all jobs, their shards, and worker health.
func (c *etcdCluster) GetClusterStatus(ctx context.Context) (*ClusterStatus, error) {
	jobs, err := c.ListJobs(ctx)
	if err != nil {
		return nil, err
	}
	workers, err := c.ListWorkers(ctx)
	if err != nil {
		return nil, err
	}
	jobStates := make([]JobStatus, 0, len(jobs))
	for _, job := range jobs {
		shards, err := c.GetShardAssignments(ctx, job.ID)
		if err != nil {
			continue // skip on error
		}
		jobStates = append(jobStates, JobStatus{
			Job:    job,
			Shards: shards,
		})
	}
	return &ClusterStatus{
		Jobs:    jobStates,
		Workers: workers,
	}, nil
}
