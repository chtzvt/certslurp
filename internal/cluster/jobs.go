package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/chtzvt/certslurp/internal/job"
	"github.com/google/uuid"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type JobInfo struct {
	ID        string       `json:"id"`
	Spec      *job.JobSpec `json:"spec"`
	Submitted time.Time    `json:"submitted"`
	Started   time.Time    `json:"started,omitempty"`
	Completed time.Time    `json:"completed,omitempty"`
	Status    JobState     `json:"status"`
	Cancelled time.Time    `json:"cancelled,omitempty"`
}

type JobState string

const (
	JobStatePending   JobState = "pending"
	JobStateRunning   JobState = "running"
	JobStateCompleted JobState = "completed"
	JobStateCancelled JobState = "cancelled"
)

func (c *etcdCluster) SubmitJob(ctx context.Context, spec *job.JobSpec) (string, error) {
	jobID := uuid.New().String()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	base := fmt.Sprintf("%s/jobs/%s", c.Prefix(), jobID)
	txn := c.client.Txn(ctx).Then(
		clientv3.OpPut(base+"/spec", mustJSON(spec)),
		clientv3.OpPut(base+"/submitted", now),
		clientv3.OpPut(base+"/status", string(JobStatePending)),
	)
	_, err := txn.Commit()
	if err != nil {
		return "", err
	}
	return jobID, nil
}

func (c *etcdCluster) ListJobs(ctx context.Context) ([]JobInfo, error) {
	prefix := fmt.Sprintf("%s/jobs/", c.Prefix())
	resp, err := c.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	jobMap := make(map[string]*JobInfo)
	for _, kv := range resp.Kvs {
		parts := strings.Split(string(kv.Key), "/")
		if len(parts) < 4 {
			continue
		}
		jobID := parts[3]
		if jobMap[jobID] == nil {
			jobMap[jobID] = &JobInfo{ID: jobID}
		}
		switch {
		case strings.HasSuffix(string(kv.Key), "/spec"):
			var spec job.JobSpec
			if err := json.Unmarshal(kv.Value, &spec); err == nil {
				jobMap[jobID].Spec = &spec
			}
		case strings.HasSuffix(string(kv.Key), "/submitted"):
			if ts, err := time.Parse(time.RFC3339Nano, string(kv.Value)); err == nil {
				jobMap[jobID].Submitted = ts
			}
		case strings.HasSuffix(string(kv.Key), "/started"):
			if ts, err := time.Parse(time.RFC3339Nano, string(kv.Value)); err == nil {
				jobMap[jobID].Started = ts
			}
		case strings.HasSuffix(string(kv.Key), "/completed"):
			if ts, err := time.Parse(time.RFC3339Nano, string(kv.Value)); err == nil {
				jobMap[jobID].Completed = ts
			}
		case strings.HasSuffix(string(kv.Key), "/cancelled"):
			if ts, err := time.Parse(time.RFC3339Nano, string(kv.Value)); err == nil {
				jobMap[jobID].Cancelled = ts
			}
		case strings.HasSuffix(string(kv.Key), "/status"):
			jobMap[jobID].Status = JobState(kv.Value)
		}
	}
	jobs := make([]JobInfo, 0, len(jobMap))
	for _, info := range jobMap {
		jobs = append(jobs, *info)
	}
	return jobs, nil
}

func (c *etcdCluster) GetJob(ctx context.Context, jobID string) (*JobInfo, error) {
	prefix := fmt.Sprintf("%s/jobs/%s/", c.Prefix(), jobID)
	resp, err := c.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("job %q not found", jobID)
	}
	info := &JobInfo{ID: jobID}
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		switch {
		case strings.HasSuffix(key, "/spec"):
			var spec job.JobSpec
			if err := json.Unmarshal(kv.Value, &spec); err == nil {
				info.Spec = &spec
			}
		case strings.HasSuffix(key, "/submitted"):
			if ts, err := time.Parse(time.RFC3339Nano, string(kv.Value)); err == nil {
				info.Submitted = ts
			}
		case strings.HasSuffix(key, "/started"):
			if ts, err := time.Parse(time.RFC3339Nano, string(kv.Value)); err == nil {
				info.Started = ts
			}
		case strings.HasSuffix(key, "/completed"):
			if ts, err := time.Parse(time.RFC3339Nano, string(kv.Value)); err == nil {
				info.Completed = ts
			}
		case strings.HasSuffix(key, "/cancelled"):
			if ts, err := time.Parse(time.RFC3339Nano, string(kv.Value)); err == nil {
				info.Cancelled = ts
			}
		case strings.HasSuffix(key, "/status"):
			info.Status = JobState(kv.Value)
		}
	}
	return info, nil
}

func (c *etcdCluster) UpdateJobStatus(ctx context.Context, jobID string, status JobState) error {
	key := fmt.Sprintf("%s/jobs/%s/status", c.Prefix(), jobID)
	_, err := c.client.Put(ctx, key, string(status))
	return err
}

func (c *etcdCluster) MarkJobStarted(ctx context.Context, jobID string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	statusKey := fmt.Sprintf("%s/jobs/%s/status", c.Prefix(), jobID)
	startedKey := fmt.Sprintf("%s/jobs/%s/started", c.Prefix(), jobID)

	txn := c.client.Txn(ctx).Then(
		clientv3.OpPut(startedKey, now),
		clientv3.OpPut(statusKey, string(JobStateRunning)),
	)
	_, err := txn.Commit()
	return err
}

func (c *etcdCluster) MarkJobCompleted(ctx context.Context, jobID string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	statusKey := fmt.Sprintf("%s/jobs/%s/status", c.Prefix(), jobID)
	completedKey := fmt.Sprintf("%s/jobs/%s/completed", c.Prefix(), jobID)

	txn := c.client.Txn(ctx).Then(
		clientv3.OpPut(completedKey, now),
		clientv3.OpPut(statusKey, string(JobStateCompleted)),
	)
	_, err := txn.Commit()
	return err
}

func (c *etcdCluster) CancelJob(ctx context.Context, jobID string) error {
	_, err := c.GetJob(ctx, jobID)
	if err != nil {
		return err // or ignore if not found
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	statusKey := fmt.Sprintf("%s/jobs/%s/status", c.Prefix(), jobID)
	cancelledKey := fmt.Sprintf("%s/jobs/%s/cancelled", c.Prefix(), jobID)

	txn := c.client.Txn(ctx).Then(
		clientv3.OpPut(cancelledKey, now),
		clientv3.OpPut(statusKey, string(JobStateCancelled)),
	)
	_, err = txn.Commit()
	return err
}

// Helper to check if job is cancelled
func (c *etcdCluster) IsJobCancelled(ctx context.Context, jobID string) (bool, error) {
	key := fmt.Sprintf("%s/jobs/%s/cancelled", c.Prefix(), jobID)
	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return false, err
	}
	return len(resp.Kvs) > 0, nil
}
