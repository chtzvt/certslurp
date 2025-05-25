package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/chtzvt/ctsnarf/internal/job"
	"github.com/google/uuid"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func (c *etcdCluster) SubmitJob(ctx context.Context, spec *job.JobSpec) (string, error) {
	jobID := uuid.New().String()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	base := fmt.Sprintf("%s/jobs/%s", c.cfg.Prefix, jobID)
	txn := c.client.Txn(ctx).Then(
		clientv3.OpPut(base+"/spec", mustJSON(spec)),
		clientv3.OpPut(base+"/submitted", now),
		clientv3.OpPut(base+"/status", "pending"),
	)
	_, err := txn.Commit()
	if err != nil {
		return "", err
	}
	return jobID, nil
}

func (c *etcdCluster) ListJobs(ctx context.Context) ([]JobInfo, error) {
	prefix := fmt.Sprintf("%s/jobs/", c.cfg.Prefix)
	resp, err := c.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	jobMap := make(map[string]*JobInfo)
	for _, kv := range resp.Kvs {
		parts := strings.Split(string(kv.Key), "/")
		// /prefix/jobs/<job-id>/...
		if len(parts) < 4 {
			continue
		}
		jobID := parts[3]
		if strings.HasSuffix(string(kv.Key), "/spec") {
			var spec job.JobSpec
			if err := json.Unmarshal(kv.Value, &spec); err == nil {
				if jobMap[jobID] == nil {
					jobMap[jobID] = &JobInfo{ID: jobID}
				}
				jobMap[jobID].Spec = &spec
			}
		}
		// You could also track status/submit time by using a /submitted or /status key per job
	}
	jobs := make([]JobInfo, 0, len(jobMap))
	for _, info := range jobMap {
		jobs = append(jobs, *info)
	}
	return jobs, nil
}

func (c *etcdCluster) GetJob(ctx context.Context, jobID string) (*job.JobSpec, error) {
	key := fmt.Sprintf("%s/jobs/%s/spec", c.cfg.Prefix, jobID)
	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("job %q not found", jobID)
	}
	var spec job.JobSpec
	if err := json.Unmarshal(resp.Kvs[0].Value, &spec); err != nil {
		return nil, err
	}
	return &spec, nil
}

func (c *etcdCluster) CancelJob(ctx context.Context, jobID string) error {
	key := fmt.Sprintf("%s/jobs/%s/cancelled", c.cfg.Prefix, jobID)
	_, err := c.client.Put(ctx, key, "1")
	return err
}

// Helper to check if job is cancelled
func (c *etcdCluster) IsJobCancelled(ctx context.Context, jobID string) (bool, error) {
	key := fmt.Sprintf("%s/jobs/%s/cancelled", c.cfg.Prefix, jobID)
	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return false, err
	}
	return len(resp.Kvs) > 0, nil
}
