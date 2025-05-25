package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"path"

	"github.com/chtzvt/ctsnarf/internal/job"
	"github.com/google/uuid"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func (c *etcdCluster) SubmitJob(ctx context.Context, spec *job.JobSpec) (string, error) {
	jobID := uuid.New().String()
	key := path.Join(c.cfg.Prefix, "jobs", jobID)
	specBytes, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	_, err = c.client.Put(ctx, key, string(specBytes))
	if err != nil {
		return "", err
	}
	return jobID, nil
}

func (c *etcdCluster) ListJobs(ctx context.Context) ([]JobInfo, error) {
	prefix := path.Join(c.cfg.Prefix, "jobs") + "/"
	resp, err := c.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	jobs := make([]JobInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var spec job.JobSpec
		if err := json.Unmarshal(kv.Value, &spec); err != nil {
			continue // or collect error
		}
		id := path.Base(string(kv.Key))
		jobs = append(jobs, JobInfo{ID: id, Spec: &spec})
	}
	return jobs, nil
}

func (c *etcdCluster) GetJob(ctx context.Context, jobID string) (*job.JobSpec, error) {
	key := path.Join(c.cfg.Prefix, "jobs", jobID)
	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("job %s not found", jobID)
	}
	var spec job.JobSpec
	if err := json.Unmarshal(resp.Kvs[0].Value, &spec); err != nil {
		return nil, err
	}
	return &spec, nil
}
