// api/api_test.go

package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"

	"github.com/chtzvt/certslurp/internal/cluster"
	"github.com/chtzvt/certslurp/internal/job"
	"github.com/chtzvt/certslurp/internal/secrets"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type stubCluster struct {
	jobs map[string]*cluster.JobInfo
}

func newStubCluster() *stubCluster {
	return &stubCluster{
		jobs: make(map[string]*cluster.JobInfo),
	}
}

func (s *stubCluster) SubmitJob(ctx context.Context, spec *job.JobSpec) (string, error) {
	id := "testjob123"
	s.jobs[id] = &cluster.JobInfo{ID: id, Spec: spec}
	return id, nil
}

func (s *stubCluster) ListJobs(ctx context.Context) ([]cluster.JobInfo, error) {
	out := []cluster.JobInfo{}
	for _, j := range s.jobs {
		out = append(out, *j)
	}
	return out, nil
}

func (s *stubCluster) GetJob(ctx context.Context, jobID string) (*cluster.JobInfo, error) {
	job, ok := s.jobs[jobID]
	if !ok {
		return nil, errors.New("not found")
	}
	return job, nil
}

func setupTestServer() (*httptest.Server, *stubCluster) {
	stub := newStubCluster()
	mux := http.NewServeMux()
	RegisterJobHandlers(mux, stub)
	return httptest.NewServer(mux), stub
}

func setupAuthTestServer(token string) (*httptest.Server, *stubCluster) {
	stub := newStubCluster()
	mux := http.NewServeMux()
	RegisterJobHandlers(mux, stub)
	server := httptest.NewServer(TokenAuthMiddleware([]string{token}, mux))
	return server, stub
}

// Other methods of cluster.Cluster can panic/return errors as not needed for this package's test suite
func (s *stubCluster) GetClusterStatus(context.Context) (*cluster.ClusterStatus, error) {
	return nil, nil
}
func (s *stubCluster) UpdateJobStatus(context.Context, string, cluster.JobState) error { return nil }
func (s *stubCluster) MarkJobStarted(context.Context, string) error                    { return nil }
func (s *stubCluster) MarkJobCompleted(context.Context, string) error                  { return nil }
func (s *stubCluster) CancelJob(context.Context, string) error                         { return nil }
func (s *stubCluster) IsJobCancelled(context.Context, string) (bool, error)            { return false, nil }
func (s *stubCluster) RegisterWorker(context.Context, cluster.WorkerInfo) (string, error) {
	return "", nil
}
func (s *stubCluster) ListWorkers(context.Context) ([]cluster.WorkerInfo, error) { return nil, nil }
func (s *stubCluster) HeartbeatWorker(context.Context, string) error             { return nil }
func (s *stubCluster) BulkCreateShards(context.Context, string, []cluster.ShardRange) error {
	return nil
}
func (s *stubCluster) GetShardCount(context.Context, string) (int, error)     { return 0, nil }
func (s *stubCluster) AssignShard(context.Context, string, int, string) error { return nil }
func (s *stubCluster) GetShardAssignments(context.Context, string) (map[int]cluster.ShardAssignmentStatus, error) {
	return nil, nil
}
func (s *stubCluster) GetShardAssignmentsWindow(context.Context, string, int, int) (map[int]cluster.ShardAssignmentStatus, error) {
	return nil, nil
}
func (s *stubCluster) GetShardStatus(context.Context, string, int) (cluster.ShardStatus, error) {
	return cluster.ShardStatus{}, nil
}
func (s *stubCluster) ReportShardDone(context.Context, string, int, cluster.ShardManifest) error {
	return nil
}
func (s *stubCluster) ReportShardFailed(context.Context, string, int) error { return nil }
func (s *stubCluster) RequestShardSplit(context.Context, string, int, []cluster.ShardRange) error {
	return nil
}
func (s *stubCluster) FindOrphanedShards(context.Context, string) ([]int, error) { return nil, nil }
func (s *stubCluster) ReassignOrphanedShards(context.Context, string, string) ([]int, error) {
	return nil, nil
}
func (s *stubCluster) ShardKey(string, int) string { return "" }
func (s *stubCluster) Secrets() *secrets.Store     { return nil }
func (s *stubCluster) Prefix() string              { return "" }
func (s *stubCluster) Client() *clientv3.Client    { return nil }
func (s *stubCluster) Close() error                { return nil }
