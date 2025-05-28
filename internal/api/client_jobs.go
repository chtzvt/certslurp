package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/chtzvt/certslurp/internal/cluster"
	"github.com/chtzvt/certslurp/internal/job"
)

// SubmitJob posts a new job spec, returns the job ID.
func (c *Client) SubmitJob(ctx context.Context, spec *job.JobSpec) (string, error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", c.BaseURL+"/api/jobs", bytes.NewReader(b))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.AuthToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return "", parseAPIError(resp)
	}
	var out struct {
		JobID string `json:"job_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.JobID, nil
}

// GetJob fetches a job by ID.
func (c *Client) GetJob(ctx context.Context, id string) (*cluster.JobInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.BaseURL+"/api/jobs/"+id, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.AuthToken)
	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, parseAPIError(resp)
	}
	var info cluster.JobInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, err
	}
	return &info, nil
}

// ListJobs returns all jobs.
func (c *Client) ListJobs(ctx context.Context) ([]cluster.JobInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.BaseURL+"/api/jobs", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.AuthToken)
	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, parseAPIError(resp)
	}
	var jobs []cluster.JobInfo
	if err := json.NewDecoder(resp.Body).Decode(&jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

// UpdateJobStatus PATCH /api/jobs/{id}/status
func (c *Client) UpdateJobStatus(ctx context.Context, jobID string, status cluster.JobState) error {
	body := map[string]string{"status": string(status)}
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, "PATCH", c.BaseURL+"/api/jobs/"+url.PathEscape(jobID)+"/status", bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.AuthToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return parseAPIError(resp)
	}
	return nil
}

// MarkJobStarted POST /api/jobs/{id}/start
func (c *Client) MarkJobStarted(ctx context.Context, jobID string) error {
	req, err := http.NewRequestWithContext(ctx, "POST", c.BaseURL+"/api/jobs/"+url.PathEscape(jobID)+"/start", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.AuthToken)
	resp, err := c.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return parseAPIError(resp)
	}
	return nil
}

// MarkJobCompleted POST /api/jobs/{id}/complete
func (c *Client) MarkJobCompleted(ctx context.Context, jobID string) error {
	req, err := http.NewRequestWithContext(ctx, "POST", c.BaseURL+"/api/jobs/"+url.PathEscape(jobID)+"/complete", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.AuthToken)
	resp, err := c.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return parseAPIError(resp)
	}
	return nil
}

// CancelJob POST /api/jobs/{id}/cancel
func (c *Client) CancelJob(ctx context.Context, jobID string) error {
	req, err := http.NewRequestWithContext(ctx, "POST", c.BaseURL+"/api/jobs/"+url.PathEscape(jobID)+"/cancel", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.AuthToken)
	resp, err := c.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return parseAPIError(resp)
	}
	return nil
}

// GetShardAssignments GET /api/jobs/{jobID}/shards?start=...&end=...
func (c *Client) GetShardAssignments(ctx context.Context, jobID string, start, end *int) (map[int]cluster.ShardAssignmentStatus, error) {
	urlStr := c.BaseURL + "/api/jobs/" + url.PathEscape(jobID) + "/shards"
	values := url.Values{}
	if start != nil {
		values.Set("start", strconv.Itoa(*start))
	}
	if end != nil {
		values.Set("end", strconv.Itoa(*end))
	}
	if len(values) > 0 {
		urlStr += "?" + values.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, "GET", urlStr, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.AuthToken)
	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, parseAPIError(resp)
	}
	var result map[int]cluster.ShardAssignmentStatus
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetShardStatus GET /api/jobs/{jobID}/shards/{shardID}
func (c *Client) GetShardStatus(ctx context.Context, jobID string, shardID int) (cluster.ShardStatus, error) {
	urlStr := fmt.Sprintf("%s/api/jobs/%s/shards/%d", c.BaseURL, url.PathEscape(jobID), shardID)
	req, err := http.NewRequestWithContext(ctx, "GET", urlStr, nil)
	if err != nil {
		return cluster.ShardStatus{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.AuthToken)
	resp, err := c.Client.Do(req)
	if err != nil {
		return cluster.ShardStatus{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return cluster.ShardStatus{}, parseAPIError(resp)
	}
	var status cluster.ShardStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return cluster.ShardStatus{}, err
	}
	return status, nil
}
