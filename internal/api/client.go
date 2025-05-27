package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/chtzvt/certslurp/internal/cluster"
	"github.com/chtzvt/certslurp/internal/job"
)

type Client struct {
	BaseURL   string
	AuthToken string
	Client    *http.Client // Allow override for testing
}

// NewClient returns a new API client.
func NewClient(baseURL, token string) *Client {
	return &Client{
		BaseURL:   strings.TrimRight(baseURL, "/"),
		AuthToken: token,
		Client:    &http.Client{Timeout: 10 * time.Second},
	}
}

// Error returned by API calls.
type APIError struct {
	Status int
	Msg    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("api error (%d): %s", e.Status, e.Msg)
}

func parseAPIError(resp *http.Response) error {
	var j struct {
		Error string `json:"error"`
	}
	body, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(body, &j)
	msg := j.Error
	if msg == "" {
		msg = string(body)
	}
	return &APIError{Status: resp.StatusCode, Msg: msg}
}

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
