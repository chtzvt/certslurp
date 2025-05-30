package api

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/chtzvt/certslurp/internal/cluster"
)

// ListWorkers returns all registered workers and their metrics.
func (c *Client) ListWorkers(ctx context.Context) ([]WorkerStatus, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.BaseURL+"/api/workers", nil)
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
	var workers []WorkerStatus
	if err := json.NewDecoder(resp.Body).Decode(&workers); err != nil {
		return nil, err
	}
	return workers, nil
}

// GetWorkerMetrics fetches metrics for a worker by ID.
func (c *Client) GetWorkerMetrics(ctx context.Context, workerID string) (*cluster.WorkerMetricsView, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.BaseURL+"/api/workers/"+workerID, nil)
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
	var metrics cluster.WorkerMetricsView
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		return nil, err
	}
	return &metrics, nil
}
