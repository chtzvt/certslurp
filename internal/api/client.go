package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/chtzvt/certslurp/internal/cluster"
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

func (c *Client) GetClusterStatus(ctx context.Context) (*cluster.ClusterStatus, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.BaseURL+"/api/status", nil)
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
	var status cluster.ClusterStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}
	return &status, nil
}
