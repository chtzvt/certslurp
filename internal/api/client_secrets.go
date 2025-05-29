package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/chtzvt/certslurp/internal/secrets"
)

// ListPendingNodes fetches all pending worker registrations.
func (c *Client) ListPendingNodes(ctx context.Context) ([]secrets.PendingRegistration, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.BaseURL+"/api/secrets/nodes/pending", nil)
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
	// Compatible struct (JSON: node_id, public_key)
	var out []struct {
		NodeID    string `json:"node_id"`
		PublicKey string `json:"public_key"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	pending := make([]secrets.PendingRegistration, len(out))
	for i, n := range out {
		pending[i] = secrets.PendingRegistration{
			NodeID:    n.NodeID,
			PubKeyB64: n.PublicKey,
		}
	}
	return pending, nil
}

// ApproveNode approves a worker registration with the provided base64 cluster key.
func (c *Client) ApproveNode(ctx context.Context, nodeID, clusterKeyBase64 string) error {
	body := map[string]string{
		"node_id":     nodeID,
		"cluster_key": clusterKeyBase64,
	}
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, "POST", c.BaseURL+"/api/secrets/nodes/approve", bytes.NewReader(b))
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

// ListSecrets lists all secret keys in the store (optionally with prefix).
func (c *Client) ListSecrets(ctx context.Context, prefix string) ([]string, error) {
	urlStr := c.BaseURL + "/api/secrets/store"
	if prefix != "" {
		urlStr += "?prefix=" + url.QueryEscape(prefix)
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
	var keys []string
	if err := json.NewDecoder(resp.Body).Decode(&keys); err != nil {
		return nil, err
	}
	return keys, nil
}

// GetSecret fetches the *encrypted* value of the secret key (as raw bytes, not decoded).
// The returned value is the decoded base64 payload (still encrypted with secretbox).
func (c *Client) GetSecret(ctx context.Context, key string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.BaseURL+"/api/secrets/store/"+url.PathEscape(key), nil)
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
	var out struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	val, err := base64.StdEncoding.DecodeString(out.Value)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// PutSecret sets a secret. Accepts value as raw bytes; encodes to base64 and sends JSON.
// The API client (caller) is responsible for encrypting the value with the cluster key
// prior to submission
func (c *Client) PutSecret(ctx context.Context, key string, value []byte) error {
	body := map[string]string{"value": base64.StdEncoding.EncodeToString(value)}
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, "PUT", c.BaseURL+"/api/secrets/store/"+url.PathEscape(key), bytes.NewReader(b))
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

// DeleteSecret deletes a secret by key.
func (c *Client) DeleteSecret(ctx context.Context, key string) error {
	req, err := http.NewRequestWithContext(ctx, "DELETE", c.BaseURL+"/api/secrets/store/"+url.PathEscape(key), nil)
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
