package api

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/chtzvt/certslurp/internal/cluster"
)

// RegisterSecretHandlers wires secret & admin node endpoints into the given mux.
func RegisterSecretHandlers(mux *http.ServeMux, cl cluster.Cluster) {
	// List pending nodes (admin)
	mux.HandleFunc("/api/secrets/nodes/pending", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		handleListPendingNodes(w, r, cl)
	})
	// Approve pending node (admin)
	mux.HandleFunc("/api/secrets/nodes/approve", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		handleApproveNode(w, r, cl)
	})

	// /api/secrets/store (list keys)
	mux.HandleFunc("/api/secrets/store", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		handleListSecretKeys(w, r, cl)
	})

	// /api/secrets/store/{key} for GET, PUT, DELETE
	mux.HandleFunc("/api/secrets/store/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/api/secrets/store/")
		if key == "" {
			jsonError(w, http.StatusBadRequest, "missing secret key")
			return
		}
		switch r.Method {
		case "GET":
			handleGetSecret(w, r, cl, key)
		case "PUT":
			handlePutSecret(w, r, cl, key)
		case "DELETE":
			handleDeleteSecret(w, r, cl, key)
		default:
			jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func handleListPendingNodes(w http.ResponseWriter, r *http.Request, cl cluster.Cluster) {
	nodes, err := cl.Secrets().ListPendingRegistrations(r.Context())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "error listing pending registrations: "+err.Error())
		return
	}
	type outNode struct {
		NodeID    string `json:"node_id"`
		PublicKey string `json:"public_key"`
	}
	result := make([]outNode, 0, len(nodes))
	for _, n := range nodes {
		result = append(result, outNode{NodeID: n.NodeID, PublicKey: n.PubKeyB64})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func handleApproveNode(w http.ResponseWriter, r *http.Request, cl cluster.Cluster) {
	var req struct {
		NodeID string `json:"node_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "invalid request: "+err.Error())
		return
	}

	if err := cl.Secrets().ApproveNode(r.Context(), req.NodeID); err != nil {
		jsonError(w, http.StatusBadRequest, "could not approve node: "+err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func handleListSecretKeys(w http.ResponseWriter, r *http.Request, cl cluster.Cluster) {
	prefix := r.URL.Query().Get("prefix")
	keys, err := cl.Secrets().List(r.Context(), prefix)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "error listing secrets: "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(keys)
}

func handleGetSecret(w http.ResponseWriter, r *http.Request, cl cluster.Cluster, key string) {
	etcdKey := cl.Secrets().Prefix() + "/secrets/store/" + key
	resp, err := cl.Client().Get(r.Context(), etcdKey)
	if err != nil || len(resp.Kvs) == 0 {
		jsonError(w, http.StatusNotFound, "not found")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"value": string(resp.Kvs[0].Value)})
}

func handlePutSecret(w http.ResponseWriter, r *http.Request, cl cluster.Cluster, key string) {
	var value []byte
	ct := r.Header.Get("Content-Type")
	if strings.HasPrefix(ct, "application/json") {
		var body struct{ Value string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			jsonError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
			return
		}
		val, err := base64.StdEncoding.DecodeString(body.Value)
		if err != nil {
			jsonError(w, http.StatusBadRequest, "invalid base64 value")
			return
		}
		value = val
	} else {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			jsonError(w, http.StatusBadRequest, "could not read body")
			return
		}
		value, err = base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
		if err != nil {
			jsonError(w, http.StatusBadRequest, "invalid base64 value")
			return
		}
	}
	if err := cl.Secrets().SetSealed(r.Context(), key, value); err != nil {
		jsonError(w, http.StatusInternalServerError, "set failed: "+err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func handleDeleteSecret(w http.ResponseWriter, r *http.Request, cl cluster.Cluster, key string) {
	if err := cl.Secrets().Delete(r.Context(), key); err != nil {
		jsonError(w, http.StatusInternalServerError, "delete failed: "+err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
