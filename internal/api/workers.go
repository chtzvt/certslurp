// api/workers.go
package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/chtzvt/certslurp/internal/cluster"
)

type WorkerStatus struct {
	ID               string    `json:"id"`
	Host             string    `json:"host"`
	LastSeen         time.Time `json:"last_seen"`
	ShardsProcessed  int64     `json:"shards_processed"`
	ShardsFailed     int64     `json:"shards_failed"`
	ProcessingTimeNs int64     `json:"processing_time_ns"`
	LastUpdated      time.Time `json:"last_updated"`
}

func RegisterWorkerHandlers(mux *http.ServeMux, cl cluster.Cluster) {
	// List all worker metrics
	mux.HandleFunc("/api/workers", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		workers, err := cl.ListWorkers(r.Context())
		if err != nil {
			jsonError(w, http.StatusInternalServerError, "failed to list workers: "+err.Error())
			return
		}
		// Combine with metrics
		statuses := make([]*WorkerStatus, 0, len(workers))
		for _, wi := range workers {
			ws := &WorkerStatus{
				ID:       wi.ID,
				Host:     wi.Host,
				LastSeen: wi.LastSeen,
			}
			// Try to get metrics, but tolerate absence
			if vm, err := cl.GetWorkerMetrics(r.Context(), wi.ID); err == nil && vm != nil {
				ws.ShardsProcessed = vm.ShardsProcessed
				ws.ShardsFailed = vm.ShardsFailed
				ws.ProcessingTimeNs = vm.ProcessingTimeNs
				ws.LastUpdated = vm.LastUpdated
			}
			statuses = append(statuses, ws)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(statuses)
	})

	// Get metrics for specific worker
	mux.HandleFunc("/api/workers/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/api/workers/")
		if id == "" {
			jsonError(w, http.StatusBadRequest, "missing worker id")
			return
		}
		vm, err := cl.GetWorkerMetrics(r.Context(), id)
		if err != nil || vm == nil {
			jsonError(w, http.StatusNotFound, "not found: "+id)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(vm)
	})
}
