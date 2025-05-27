// api/workers.go
package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/chtzvt/certslurp/internal/cluster"
)

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
		views := make([]*cluster.WorkerMetricsView, 0, len(workers))
		for _, wi := range workers {
			vm, err := cl.GetWorkerMetrics(r.Context(), wi.ID)
			if err == nil && vm != nil {
				views = append(views, vm)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(views)
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
