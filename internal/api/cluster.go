package api

import (
	"encoding/json"
	"net/http"

	"github.com/chtzvt/certslurp/internal/cluster"
)

func RegisterStatusHandler(mux *http.ServeMux, cl cluster.Cluster) {
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		status, err := cl.GetClusterStatus(r.Context())
		if err != nil {
			jsonError(w, http.StatusInternalServerError, "unable to get status: "+err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	})
}
