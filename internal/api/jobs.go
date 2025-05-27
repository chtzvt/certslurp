package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/chtzvt/certslurp/internal/cluster"
	"github.com/chtzvt/certslurp/internal/job"
)

// RegisterJobHandlers wires job endpoints into the given mux.
func RegisterJobHandlers(mux *http.ServeMux, cl cluster.Cluster) {
	// POST /api/jobs (submit) & GET /api/jobs (list)
	mux.HandleFunc("/api/jobs", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			handleSubmitJob(w, r, cl)
		case "GET":
			handleListJobs(w, r, cl)
		default:
			jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
	// GET /api/jobs/{id}
	mux.HandleFunc("/api/jobs/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		handleGetJob(w, r, cl)
	})
}

func handleSubmitJob(w http.ResponseWriter, r *http.Request, cl cluster.Cluster) {
	var spec job.JobSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		jsonError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if err := spec.Validate(); err != nil {
		jsonError(w, http.StatusBadRequest, "job spec invalid: "+err.Error())
		return
	}
	jobID, err := cl.SubmitJob(r.Context(), &spec)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to submit job: "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]string{"job_id": jobID})
}

func handleGetJob(w http.ResponseWriter, r *http.Request, cl cluster.Cluster) {
	id := strings.TrimPrefix(r.URL.Path, "/api/jobs/")
	if id == "" {
		jsonError(w, http.StatusBadRequest, "missing job id")
		return
	}
	jobInfo, err := cl.GetJob(r.Context(), id)
	if err != nil {
		jsonError(w, http.StatusNotFound, "not found: "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(jobInfo)
}

func handleListJobs(w http.ResponseWriter, r *http.Request, cl cluster.Cluster) {
	jobs, err := cl.ListJobs(r.Context())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to list jobs: "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(jobs)
}
