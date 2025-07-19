package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
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

	// Everything else (subresources)
	mux.HandleFunc("/api/jobs/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/api/jobs/")
		parts := strings.Split(strings.Trim(path, "/"), "/")
		id := parts[0]
		if id == "" {
			jsonError(w, http.StatusBadRequest, "missing job id")
			return
		}

		// PATCH /api/jobs/{id}/status
		if len(parts) == 2 && parts[1] == "status" && r.Method == "PATCH" {
			handleUpdateJobStatus(w, r, cl, id)
			return
		}

		// POST /api/jobs/{id}/start, /complete, /cancel
		if len(parts) == 2 && r.Method == "POST" {
			switch parts[1] {
			case "start":
				handleMarkJobStarted(w, r, cl, id)
				return
			case "complete":
				handleMarkJobCompleted(w, r, cl, id)
				return
			case "cancel":
				handleCancelJob(w, r, cl, id)
				return
			}
		}

		// SHARDS: /api/jobs/{id}/shards or /api/jobs/{id}/shards/{shardId}
		if len(parts) >= 2 && parts[1] == "shards" {
			if r.Method == "GET" {
				if len(parts) == 2 {
					handleGetShardAssignments(w, r, cl, id) // Windowing support inside handler
					return
				}
				if len(parts) == 3 {
					handleGetShardStatus(w, r, cl, id, parts[2])
					return
				}
			}

			if r.Method == "POST" {
				if len(parts) == 3 && parts[2] == "reset-failed" {
					handleResetFailedShards(w, r, cl, id)
					return
				}
				if len(parts) == 4 && parts[1] == "shards" && parts[3] == "reset-failed" {
					handleResetFailedShard(w, r, cl, id, parts[2])
					return
				}
			}

			jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		// Default: GET /api/jobs/{id}
		if r.Method == "GET" && len(parts) == 1 {
			handleGetJob(w, r, cl)
			return
		}

		jsonError(w, http.StatusNotFound, "unknown endpoint")
	})
}

// --- handlers ---

func handleUpdateJobStatus(w http.ResponseWriter, r *http.Request, cl cluster.Cluster, id string) {
	var req struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "invalid body")
		return
	}
	err := cl.UpdateJobStatus(r.Context(), id, cluster.JobState(req.Status))
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to update job: "+err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func handleMarkJobStarted(w http.ResponseWriter, r *http.Request, cl cluster.Cluster, id string) {
	if err := cl.MarkJobStarted(r.Context(), id); err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func handleMarkJobCompleted(w http.ResponseWriter, r *http.Request, cl cluster.Cluster, id string) {
	if err := cl.MarkJobCompleted(r.Context(), id); err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func handleCancelJob(w http.ResponseWriter, r *http.Request, cl cluster.Cluster, id string) {
	if err := cl.CancelJob(r.Context(), id); err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func handleGetShardAssignments(w http.ResponseWriter, r *http.Request, cl cluster.Cluster, jobID string) {
	q := r.URL.Query()
	var (
		start, end int
		hasStart   = false
		hasEnd     = false
	)
	if s := q.Get("start"); s != "" {
		start, _ = strconv.Atoi(s)
		hasStart = true
	}
	if e := q.Get("end"); e != "" {
		end, _ = strconv.Atoi(e)
		hasEnd = true
	}
	var (
		assignments map[int]cluster.ShardAssignmentStatus
		err         error
	)
	if hasStart || hasEnd {
		assignments, err = cl.GetShardAssignmentsWindow(r.Context(), jobID, start, end)
	} else {
		assignments, err = cl.GetShardAssignments(r.Context(), jobID)
	}
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(assignments)
}

func handleGetShardStatus(w http.ResponseWriter, r *http.Request, cl cluster.Cluster, jobID, shardIDStr string) {
	shardID, err := strconv.Atoi(shardIDStr)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "invalid shard id")
		return
	}
	status, err := cl.GetShardStatus(r.Context(), jobID, shardID)
	if err != nil {
		jsonError(w, http.StatusNotFound, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
}

func handleResetFailedShards(w http.ResponseWriter, r *http.Request, cl cluster.Cluster, jobID string) {
	shards, err := cl.ResetFailedShards(r.Context(), jobID)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to reset failed shards: "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"reset_shards": shards,
	})
}

func handleResetFailedShard(w http.ResponseWriter, r *http.Request, cl cluster.Cluster, jobID, shardIDStr string) {
	shardID, err := strconv.Atoi(shardIDStr)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "invalid shard id")
		return
	}
	if err := cl.ResetFailedShard(r.Context(), jobID, shardID); err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to reset failed shard: "+err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
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

	// If IndexEnd is zero, fetch from CT log (requires network)
	start := spec.Options.Fetch.IndexStart
	end := spec.Options.Fetch.IndexEnd
	if end == 0 {
		treeSize, err := fetchCTLogTreeSize(spec.LogURI)
		if err != nil {
			jsonError(w, http.StatusBadRequest, "could not determine end index: "+err.Error())
			return
		}
		end = treeSize
		spec.Options.Fetch.IndexEnd = treeSize
	}

	shardSize := spec.Options.Fetch.ShardSize
	if shardSize == 0 {
		shardSize = autoShardSize(start, end)
	}

	// Create the shards
	ranges := makeShardRanges(start, end, shardSize)

	ctx := r.Context()
	jobID, err := cl.SubmitJob(ctx, &spec)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to submit job: "+err.Error())
		return
	}

	if len(ranges) == 0 {
		jsonError(w, http.StatusBadRequest, "no shards would be created with provided indices/shard size")
		return
	}
	if err := cl.BulkCreateShards(ctx, jobID, ranges); err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to create shards: "+err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]string{"job_id": jobID})
}

// --- Helpers ---

func fetchCTLogTreeSize(logURI string) (int64, error) {
	// Try to transform logURI if necessary (handle trailing slashes etc)
	base := strings.TrimRight(logURI, "/")
	resp, err := http.Get(base + "/ct/v1/get-sth")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("ct log get-sth failed: %d", resp.StatusCode)
	}
	var sth struct {
		TreeSize int64 `json:"tree_size"`
	}
	dec := json.NewDecoder(io.LimitReader(resp.Body, 65536))
	if err := dec.Decode(&sth); err != nil {
		return 0, err
	}
	return sth.TreeSize, nil
}

func autoShardSize(start, end int64) int {
	size := end - start
	switch {
	case size >= 1_000_000_000:
		return 10_000_000
	case size >= 100_000_000:
		return 1_000_000
	case size >= 10_000_000:
		return 500_000
	case size >= 1_000_000:
		return 100_000
	case size >= 100_000:
		return 10_000
	case size >= 10_000:
		return 1_000
	case size >= 1_000:
		return 500
	default:
		return 100
	}
}

func makeShardRanges(start, end int64, shardSize int) []cluster.ShardRange {
	var ranges []cluster.ShardRange
	for i, from := 0, start; from < end; i++ {
		to := from + int64(shardSize)
		if to > end {
			to = end
		}
		ranges = append(ranges, cluster.ShardRange{
			ShardID:   i,
			IndexFrom: from,
			IndexTo:   to,
		})
		from = to
	}
	return ranges
}
