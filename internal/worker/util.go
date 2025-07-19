package worker

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/chtzvt/certslurp/internal/cluster"
	"github.com/chtzvt/certslurp/internal/job"
)

func (w *Worker) jitterDuration() time.Duration {
	if w.DisableJitterAndSmoothingForTests {
		return 0 * time.Second
	}

	min := 100 * time.Millisecond
	max := 3 * time.Second

	return min + time.Duration(rand.Int63n(int64(max-min)))
}

func (w *Worker) maybeSleep() {
	if w.DisableJitterAndSmoothingForTests {
		return
	}

	if rand.Float64() < 0.05 { // 5% of the time
		time.Sleep(w.jitterDuration() + 1*time.Second)
	}
}

func httpTransportForShard(cfg job.FetchConfig) (*http.Transport, time.Duration) {
	entries := cfg.IndexEnd - cfg.IndexStart
	if entries < 0 {
		entries = 0
	}
	reqs := 1
	if cfg.FetchSize > 0 && entries > 0 {
		reqs = int(math.Ceil(float64(entries) / float64(cfg.FetchSize)))
	}

	const (
		perRequestTimeout        = 20 * time.Second
		minTimeout               = 2 * time.Minute
		maxTimeout               = 10 * time.Minute
		minIdleConns             = 4
		maxIdleConns             = 100
		minResponseHeaderTimeout = 20 * time.Second
		maxResponseHeaderTimeout = 60 * time.Second
	)

	timeout := time.Duration(reqs) * perRequestTimeout
	if timeout < minTimeout {
		timeout = minTimeout
	}
	if timeout > maxTimeout {
		timeout = maxTimeout
	}

	idleConns := cfg.FetchWorkers * 2
	if idleConns < minIdleConns {
		idleConns = minIdleConns
	}
	if idleConns > maxIdleConns {
		idleConns = maxIdleConns
	}

	// Dynamically set response header timeout based on fetch size.
	rhTimeout := minResponseHeaderTimeout
	if cfg.FetchSize > 512 {
		rhTimeout = maxResponseHeaderTimeout
	}

	transport := &http.Transport{
		TLSHandshakeTimeout:   30 * time.Second,
		ResponseHeaderTimeout: rhTimeout,
		MaxIdleConnsPerHost:   idleConns,
		MaxIdleConns:          idleConns,
		IdleConnTimeout:       90 * time.Second,
		DisableKeepAlives:     false,
		ExpectContinueTimeout: 1 * time.Second,
	}

	return transport, timeout
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	base := w.jitterDuration() + 10*time.Second

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case <-time.After(base + w.jitterDuration()):
			w.maybeSleep()
			if err := w.Cluster.HeartbeatWorker(ctx, w.ID); err != nil {
				w.Logger.Printf("heartbeat failed: %v", err)
			}
		}
	}
}

func (w *Worker) metricsLoop(ctx context.Context) {
	base := w.jitterDuration() + 10*time.Second

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case <-time.After(base + w.jitterDuration()):
			w.maybeSleep()
			if err := w.Cluster.SendMetrics(ctx, w.ID, w.Metrics); err != nil {
				w.Logger.Printf("SendMetrics failed: %v", err)
			}
		}
	}
}

// Check for job cancellation (set by CancelJob).
func (w *Worker) checkJobCancelled(ctx context.Context, jobID string) (bool, error) {
	status, err := w.Cluster.IsJobCancelled(ctx, jobID)
	if err != nil {
		return false, err
	}
	return status, nil
}

// findAllClaimableShards returns up to batchSize claimable shards across all jobs.
func (w *Worker) findAllClaimableShards(ctx context.Context, batchSize int) []ShardRef {
	w.maybeSleep()
	jobs, err := w.Cluster.ListJobs(ctx)
	if err != nil {
		w.Logger.Printf("error listing jobs: %v", err)
		return nil
	}
	now := time.Now()
	claimable := make([]ShardRef, 0, batchSize)
	const windowSize = 128
	const maxEmptyWindows = 8

	randShuffle := func(refs []ShardRef) []ShardRef {
		rand.Shuffle(len(refs), func(i, j int) {
			refs[i], refs[j] = refs[j], refs[i]
		})
		return refs
	}

	for _, job := range jobs {
		w.maybeSleep()
		shardCount, err := w.Cluster.GetShardCount(ctx, job.ID)
		if err != nil || shardCount == 0 {
			continue
		}
		emptyWindows := 0
		checked := map[int]struct{}{}
		lastWindowScanned := false

		for {
			// Fallback: scan ALL
			if shardCount < windowSize || emptyWindows >= maxEmptyWindows {
				w.maybeSleep()
				window, err := w.Cluster.GetShardAssignmentsWindow(ctx, job.ID, 0, shardCount)
				if len(claimable) < batchSize {
					var stuck []int
					for sID, stat := range window {
						if !stat.Done && !stat.Failed && !stat.Assigned && (stat.BackoffUntil.IsZero() || now.After(stat.BackoffUntil)) {
							stuck = append(stuck, sID)
						}
					}
				}
				if err != nil {
					break
				}
				for sID, stat := range window {
					if _, alreadyChecked := checked[sID]; !alreadyChecked && !stat.Assigned && !stat.Done && !stat.Failed &&
						(stat.BackoffUntil.IsZero() || now.After(stat.BackoffUntil)) {
						claimable = append(claimable, ShardRef{JobID: job.ID, ShardID: sID})
						if len(claimable) >= batchSize {
							return randShuffle(claimable)
						}
					}
				}
				break
			}

			// Standard random window
			offset := rand.Intn(shardCount - windowSize + 1)
			w.maybeSleep()
			window, err := w.Cluster.GetShardAssignmentsWindow(ctx, job.ID, offset, offset+windowSize)
			if err != nil {
				break
			}
			found := false
			for sID, stat := range window {
				checked[sID] = struct{}{}
				if !stat.Assigned && !stat.Done && !stat.Failed &&
					(stat.BackoffUntil.IsZero() || now.After(stat.BackoffUntil)) {
					claimable = append(claimable, ShardRef{JobID: job.ID, ShardID: sID})
					if len(claimable) >= batchSize {
						return randShuffle(claimable)
					}
					found = true
				}
			}
			if found {
				break
			}
			emptyWindows++

			// Ensure we always explicitly check the final window at least once
			if !lastWindowScanned && shardCount > windowSize {
				lastWindowScanned = true
				offset := shardCount - windowSize
				w.maybeSleep()
				window, err := w.Cluster.GetShardAssignmentsWindow(ctx, job.ID, offset, shardCount)
				if err == nil {
					for sID, stat := range window {
						checked[sID] = struct{}{}
						if !stat.Assigned && !stat.Done && !stat.Failed &&
							(stat.BackoffUntil.IsZero() || now.After(stat.BackoffUntil)) {
							claimable = append(claimable, ShardRef{JobID: job.ID, ShardID: sID})
							if len(claimable) >= batchSize {
								return randShuffle(claimable)
							}
							found = true
						}
					}
					if found {
						break
					}
				}
			}
		}
	}

	return randShuffle(claimable)
}

// tryAssignShardWithRetry tries to assign a shard with retries on race/assignment contention.
func (w *Worker) tryAssignShardWithRetry(ctx context.Context, jobID string, shardID int) error {
	var lastErr error
	for attempt := 1; attempt <= maxAssignShardRetries; attempt++ {
		err := w.Cluster.AssignShard(ctx, jobID, shardID, w.ID)
		if err == nil {
			return nil
		}

		// Recognize assignment-race or already assigned errors
		msg := err.Error()
		if strings.Contains(msg, "assignment race") ||
			strings.Contains(msg, "already assigned") ||
			strings.Contains(msg, "in backoff") {
			backoff := w.PollPeriod + w.jitterDuration()
			time.Sleep(backoff)
			lastErr = err
			continue
		}
		// Any other error: break and return immediately
		return err
	}
	return fmt.Errorf("failed to assign shard %d (job %s) after %d retries: last error: %v", shardID, jobID, maxAssignShardRetries, lastErr)
}

// baseNameForPipeline returns a normalized name for the data output by this shard's ETL pipeline, in
// the format <log url>.<log index range>.<job uuid>.<shard id>
// Example: mysite_domain_com__some__path.0_1000000.17E28132-8B25-4FB2-99C5-89938D4D3D24.1
func baseNameForPipeline(spec *job.JobSpec, shardStatus cluster.ShardStatus, jobID string, shardID int) string {
	logUrl, err := normalizeURL(spec.LogURI)
	if err != nil {
		logUrl = jobID
	}

	shardRange := fmt.Sprintf("%d_%d", shardStatus.IndexFrom, shardStatus.IndexTo)

	return strings.ToLower(fmt.Sprintf("%s.%s.%s.%d", logUrl, shardRange, jobID, shardID))
}

func normalizeURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("invalid URL: %w", err)
	}

	host := parsed.Hostname()
	if host == "" {
		return "", fmt.Errorf("invalid URL: no hostname found")
	}

	path := strings.Trim(parsed.EscapedPath(), "/")

	host = strings.ReplaceAll(host, ".", "_")
	path = strings.ReplaceAll(path, "/", "__")

	if path != "" {
		return host + "__" + path, nil
	}
	return host, nil
}
