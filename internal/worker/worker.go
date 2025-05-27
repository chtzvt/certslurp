package worker

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/chtzvt/certslurp/internal/cluster"
	"github.com/chtzvt/certslurp/internal/etl"
	"github.com/chtzvt/certslurp/internal/job"
	ct "github.com/google/certificate-transparency-go"
	"github.com/google/certificate-transparency-go/client"
	"github.com/google/certificate-transparency-go/jsonclient"
	"github.com/google/certificate-transparency-go/scanner"
)

// Worker supervises concurrent processing of shards for a job.
type Worker struct {
	ID          string
	Cluster     cluster.Cluster
	MaxParallel int
	BatchSize   int
	PollPeriod  time.Duration
	LeaseSecs   int
	Logger      *log.Logger
	Metrics     *cluster.WorkerMetrics

	stopCh  chan struct{}
	stopped chan struct{}
	wg      sync.WaitGroup

	mainLoopErrorCount int64
	mainLoopBackoff    time.Duration
}

const (
	mainLoopErrorThreshold = 3
	maxMainLoopBackoff     = 30 * time.Second
	maxAssignShardRetries  = 5
)

func NewWorker(cl cluster.Cluster, id string, logger *log.Logger) *Worker {
	return &Worker{
		ID:          id,
		Cluster:     cl,
		MaxParallel: 4, // configurable
		BatchSize:   8,
		PollPeriod:  1 * time.Second,
		LeaseSecs:   60,
		Logger:      logger,
		stopCh:      make(chan struct{}),
		stopped:     make(chan struct{}),
		Metrics:     &cluster.WorkerMetrics{},
	}
}

// Run is the worker's main supervisory loop. Returns on stop/cancel.
func (w *Worker) Run(ctx context.Context) error {
	defer close(w.stopped)

	hostName, err := os.Hostname()
	if err != nil {
		hostName = "unknown.host"
	}

	_, err = w.Cluster.RegisterWorker(ctx, cluster.WorkerInfo{ID: w.ID, Host: hostName})
	if err != nil {
		return err
	}

	var lastErr error
	heartbeatTicker := time.NewTicker(5 * time.Second)
	defer heartbeatTicker.Stop()

	sem := make(chan struct{}, w.MaxParallel)
	for {
		select {
		case <-ctx.Done():
			w.Logger.Println("worker: context cancelled")
			w.wg.Wait()
			return ctx.Err()
		case <-w.stopCh:
			w.Logger.Println("worker: stop requested")
			w.wg.Wait()
			return nil
		case <-heartbeatTicker.C:
			w.heartbeat(ctx)
		default:
			// --- Main Loop Error Handling ---
			if lastErr != nil {
				w.mainLoopErrorCount++
				if w.mainLoopErrorCount >= mainLoopErrorThreshold {
					if w.mainLoopBackoff < maxMainLoopBackoff {
						w.mainLoopBackoff = 2 * w.mainLoopBackoff
						if w.mainLoopBackoff == 0 {
							w.mainLoopBackoff = 1 * time.Second
						}
					}
					w.Logger.Printf("worker: backing off for %s due to repeated errors", w.mainLoopBackoff)
					time.Sleep(w.mainLoopBackoff)
				}
			} else {
				w.mainLoopErrorCount = 0
				w.mainLoopBackoff = 0
			}

			// --- Find and attempt to assign multiple claimable shards ---
			claimable := w.findAllClaimableShards(ctx, w.BatchSize)
			lastErr = nil
			if len(claimable) == 0 {
				time.Sleep(w.PollPeriod)
				continue
			}
			for _, ref := range claimable {
				sem <- struct{}{}
				w.wg.Add(1)
				go func(jobID string, shardID int) {
					defer func() { <-sem; w.wg.Done() }()
					// Attempt to assign the shard before processing
					err := w.tryAssignShardWithRetry(ctx, jobID, shardID)
					if err != nil {
						w.Logger.Printf("assign failed: shard %d (job %s): %v", shardID, jobID, err)
						return
					}
					w.processShardLoop(ctx, jobID, shardID)
				}(ref.JobID, ref.ShardID)
			}
			// Only wait poll period after all launches, to avoid hammering etcd
			time.Sleep(w.PollPeriod)
		}
	}
}

// Stop signals the worker to exit gracefully.
func (w *Worker) Stop() {
	select {
	case <-w.stopCh:
	default:
		close(w.stopCh)
	}
	<-w.stopped
}

// Heartbeat keeps worker lease alive in etcd.
func (w *Worker) heartbeat(ctx context.Context) {
	if err := w.Cluster.HeartbeatWorker(ctx, w.ID); err != nil {
		w.Logger.Printf("heartbeat failed: %v", err)
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

type ShardRef struct {
	JobID   string
	ShardID int
}

// findAllClaimableShards returns up to batchSize claimable shards across all jobs.
func (w *Worker) findAllClaimableShards(ctx context.Context, batchSize int) []ShardRef {
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
			backoff := time.Duration(50+rand.Intn(150)) * time.Millisecond
			time.Sleep(backoff)
			lastErr = err
			continue
		}
		// Any other error: break and return immediately
		return err
	}
	return fmt.Errorf("failed to assign shard %d (job %s) after %d retries: last error: %v", shardID, jobID, maxAssignShardRetries, lastErr)
}

// StreamShard streams log entries for the given shard range directly into the provided channel.
// Closes the channel when done or on error.
func (w *Worker) StreamShard(ctx context.Context, jobSpec job.JobSpec, from, to int64, ch chan<- *ct.RawLogEntry) error {
	matchCfg := jobSpec.Options.Match
	fetchCfg := jobSpec.Options.Fetch

	matcher, matcherInit := buildMatcher(matchCfg)
	opts := scanner.ScannerOptions{
		FetcherOptions: scanner.FetcherOptions{
			BatchSize:     fetchCfg.BatchSize,
			ParallelFetch: fetchCfg.Workers,
			StartIndex:    from,
			EndIndex:      to,
		},
		Matcher:     matcher,
		PrecertOnly: matchCfg.PrecertsOnly,
		NumWorkers:  w.MaxParallel,
	}
	if matchCfg.Workers > 0 {
		opts.NumWorkers = matchCfg.Workers
	}

	logClient, err := client.New(jobSpec.LogURI, &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSHandshakeTimeout:   30 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			MaxIdleConnsPerHost:   10,
			DisableKeepAlives:     false,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}, jsonclient.Options{UserAgent: "certslurp/1.0"})
	if err != nil {
		close(ch)
		return fmt.Errorf("failed to create log client: %w", err)
	}
	if matcherInit != nil {
		if err := matcherInit(ctx, logClient); err != nil {
			close(ch)
			return err
		}
	}

	s := scanner.NewScanner(logClient, opts)
	// Send entries to channel as they are found
	collect := func(entry *ct.RawLogEntry) {
		select {
		case ch <- entry:
		case <-ctx.Done():
		}
	}
	err = s.Scan(ctx, collect, collect)
	close(ch)
	return err
}

func (w *Worker) processShardLoop(ctx context.Context, jobID string, shardID int) {
	start := time.Now()
	var shardReported bool // track if we've reported Done/Failed
	defer func() {
		// Recover from panics to avoid leaving shard stuck
		if r := recover(); r != nil {
			w.Logger.Printf("panic in shard processing: %v", r)
			_ = w.Cluster.ReportShardFailed(context.Background(), jobID, shardID)
			w.Metrics.IncFailed()
			shardReported = true
		}
		// If not reported, mark as failed (covers context cancel or other silent exit)
		if !shardReported {
			_ = w.Cluster.ReportShardFailed(context.Background(), jobID, shardID)
			w.Metrics.IncFailed()
		}
		w.Metrics.AddProcessingTime(time.Since(start))
	}()

	status, err := w.Cluster.GetShardStatus(ctx, jobID, shardID)
	if err != nil {
		w.Logger.Printf("get shard status failed: %v", err)
		return
	}
	jobInfo, err := w.Cluster.GetJob(ctx, jobID)
	if err != nil {
		w.Logger.Printf("failed to get job spec: %v", err)
		return
	}

	cancelled, err := w.checkJobCancelled(ctx, jobID)
	if err != nil {
		w.Logger.Printf("job cancelled check failed: %v", err)
		return
	}
	if cancelled {
		w.Logger.Printf("job %s cancelled, skipping shard %d", jobID, shardID)
		return
	}

	pipeline, err := etl.NewPipeline(jobInfo.Spec, w.Cluster.Secrets(), fmt.Sprintf("job-%s-shard-%d", jobID, shardID))
	if err != nil {
		w.Logger.Printf("etl pipeline init failed: %v", err)
		return
	}

	entries := make(chan *ct.RawLogEntry, 32)
	etlErrCh := make(chan error, 1)
	go func() {
		etlErrCh <- pipeline.StreamProcess(ctx, entries)
	}()
	scanErr := w.StreamShard(ctx, *jobInfo.Spec, status.IndexFrom, status.IndexTo, entries)
	etlErr := <-etlErrCh

	// Check if context was cancelled during work (e.g., test/shutdown/compaction)
	if ctx.Err() != nil {
		w.Logger.Printf("context cancelled during shard processing: %v", ctx.Err())
		return
	}

	if scanErr != nil {
		w.Logger.Printf("scanner failed: %v", scanErr)
		return
	}
	if etlErr != nil {
		w.Logger.Printf("etl process failed: %v", etlErr)
		return
	}

	manifest := cluster.ShardManifest{}
	if err := w.Cluster.ReportShardDone(ctx, jobID, shardID, manifest); err != nil {
		w.Logger.Printf("report done failed: %v", err)
		return
	}
	w.Metrics.IncProcessed()
	w.Logger.Printf("shard %d (job %s) completed", shardID, jobID)
	shardReported = true
}
