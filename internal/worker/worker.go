package worker

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/chtzvt/ctsnarf/internal/cluster"
	"github.com/chtzvt/ctsnarf/internal/etl"
	"github.com/chtzvt/ctsnarf/internal/job"
	ct "github.com/google/certificate-transparency-go"
	"github.com/google/certificate-transparency-go/client"
	"github.com/google/certificate-transparency-go/jsonclient"
	"github.com/google/certificate-transparency-go/scanner"
)

// Worker supervises concurrent processing of shards for a job.
type Worker struct {
	ID          string
	Cluster     cluster.Cluster
	JobID       string
	MaxParallel int
	BatchSize   int
	PollPeriod  time.Duration
	LeaseSecs   int
	Logger      *log.Logger
	Metrics     *WorkerMetrics

	stopCh  chan struct{}
	stopped chan struct{}
	wg      sync.WaitGroup

	mainLoopErrorCount int64
	mainLoopBackoff    time.Duration
}

const (
	mainLoopErrorThreshold = 3
	maxMainLoopBackoff     = 30 * time.Second
)

// NewWorker constructs a worker with reasonable defaults.
func NewWorker(cluster cluster.Cluster, jobID, id string, logger *log.Logger) *Worker {
	return &Worker{
		ID:          id,
		Cluster:     cluster,
		JobID:       jobID,
		MaxParallel: 4, // or configurable
		BatchSize:   8,
		PollPeriod:  1 * time.Second,
		LeaseSecs:   60,
		Logger:      logger,
		stopCh:      make(chan struct{}),
		stopped:     make(chan struct{}),
		Metrics:     &WorkerMetrics{},
	}
}

// Run is the worker's main supervisory loop. Returns on stop/cancel.
func (w *Worker) Run(ctx context.Context) error {
	defer close(w.stopped)

	_, err := w.Cluster.RegisterWorker(ctx, cluster.WorkerInfo{ID: w.ID, Host: "localhost"})
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
			// --- Regular Logic ---
			cancelled, err := w.checkJobCancelled(ctx)
			lastErr = err
			if err != nil {
				w.Logger.Printf("worker: error checking job cancelled: %v", err)
				continue
			}
			if cancelled {
				w.Logger.Println("worker: job cancelled")
				return nil
			}
			claimable := w.findClaimableShards(ctx, w.BatchSize)
			lastErr = nil // Only set lastErr if there was a "hard" error above
			if len(claimable) == 0 {
				time.Sleep(w.PollPeriod)
				continue
			}
			for _, shardID := range claimable {
				sem <- struct{}{}
				w.wg.Add(1)
				go func(shardID int) {
					defer func() { <-sem; w.wg.Done() }()
					w.processShardLoop(ctx, shardID)
				}(shardID)
			}
			time.Sleep(w.PollPeriod)
		}
	}
}

// Stop signals the worker to exit gracefully.
func (w *Worker) Stop() {
	select {
	case <-w.stopCh:
		// already closed
	default:
		close(w.stopCh)
	}
	<-w.stopped // Wait for Run to exit
}

// Heartbeat keeps worker lease alive in etcd.
func (w *Worker) heartbeat(ctx context.Context) {
	if err := w.Cluster.HeartbeatWorker(ctx, w.ID); err != nil {
		w.Logger.Printf("heartbeat failed: %v", err)
	}
}

// Check for job cancellation (set by CancelJob).
func (w *Worker) checkJobCancelled(ctx context.Context) (bool, error) {
	status, err := w.Cluster.IsJobCancelled(ctx, w.JobID)
	if err != nil {
		return false, err
	}

	return status, nil
}

// findClaimableShards returns up to batchSize shards ready for processing.
func (w *Worker) findClaimableShards(ctx context.Context, batchSize int) []int {
	assignments, err := w.Cluster.GetShardAssignments(ctx, w.JobID)
	if err != nil {
		w.Logger.Printf("error listing assignments: %v", err)
		return nil
	}
	now := time.Now()
	claimable := make([]int, 0, batchSize)
	for shardID, stat := range assignments {
		if !stat.Assigned && !stat.Done && !stat.Failed &&
			(stat.BackoffUntil.IsZero() || now.After(stat.BackoffUntil)) {
			claimable = append(claimable, shardID)
			if len(claimable) >= batchSize {
				break
			}
		}
	}
	return claimable
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

func (w *Worker) processShardLoop(ctx context.Context, shardID int) {
	start := time.Now()
	defer func() {
		w.Metrics.AddProcessingTime(time.Since(start))
	}()

	if err := w.Cluster.AssignShard(ctx, w.JobID, shardID, w.ID); err != nil {
		w.Logger.Printf("assign failed: %v", err)
		return
	}
	status, err := w.Cluster.GetShardStatus(ctx, w.JobID, shardID)
	if err != nil {
		w.Logger.Printf("get shard status failed: %v", err)
		_ = w.Cluster.ReportShardFailed(ctx, w.JobID, shardID)
		w.Metrics.IncFailed()
		return
	}
	job, err := w.Cluster.GetJob(ctx, w.JobID)
	if err != nil {
		w.Logger.Printf("failed to get job spec: %v", err)
		_ = w.Cluster.ReportShardFailed(ctx, w.JobID, shardID)
		w.Metrics.IncFailed()
		return
	}

	// Create ETL pipeline for this shard
	pipeline, err := etl.NewPipeline(job.Spec, w.Cluster.Secrets(), fmt.Sprintf("job-%s-shard-%d", w.JobID, shardID))
	if err != nil {
		w.Logger.Printf("etl pipeline init failed: %v", err)
		_ = w.Cluster.ReportShardFailed(ctx, w.JobID, shardID)
		w.Metrics.IncFailed()
		return
	}

	// Set up a channel and run the ETL pipeline as a goroutine
	entries := make(chan *ct.RawLogEntry, 32)
	etlErrCh := make(chan error, 1)
	go func() {
		etlErrCh <- pipeline.StreamProcess(ctx, entries)
	}()

	// Stream the entries directly from the scanner into the channel
	scanErr := w.StreamShard(ctx, *job.Spec, status.IndexFrom, status.IndexTo, entries)
	etlErr := <-etlErrCh

	if scanErr != nil {
		w.Logger.Printf("scanner failed: %v", scanErr)
		_ = w.Cluster.ReportShardFailed(ctx, w.JobID, shardID)
		w.Metrics.IncFailed()
		return
	}
	if etlErr != nil {
		w.Logger.Printf("etl process failed: %v", etlErr)
		_ = w.Cluster.ReportShardFailed(ctx, w.JobID, shardID)
		w.Metrics.IncFailed()
		return
	}

	manifest := cluster.ShardManifest{}
	if err := w.Cluster.ReportShardDone(ctx, w.JobID, shardID, manifest); err != nil {
		w.Logger.Printf("report done failed: %v", err)
		w.Metrics.IncFailed()
		return
	}
	w.Metrics.IncProcessed()
	w.Logger.Printf("shard %d completed", shardID)
}
