package worker

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/chtzvt/ctsnarf/internal/cluster"
)

// WorkerMetrics can be wired to metrics system or just logged.
type WorkerMetrics struct {
	ShardsProcessed int64
	ShardsFailed    int64
	ProcessingTime  time.Duration
}

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
}

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
			if cancelled, _ := w.checkJobCancelled(ctx); cancelled {
				w.Logger.Println("worker: job cancelled")
				return nil
			}
			claimable := w.findClaimableShards(ctx, w.BatchSize)
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
	close(w.stopCh)
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
	status, err := w.Cluster.GetJob(ctx, w.JobID)
	if err != nil {
		return false, err
	}
	// (You may want to add a dedicated /cancelled key, as in previous recs)
	// Or use another method to check cancellation.
	return status.Cancelled, nil
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

// processShardLoop handles claim, pipeline, and report for a single shard.
func (w *Worker) processShardLoop(ctx context.Context, shardID int) {
	start := time.Now()
	defer func() {
		w.Metrics.ProcessingTime += time.Since(start)
	}()

	// Try to claim (ignore error if already claimed by another)
	if err := w.Cluster.AssignShard(ctx, w.JobID, shardID, w.ID); err != nil {
		w.Logger.Printf("assign failed: %v", err)
		return
	}
	// Get shard range/status
	status, err := w.Cluster.GetShardStatus(ctx, w.JobID, shardID)
	if err != nil {
		w.Logger.Printf("get shard status failed: %v", err)
		_ = w.Cluster.ReportShardFailed(ctx, w.JobID, shardID)
		w.Metrics.ShardsFailed++
		return
	}
	// Pipeline stubs (replace with real impl)
	entries, err := w.FetchEntries(ctx, status.IndexFrom, status.IndexTo)
	if err != nil {
		_ = w.Cluster.ReportShardFailed(ctx, w.JobID, shardID)
		w.Metrics.ShardsFailed++
		return
	}
	matches := w.MatchEntries(entries)
	outputPath, err := w.WriteOutput(matches)
	if err != nil {
		_ = w.Cluster.ReportShardFailed(ctx, w.JobID, shardID)
		w.Metrics.ShardsFailed++
		return
	}
	// Mark done
	man := cluster.ShardManifest{OutputPath: outputPath}
	if err := w.Cluster.ReportShardDone(ctx, w.JobID, shardID, man); err != nil {
		w.Logger.Printf("report done failed: %v", err)
		w.Metrics.ShardsFailed++
		return
	}
	w.Metrics.ShardsProcessed++
	w.Logger.Printf("shard %d completed", shardID)
}

// --- Pipeline stubs to be implemented/extended ---

func (w *Worker) FetchEntries(ctx context.Context, from, to int64) ([]interface{}, error) {
	// TODO: Implement fetching from CT log
	return nil, nil
}

func (w *Worker) MatchEntries(entries []interface{}) []interface{} {
	// TODO: Implement filtering/matching logic
	return entries
}

func (w *Worker) WriteOutput(matches []interface{}) (string, error) {
	// TODO: Implement output (transformers/targets)
	return "", nil
}
