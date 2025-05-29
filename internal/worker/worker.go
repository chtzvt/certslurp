package worker

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/chtzvt/certslurp/internal/cluster"
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

type ShardRef struct {
	JobID   string
	ShardID int
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

	go w.heartbeatLoop(ctx)
	go w.metricsLoop(ctx)

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

// StreamShard streams log entries for the given shard range directly into the provided channel.
// Closes the channel when done or on error.
func (w *Worker) StreamShard(ctx context.Context, jobSpec job.JobSpec, from, to int64, ch chan<- *ct.RawLogEntry) error {
	matchCfg := jobSpec.Options.Match
	fetchCfg := jobSpec.Options.Fetch

	matcher, matcherInit := buildMatcher(matchCfg)
	opts := scanner.ScannerOptions{
		FetcherOptions: scanner.FetcherOptions{
			BatchSize:     fetchCfg.FetchSize,
			ParallelFetch: fetchCfg.FetchWorkers,
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

	transport, timeout := httpTransportForShard(fetchCfg)

	logClient, err := client.New(jobSpec.LogURI, &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}, jsonclient.Options{UserAgent: "certslurp/1.0", Logger: w.Logger})

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
