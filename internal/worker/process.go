package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/chtzvt/certslurp/internal/cluster"
	"github.com/chtzvt/certslurp/internal/etl"
	ct "github.com/google/certificate-transparency-go"
)

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
