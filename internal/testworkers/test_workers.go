package testworkers

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/chtzvt/ctsnarf/internal/cluster"
	"github.com/chtzvt/ctsnarf/internal/testutil"
	"github.com/chtzvt/ctsnarf/internal/worker"
)

// Run N workers in parallel; returns a slice of workers for further control
func RunWorkers(ctx context.Context, t *testing.T, cl cluster.Cluster, jobID string, workerCount int, logger *log.Logger) []*worker.Worker {
	t.Helper()
	var wg sync.WaitGroup
	workers := make([]*worker.Worker, workerCount)
	for i := 0; i < workerCount; i++ {
		id := "worker-" + testutil.RandString(5)
		w := worker.NewWorker(cl, jobID, id, logger)
		workers[i] = w
		wg.Add(1)
		go func(w *worker.Worker) {
			defer wg.Done()
			_ = w.Run(ctx)
		}(w)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if testutil.AllShardsDone(t, cl, jobID) {
					_ = cl.MarkJobCompleted(ctx, jobID)
					_ = cl.UpdateJobStatus(ctx, jobID, cluster.JobStateCompleted)
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	return workers
}
