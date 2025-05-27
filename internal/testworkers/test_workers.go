package testworkers

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/chtzvt/certslurp/internal/cluster"
	"github.com/chtzvt/certslurp/internal/testutil"
	"github.com/chtzvt/certslurp/internal/worker"
)

// Run N workers in parallel; returns a slice of workers for further control
func RunWorkers(ctx context.Context, t *testing.T, cl cluster.Cluster, jobID string, workerCount int, logger *log.Logger) []*worker.Worker {
	t.Helper()
	var wg sync.WaitGroup
	workers := make([]*worker.Worker, workerCount)
	for i := 0; i < workerCount; i++ {
		id := "worker-" + testutil.RandString(5)
		w := worker.NewWorker(cl, id, logger)
		// Aggressive settings for testing
		w.PollPeriod = 50 * time.Millisecond
		w.BatchSize = 32
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
				assignments, _ := cl.GetShardAssignments(context.Background(), jobID)
				unfinished := 0
				for _, stat := range assignments {
					if !stat.Done && !stat.Failed {
						unfinished++
					}
				}

				if testing.Verbose() {
					fmt.Printf("Shards remaining: %d\n", unfinished)
				}

				if unfinished == 0 {
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
