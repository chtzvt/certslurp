package worker

import (
	"sync"
	"sync/atomic"
	"time"
)

type WorkerMetrics struct {
	ShardsProcessed int64 // atomic
	ShardsFailed    int64 // atomic
	processingTime  int64 // nanoseconds, atomic

	mu sync.Mutex
}

func (m *WorkerMetrics) Snapshot() (processed, failed int64, totalTime time.Duration) {
	return atomic.LoadInt64(&m.ShardsProcessed),
		atomic.LoadInt64(&m.ShardsFailed),
		time.Duration(atomic.LoadInt64(&m.processingTime))
}

// Helper methods for atomic increments
func (m *WorkerMetrics) IncProcessed() {
	atomic.AddInt64(&m.ShardsProcessed, 1)
}
func (m *WorkerMetrics) IncFailed() {
	atomic.AddInt64(&m.ShardsFailed, 1)
}
func (m *WorkerMetrics) AddProcessingTime(d time.Duration) {
	atomic.AddInt64(&m.processingTime, d.Nanoseconds())
}
func (m *WorkerMetrics) ProcessingTime() time.Duration {
	return time.Duration(atomic.LoadInt64(&m.processingTime))
}
