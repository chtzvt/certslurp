package main

import (
	"fmt"
	"sync/atomic"
	"time"
)

type SlurploadMetrics struct {
	ShardsProcessed int64 // atomic
	ShardsFailed    int64 // atomic
	processingStart int64 // stores UnixNano, atomic
}

func NewSlurploadMetrics() *SlurploadMetrics {
	return &SlurploadMetrics{}
}

func (m *SlurploadMetrics) Start() {
	atomic.StoreInt64(&m.processingStart, time.Now().UnixNano())
	atomic.StoreInt64(&m.ShardsProcessed, 0)
	atomic.StoreInt64(&m.ShardsFailed, 0)
}

func (m *SlurploadMetrics) Snapshot() (processed, failed int64, elapsed time.Duration) {
	start := atomic.LoadInt64(&m.processingStart)
	if start == 0 {
		return 0, 0, 0
	}
	return atomic.LoadInt64(&m.ShardsProcessed),
		atomic.LoadInt64(&m.ShardsFailed),
		m.Elapsed()
}

func (m *SlurploadMetrics) String() string {
	processed, failed, elapsed := m.Snapshot()
	return fmt.Sprintf("batches processed=%d / batches failed=%d / time elapsed=%v", processed, failed, elapsed)
}

// Helpers for atomic increments
func (m *SlurploadMetrics) IncProcessed() int64 {
	return atomic.AddInt64(&m.ShardsProcessed, 1)
}

func (m *SlurploadMetrics) IncFailed() int64 {
	return atomic.AddInt64(&m.ShardsFailed, 1)
}

func (m *SlurploadMetrics) Elapsed() time.Duration {
	start := atomic.LoadInt64(&m.processingStart)
	if start == 0 {
		return 0
	}
	return time.Since(time.Unix(0, start))
}
