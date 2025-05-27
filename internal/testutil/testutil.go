package testutil

import (
	"log"
	"os"
	"testing"
	"time"
)

// Random string for unique prefixes
func RandString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyz0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[time.Now().UnixNano()%int64(len(letters))]
	}
	return string(b)
}

// Utility: Wait for a condition or timeout
func WaitFor(t *testing.T, cond func() bool, timeout time.Duration, tick time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(tick)
	}
	t.Fatalf("WaitFor timeout: %s", msg)
}

// Helper for creating a test logger that discards or logs as needed
func NewTestLogger(discard bool) *log.Logger {
	if discard {
		return log.New(os.Stderr, "[worker] ", log.LstdFlags)
	}
	return log.New(os.Stdout, "[worker] ", log.LstdFlags)
}

func SetupTempDir(t *testing.T) (string, func()) {
	tempDir, err := os.MkdirTemp("", "testutil")
	if err != nil {
		t.Fatalf("failed to create temporary directory: %v", err)
	}
	return tempDir, func() { os.RemoveAll(tempDir) }
}
