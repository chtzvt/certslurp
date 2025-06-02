package main

import (
	"log"
	"path/filepath"
	"sync"
	"time"
)

type WatcherConfig struct {
	InboxDir     string
	DoneDir      string // Optional: Where to move processed files, or "" to delete after processing
	PollInterval time.Duration
	FilePatterns []string // e.g. []string{"*.jsonl", "*.jsonl.gz", "*.jsonl.bz2"}
	seenFiles    map[string]time.Time
	seenMu       sync.Mutex
}

func NewWatcherConfig(inboxDir, doneDir string, filePatterns []string, pollInterval time.Duration) *WatcherConfig {
	return &WatcherConfig{
		InboxDir:     inboxDir,
		DoneDir:      doneDir,
		PollInterval: pollInterval,
		FilePatterns: filePatterns,
		seenFiles:    make(map[string]time.Time),
	}
}

func (w *WatcherConfig) AddSeen(file string) {
	w.seenMu.Lock()
	defer w.seenMu.Unlock()

	w.seenFiles[file] = time.Now()
}

func (w *WatcherConfig) RemoveSeen(file string) {
	w.seenMu.Lock()
	defer w.seenMu.Unlock()

	delete(w.seenFiles, file)
}

func (w *WatcherConfig) HasSeen(file string) bool {
	w.seenMu.Lock()
	defer w.seenMu.Unlock()

	_, seen := w.seenFiles[file]

	return seen
}

// StartInboxWatcher polls the inbox directory and enqueues unprocessed files for loading.
func StartInboxWatcher(cfg *WatcherConfig, jobs chan<- InsertJob, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			log.Println("Inbox watcher: stopping")
			return
		default:
			files, err := listMatchingFiles(cfg.InboxDir, cfg.FilePatterns)
			if err != nil {
				log.Printf("Watcher error: %v", err)
				time.Sleep(cfg.PollInterval)
				continue
			}
			for _, file := range files {
				if cfg.HasSeen(file) {
					continue
				}

				cfg.AddSeen(file)
				log.Printf("Watcher: queueing file %s for loading", file)
				jobs <- InsertJob{Name: filepath.Base(file), Path: file}
				// File will be deleted/moved by batcher/worker after DB insert completes
			}
			time.Sleep(cfg.PollInterval)
		}
	}
}

// Utility: List files in dir matching any of the provided patterns
func listMatchingFiles(dir string, patterns []string) ([]string, error) {
	var result []string
	for _, pattern := range patterns {
		glob := filepath.Join(dir, pattern)
		files, err := filepath.Glob(glob)
		if err != nil {
			return nil, err
		}
		result = append(result, files...)
	}
	return result, nil
}
