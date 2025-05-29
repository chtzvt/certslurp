package main

import (
	"log"
	"os"
	"path/filepath"
	"time"
)

type WatcherConfig struct {
	InboxDir     string
	DoneDir      string // Optional: Where to move processed files, or "" to delete after processing
	PollInterval time.Duration
	FilePatterns []string // e.g. []string{"*.jsonl", "*.jsonl.gz", "*.jsonl.bz2"}
}

// StartInboxWatcher polls the inbox directory and enqueues unprocessed files for loading.
func StartInboxWatcher(cfg WatcherConfig, jobs chan<- InsertJob, stop <-chan struct{}) {
	seen := make(map[string]struct{})

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
				if _, already := seen[file]; already {
					continue
				}
				// Optionally: skip files that are being written to (check mtime/size twice)
				if isFileLocked(file) {
					log.Printf("File %s appears locked/busy, will retry later", file)
					continue
				}

				log.Printf("Watcher: queueing file %s for loading", file)
				jobs <- InsertJob{Name: filepath.Base(file), Path: file}
				seen[file] = struct{}{}
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

// Utility: Check if file is likely still being written (basic check: unchanged for 2 sec)
var isFileLocked = func(path string) bool {
	fi1, err := os.Stat(path)
	if err != nil {
		return true
	}
	time.Sleep(2 * time.Second)
	fi2, err := os.Stat(path)
	if err != nil {
		return true
	}
	return fi1.Size() != fi2.Size() || fi1.ModTime() != fi2.ModTime()
}
