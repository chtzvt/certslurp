package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/chtzvt/certslurp/internal/extractor"
	"github.com/dsnet/compress/bzip2"
)

func fileWorker(
	ctx context.Context,
	db *sql.DB,
	jobs <-chan InsertJob,
	batchSize int,
	wg *sync.WaitGroup,
	logStatEvery int64,
	metrics *SlurploadMetrics,
	doneDir string,
	watcherCfg *WatcherConfig,
) {
	defer wg.Done()

	for job := range jobs {
		err := processFileJob(ctx, db, job, batchSize, logStatEvery, metrics)
		if err != nil {
			log.Printf("[error] processing file %s: %v", job.Path, err)
			cleanupFile(job.Path, watcherCfg)
			metrics.IncFailed()
			continue
		}

		// Clean up the file after successful processing
		if doneDir != "" {
			dest := filepath.Join(doneDir, filepath.Base(job.Path))
			if err := os.Rename(job.Path, dest); err != nil {
				log.Printf("[error] failed to move %s to done dir: %v", job.Path, err)
			} else {
				watcherCfg.RemoveSeen(job.Path)
			}
		} else {
			if err := cleanupFile(job.Path, watcherCfg); err != nil {
				log.Printf("[error] failed to delete %s after processing: %v", job.Path, err)
			}
		}
	}
}

func cleanupFile(path string, w *WatcherConfig) error {
	err := os.Remove(path)
	w.RemoveSeen(path)

	return err
}

func processFileJob(
	ctx context.Context,
	db *sql.DB,
	job InsertJob,
	batchSize int,
	logStatEvery int64,
	metrics *SlurploadMetrics,
) error {
	f, err := os.Open(job.Path)
	if err != nil {
		return fmt.Errorf("open failed: %w", err)
	}
	defer f.Close()

	var reader io.Reader = f
	switch {
	case strings.HasSuffix(job.Path, ".gz"):
		gr, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("gzip reader: %w", err)
		}
		defer gr.Close()
		reader = gr
	case strings.HasSuffix(job.Path, ".bz2"):
		br, err := bzip2.NewReader(f, nil)
		if err != nil {
			return fmt.Errorf("bzip2 reader: %w", err)
		}
		reader = br
	}
	scanner := bufio.NewScanner(reader)
	batch := make([]extractor.CertFieldsExtractorOutput, 0, batchSize)

	for scanner.Scan() {
		var cert extractor.CertFieldsExtractorOutput
		if err := json.Unmarshal(scanner.Bytes(), &cert); err != nil {
			log.Printf("[warn] bad json in %s: %v", job.Path, err)
			metrics.IncFailed()
			continue
		}
		batch = append(batch, cert)

		if len(batch) >= batchSize {
			if err := insertBatch(ctx, db, batch, logStatEvery, metrics); err != nil {
				return fmt.Errorf("insert batch: %w", err)
			}
			batch = batch[:0]
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}
	if len(batch) > 0 {
		if err := insertBatch(ctx, db, batch, logStatEvery, metrics); err != nil {
			return fmt.Errorf("insert batch: %w", err)
		}
	}
	return nil
}
