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
	"sync/atomic"
	"time"

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
	errorCount *int64,
	processedRecords *int64,
	startTime time.Time,
	doneDir string,
) {
	defer wg.Done()

	for job := range jobs {
		err := processFileJob(ctx, db, job, batchSize, logStatEvery, errorCount, processedRecords, startTime)
		if err != nil {
			log.Printf("[error] processing file %s: %v", job.Path, err)
			atomic.AddInt64(errorCount, 1)
			continue
		}

		// Clean up the file after successful processing
		if doneDir != "" {
			dest := filepath.Join(doneDir, filepath.Base(job.Path))
			if err := os.Rename(job.Path, dest); err != nil {
				log.Printf("[error] failed to move %s to done dir: %v", job.Path, err)
			}
		} else {
			if err := os.Remove(job.Path); err != nil {
				log.Printf("[error] failed to delete %s after processing: %v", job.Path, err)
			}
		}
	}
}

func processFileJob(
	ctx context.Context,
	db *sql.DB,
	job InsertJob,
	batchSize int,
	logStatEvery int64,
	errorCount *int64,
	processedRecords *int64,
	startTime time.Time,
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
			atomic.AddInt64(errorCount, 1)
			continue
		}
		batch = append(batch, cert)

		if len(batch) >= batchSize {
			if err := insertBatch(ctx, db, batch, processedRecords, errorCount, logStatEvery, startTime); err != nil {
				return fmt.Errorf("insert batch: %w", err)
			}
			batch = batch[:0]
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}
	if len(batch) > 0 {
		if err := insertBatch(ctx, db, batch, processedRecords, errorCount, logStatEvery, startTime); err != nil {
			return fmt.Errorf("insert batch: %w", err)
		}
	}
	return nil
}
