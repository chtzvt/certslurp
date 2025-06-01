package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/chtzvt/certslurp/internal/extractor"
	"github.com/lib/pq"
)

type InsertJob struct {
	Name string // e.g. filename or upload-id (for logging)
	Path string // Full path to file
}

func insertBatch(
	ctx context.Context,
	db *sql.DB,
	batch []extractor.CertFieldsExtractorOutput,
	logStatEvery int64,
	metrics *SlurploadMetrics,
) error {
	if len(batch) == 0 {
		return nil
	}

	// 1. Start a transaction for COPY and flush
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		metrics.IncFailed()
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// 2. Prepare COPY statement
	stmt, err := tx.Prepare(pq.CopyIn(
		"raw_certificates",
		"cert_type", "common_name", "email_addresses", "organizational_unit", "organization",
		"locality", "province", "country", "street_address", "postal_code",
		"dns_names", "fqdn", "ip_addresses", "uris", "subject", "issuer", "serial_number",
		"not_before", "not_after", "log_index", "log_timestamp",
	))
	if err != nil {
		return fmt.Errorf("prepare COPY: %w", err)
	}

	// 3. Write all batch rows
	for _, cert := range batch {
		_, err := stmt.Exec(
			cert.Type, cert.CommonName, pqStringArray(cert.EmailAddresses), pqStringArray(cert.OrganizationalUnit),
			pqStringArray(cert.Organization), pqStringArray(cert.Locality), pqStringArray(cert.Province),
			pqStringArray(cert.Country), pqStringArray(cert.StreetAddress), pqStringArray(cert.PostalCode),
			pqStringArray(cert.DNSNames), cert.CommonName, // 'fqdn' defaults to CommonName here
			pqStringArray(cert.IPAddresses), pqStringArray(cert.URIs),
			cert.Subject, cert.Issuer, cert.SerialNumber,
			cert.NotBefore, cert.NotAfter, cert.LogIndex, cert.LogTimestamp,
		)
		if err != nil {
			return fmt.Errorf("COPY exec: %w", err)
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		return fmt.Errorf("COPY exec flush: %w", err)
	}
	if err := stmt.Close(); err != nil {
		return fmt.Errorf("COPY close: %w", err)
	}

	// 4. Call flush function for this batch (let DB handle normalization)
	_, err = tx.Exec(`SELECT flush_raw_certificates($1)`, "worker")
	if err != nil {
		return fmt.Errorf("flush_raw_certificates: %w", err)
	}

	// 5. Commit
	if err := tx.Commit(); err != nil {
		metrics.IncFailed()
		return fmt.Errorf("commit: %w", err)
	}

	if logStatEvery > 0 {
		n := metrics.IncProcessed()
		if n%logStatEvery == 0 {
			log.Printf("[progress] %s", metrics)
		}
	}

	metrics.IncProcessed()
	return nil
}

// Trigger ETL flush every FlushInterval or after FlushThreshold raw_certificates are loaded.
func RunFlusher(ctx context.Context, db *sql.DB, cfg *SlurploadConfig, metrics *SlurploadMetrics) {
	ticker := time.NewTicker(cfg.Processing.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			FlushIfNeeded(db, cfg, metrics)
		}
	}
}

// Only flush if there are enough staged rows.
func FlushIfNeeded(db *sql.DB, cfg *SlurploadConfig, metrics *SlurploadMetrics) {
	var lastProcessedID int64
	err := db.QueryRow("SELECT last_processed_id FROM etl_progress WHERE id=1").Scan(&lastProcessedID)
	if err == sql.ErrNoRows {
		lastProcessedID = 0
	} else if err != nil {
		log.Printf("error reading checkpoint: %v", err)
		return
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM raw_certificates WHERE id > $1", lastProcessedID).Scan(&count)
	if err != nil {
		log.Printf("error checking for more work: %v", err)
		return
	}

	if count < int(cfg.Processing.FlushThreshold) {
		// Not enough to flush yet
		return
	}

	_, err = db.Exec(
		"SELECT flush_raw_certificates($1, $2, $3)",
		"batch",
		cfg.Processing.FlushLimit,
		lastProcessedID,
	)
	if err != nil {
		log.Printf("error calling flush_raw_certificates: %v", err)
		return
	}
	log.Printf("ETL flush completed (%d staged rows flushed)", count)
}
