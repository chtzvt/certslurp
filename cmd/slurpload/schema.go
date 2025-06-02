package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

const schemaSQL = `
CREATE UNLOGGED TABLE IF NOT EXISTS raw_certificates (
	id BIGSERIAL PRIMARY KEY,

	cert_type TEXT,

    common_name         TEXT,
	email_addresses     TEXT[],
    organizational_unit TEXT[],
    organization        TEXT[],
    locality            TEXT[],
    province            TEXT[],
    country             TEXT[],
    street_address      TEXT[],
    postal_code         TEXT[],
	dns_names           TEXT[],
	root_domain         TEXT,
    ip_addresses        TEXT[],
    uris                TEXT[],
    subject             TEXT,
    issuer              TEXT,
    serial_number       TEXT,
	not_before          TIMESTAMPTZ,
    not_after           TIMESTAMPTZ,

	log_index           BIGINT,
    log_timestamp       TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS etl_flush_metrics (
    id              BIGSERIAL PRIMARY KEY,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at        TIMESTAMPTZ,
    rows_loaded     BIGINT,
    rows_inserted   BIGINT,
    rows_deduped    BIGINT,
    error_count     BIGINT,
    flush_type      TEXT,
    status          TEXT,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS etl_progress (
    id SERIAL PRIMARY KEY,
    last_processed_id BIGINT NOT NULL DEFAULT 0
);

-- Enable pg_cron
-- CREATE EXTENSION IF NOT EXISTS pg_cron;
-- SELECT cron.schedule('flush_raw_certificates', '*/5 * * * *', $$SELECT flush_raw_certificates()$$);

-- Enable pg_trgm
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE certificates (
    id BIGSERIAL,
    common_name TEXT,
    issuer TEXT,
    subject TEXT,
    organizational_unit TEXT,
    organization TEXT,
    locality TEXT,
    province TEXT,
    country TEXT,
    street_address TEXT,
    postal_code TEXT,
    email_addresses TEXT,
    ip_addresses TEXT,
    uris TEXT,
    dns_names TEXT[],
    dns_names_text TEXT,
    root_domain TEXT NOT NULL,
    not_before TIMESTAMPTZ NOT NULL,
    not_after TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (id, not_before),
    UNIQUE (subject, not_before, not_after)
) PARTITION BY RANGE (not_before);
`

const syncDnsNamesTrigger string = `CREATE OR REPLACE FUNCTION sync_dns_names_text() RETURNS trigger AS $$
BEGIN
    IF NEW.dns_names_text IS NULL THEN
        NEW.dns_names_text := array_to_string(NEW.dns_names, ', ');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql STABLE;

CREATE TRIGGER sync_dns_names_text_before_insert
BEFORE INSERT OR UPDATE ON certificates
FOR EACH ROW EXECUTE FUNCTION sync_dns_names_text();`

// Indexes are run outside transaction because "CONCURRENTLY" is not allowed inside a transaction block.
var indexes = []string{
	`CREATE INDEX IF NOT EXISTS etl_flush_metrics_ended_at_idx ON etl_flush_metrics (ended_at DESC);`,
	`CREATE INDEX IF NOT EXISTS idx_certificates_not_before ON certificates (not_before);`,
	`CREATE INDEX idx_icdn_common_name_trgm ON certificates USING gin (common_name gin_trgm_ops);`,
	`CREATE INDEX idx_cert_dns_names_gin ON certificates USING gin (dns_names);`,
	`CREATE INDEX IF NOT EXISTS idx_cert_dns_names_text_trgm ON certificates USING gin (dns_names_text gin_trgm_ops);`,
	`CREATE INDEX idx_icdn_country_notbefore ON certificates(country, not_before);`,
	`CREATE INDEX idx_icdn_organization_notbefore ON certificates(organization, not_before);`,
}

const certificatesPartitionTemplate = `
CREATE TABLE IF NOT EXISTS certificates_%d PARTITION OF certificates
    FOR VALUES FROM ('%04d-01-01') TO ('%04d-01-01');
ALTER TABLE certificates_%d
ADD CONSTRAINT certificates_%d_unique_subject_notbefore_notafter UNIQUE (subject, not_before, not_after);
`

const flushCertsFunc = `CREATE OR REPLACE FUNCTION flush_raw_certificates(
    flush_type TEXT DEFAULT 'manual',
    limit_rows BIGINT DEFAULT NULL,
    last_processed_id BIGINT DEFAULT 0
) RETURNS VOID AS $$
DECLARE
    v_started_at      TIMESTAMPTZ := now();
    v_ended_at        TIMESTAMPTZ;
    v_rows_loaded     BIGINT := 0;
    v_rows_inserted   BIGINT := 0;
    v_rows_deduped    BIGINT := 0;
    v_error_count     BIGINT := 0;
    v_status          TEXT := 'success';
    v_notes           TEXT := '';
    v_last_id         BIGINT := 0;
BEGIN
    -- Select batch to process
    CREATE TEMP TABLE tmp_batch AS
    SELECT *
    FROM raw_certificates
    WHERE id > last_processed_id
    ORDER BY id
    LIMIT COALESCE(limit_rows, 1000000000);

    -- Row counts, last id
    SELECT count(*), COALESCE(max(id),0) INTO v_rows_loaded, v_last_id FROM tmp_batch;
    IF v_rows_loaded = 0 THEN
        v_status := 'noop';
        v_ended_at := now();
        INSERT INTO etl_flush_metrics (
            started_at, ended_at, rows_loaded, rows_inserted, rows_deduped, error_count,
            flush_type, status, notes
        ) VALUES (
            v_started_at, v_ended_at, 0, 0, 0, 0,
            flush_type, v_status, 'Nothing to flush.'
        );
        RETURN;
    END IF;

    -- Insert certificates
    INSERT INTO certificates (
        common_name,
        issuer,
        subject,
        organizational_unit,
        organization,
        locality,
        province,
        country,
        street_address,
        postal_code,
        email_addresses,
        ip_addresses,
        uris,
        dns_names,
        dns_names_text,
        root_domain,
        not_before,
        not_after
    )
    SELECT
        common_name,
        issuer,
        subject,
        array_to_string(organizational_unit, ','),
        array_to_string(organization, ','),
        array_to_string(locality, ','),
        array_to_string(province, ','),
        array_to_string(country, ','),
        array_to_string(street_address, ','),
        array_to_string(postal_code, ','),
        array_to_string(email_addresses, ','),
        array_to_string(ip_addresses, ','),
        array_to_string(uris, ','),
        dns_names,
        array_to_string(dns_names, ','),
        root_domain,
        not_before,
        not_after
    FROM tmp_batch
    ON CONFLICT (subject, not_before, not_after) DO NOTHING;

    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    -- Metrics & cleanup
    v_rows_deduped  := v_rows_loaded - v_rows_inserted;

    v_ended_at := now();

    INSERT INTO etl_flush_metrics (
        started_at, ended_at, rows_loaded, rows_inserted, rows_deduped, error_count,
        flush_type, status, notes
    ) VALUES (
        v_started_at, v_ended_at, v_rows_loaded, v_rows_inserted, v_rows_deduped, v_error_count,
        flush_type, v_status, v_notes
    );

    -- Update checkpoint
    INSERT INTO etl_progress (id, last_processed_id)
    VALUES (1, v_last_id)
    ON CONFLICT (id) DO UPDATE SET last_processed_id = EXCLUDED.last_processed_id;

    -- Remove processed rows
    DELETE FROM raw_certificates WHERE id IN (SELECT id FROM tmp_batch);

    DROP TABLE IF EXISTS tmp_batch;

    ANALYZE certificates;
EXCEPTION WHEN OTHERS THEN
    v_status := 'failed';
    v_ended_at := now();
    v_notes := SQLERRM;
    INSERT INTO etl_flush_metrics (
        started_at, ended_at, rows_loaded, rows_inserted, rows_deduped, error_count,
        flush_type, status, notes
    ) VALUES (
        v_started_at, v_ended_at, v_rows_loaded, 0, 0, 1,
        flush_type, v_status, v_notes
    );
    RAISE;
END
$$ LANGUAGE plpgsql;`

func runInitDB(db *sql.DB) error {
	log.Printf("Initializing schema...")
	for _, stmt := range strings.Split(schemaSQL, ";") {
		s := strings.TrimSpace(stmt)
		if s == "" {
			continue
		}
		_, err := db.Exec(s)
		if err != nil {
			log.Printf("schema init failed: %s", err)
			return err
		}
	}

	for year := 2000; year <= 2070; year++ {
		certPartitionStmt := fmt.Sprintf(certificatesPartitionTemplate, year, year, year+1, year, year)
		_, err := db.Exec(certPartitionStmt)
		if err != nil {
			log.Printf("cert partition init failed: %s", err)
			return err
		}
	}

	_, err := db.Exec(syncDnsNamesTrigger)
	if err != nil {
		log.Printf("sync dns names trigger init failed: %s", err)
		return err
	}

	_, err = db.Exec(flushCertsFunc)
	if err != nil {
		log.Printf("flush certs function init failed: %s", err)
		return err
	}

	for _, idx := range indexes {
		_, err := db.Exec(idx)
		if err != nil {
			log.Printf("index error (can ignore if already exists): %v\nSQL: %s", err, idx)
			return err
		}
	}

	return nil
}
