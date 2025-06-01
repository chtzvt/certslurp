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
	fqdn           TEXT,
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

-- Root Domains
CREATE TABLE IF NOT EXISTS root_domains (
    id BIGSERIAL PRIMARY KEY,
    domain TEXT NOT NULL UNIQUE
);

-- Subdomains
CREATE TABLE IF NOT EXISTS subdomains (
    id BIGSERIAL PRIMARY KEY,
    fqdn TEXT NOT NULL,
    root_domain_id BIGINT NOT NULL REFERENCES root_domains(id),
    UNIQUE (fqdn, root_domain_id)
);

-- Certificates
CREATE TABLE certificates (
    id BIGSERIAL,
    not_before TIMESTAMPTZ NOT NULL,
    not_after TIMESTAMPTZ NOT NULL,
    cn TEXT,
    organizational_unit TEXT[],
    organization TEXT[],
    locality TEXT[],
    province TEXT[],
    country TEXT[],
    street_address TEXT[],
    postal_code TEXT[],
    email_addresses TEXT[],
    ip_addresses TEXT[],
    uris TEXT[],
    subject TEXT,
    entry_number BIGINT NOT NULL,
	PRIMARY KEY (id, not_before),
    UNIQUE (subject, not_before, not_after)
) PARTITION BY RANGE (not_before);

-- Join Table
CREATE TABLE IF NOT EXISTS subdomain_certificates (
    subdomain_id BIGINT NOT NULL REFERENCES subdomains(id) ON DELETE CASCADE,
    certificate_id BIGINT NOT NULL,
    certificate_not_before TIMESTAMPTZ NOT NULL,
    first_seen TIMESTAMPTZ,
    last_seen TIMESTAMPTZ,
    PRIMARY KEY (subdomain_id, certificate_id, certificate_not_before),
    FOREIGN KEY (certificate_id, certificate_not_before)
    REFERENCES certificates(id, not_before) ON DELETE CASCADE
) PARTITION BY RANGE (certificate_not_before);
`

// Indexes are run outside transaction because "CONCURRENTLY" is not allowed inside a transaction block.
var indexes = []string{
	`CREATE INDEX IF NOT EXISTS etl_flush_metrics_ended_at_idx ON etl_flush_metrics (ended_at DESC);`,
	`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_subdomains_fqdn ON subdomains(LOWER(fqdn));`,
	`CREATE INDEX IF NOT EXISTS idx_certificates_cn ON certificates(cn);`,
	`CREATE INDEX IF NOT EXISTS idx_certificates_not_after ON certificates(not_after);`,
	`CREATE INDEX IF NOT EXISTS idx_subdomain_certificates_subdomain_id ON subdomain_certificates(subdomain_id);`,
	`CREATE INDEX IF NOT EXISTS idx_subdomain_certificates_certificate_id ON subdomain_certificates(certificate_id);`,
	`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_subdomains_fqdn_trgm ON subdomains USING gin (fqdn gin_trgm_ops);`,
	`CREATE INDEX IF NOT EXISTS idx_certificates_organization_gin ON certificates USING gin (organization);`,
	`CREATE INDEX IF NOT EXISTS idx_certificates_country_gin ON certificates USING gin (country);`,
	`CREATE INDEX IF NOT EXISTS idx_certificates_province_gin ON certificates USING gin (province);`,
	`CREATE INDEX IF NOT EXISTS idx_certificates_subject_not_before_not_after ON certificates(subject, not_before, not_after);`,
}

const certificatesPartitionTemplate = `
CREATE TABLE IF NOT EXISTS certificates_%d PARTITION OF certificates
    FOR VALUES FROM ('%04d-01-01') TO ('%04d-01-01');`

const subdomainCertificatesPartitionTemplate = `
CREATE TABLE IF NOT EXISTS subdomain_certificates_%d PARTITION OF subdomain_certificates
    FOR VALUES FROM ('%04d-01-01') TO ('%04d-01-01');`

const flushCertsFunc = `CREATE OR REPLACE FUNCTION flush_raw_certificates(
    flush_type TEXT DEFAULT 'manual',
    limit_rows BIGINT DEFAULT NULL,           -- Number of rows per batch, NULL for all
    last_processed_id BIGINT DEFAULT 0        -- The last processed id from previous run
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
    -- 1. Stage: work in a temp table for this batch, select by id PK for safe chunking
    CREATE TEMP TABLE tmp_batch AS
    SELECT *
    FROM raw_certificates
    WHERE id > last_processed_id
    ORDER BY id
    LIMIT COALESCE(limit_rows, 1000000000);

    -- 2. Get number of rows to load and last id processed
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

    -- 3. Insert root domains (from dns_names and fqdn)
    INSERT INTO root_domains(domain)
    SELECT DISTINCT lower((regexp_replace(
        CASE
            WHEN array_length(dns_names, 1) > 0 THEN dns_names[1]
            ELSE fqdn
        END,
        '\.$', '', 'g'
    )))
    FROM tmp_batch
    WHERE (array_length(dns_names, 1) > 0 OR fqdn IS NOT NULL)
    ON CONFLICT (domain) DO NOTHING;

    -- 4. Insert subdomains
    CREATE TEMP TABLE tmp_domains AS
    SELECT DISTINCT
        lower(regexp_replace(
            unnest(
                CASE
                    WHEN array_length(dns_names, 1) > 0 THEN dns_names
                    ELSE ARRAY[fqdn]
                END
            ),
            '\.$', '', 'g'
        )) AS fqdn
    FROM tmp_batch
    WHERE (array_length(dns_names, 1) > 0 OR fqdn IS NOT NULL);

    CREATE TEMP TABLE tmp_rootmap AS
    SELECT
        fqdn,
        (SELECT id FROM root_domains WHERE domain = fqdn) AS root_domain_id
    FROM tmp_domains;

    INSERT INTO subdomains (fqdn, root_domain_id)
    SELECT fqdn, root_domain_id
    FROM tmp_rootmap
    WHERE root_domain_id IS NOT NULL
    ON CONFLICT (fqdn, root_domain_id) DO NOTHING;

    -- 5. Insert certificates
    INSERT INTO certificates (
        not_before, not_after, cn, organizational_unit, organization, locality,
        province, country, street_address, postal_code,
        email_addresses, ip_addresses, uris, subject, entry_number
    )
    SELECT
        not_before, not_after, common_name, organizational_unit, organization, locality,
        province, country, street_address, postal_code,
        email_addresses, ip_addresses, uris, subject, log_index
    FROM tmp_batch
    ON CONFLICT (subject, not_before, not_after) DO NOTHING;

    -- 6. Insert subdomain_certificates links
    CREATE TEMP TABLE tmp_certmap AS
    SELECT
        tb.subject,
        tb.not_before,
        tb.not_after,
        s.id AS subdomain_id,
        c.id AS certificate_id
    FROM tmp_batch tb
    JOIN tmp_rootmap rm ON rm.fqdn = lower(
        regexp_replace(
            CASE
                WHEN array_length(tb.dns_names, 1) > 0 THEN tb.dns_names[1]
                ELSE tb.fqdn
            END,
            '\.$', '', 'g'
        )
    )
    JOIN subdomains s ON s.fqdn = rm.fqdn AND s.root_domain_id = rm.root_domain_id
    JOIN certificates c ON c.subject = tb.subject AND c.not_before = tb.not_before AND c.not_after = tb.not_after;

    INSERT INTO subdomain_certificates (subdomain_id, certificate_id, certificate_not_before, first_seen, last_seen)
	SELECT
		subdomain_id,
		certificate_id,
		c.not_before,
		MIN(now()),  -- or: MIN(tb.not_before), up to you
		MAX(now())   -- or: MAX(tb.not_after), up to you
	FROM tmp_certmap
	JOIN certificates c ON c.id = certificate_id
	GROUP BY subdomain_id, certificate_id, c.not_before
	ON CONFLICT (subdomain_id, certificate_id, certificate_not_before) DO UPDATE
		SET last_seen = GREATEST(subdomain_certificates.last_seen, EXCLUDED.last_seen);

    -- 7. Metrics & Cleanup
    v_rows_inserted := (SELECT count(*) FROM tmp_certmap);
    v_rows_deduped  := v_rows_loaded - v_rows_inserted;

    v_ended_at := now();

    INSERT INTO etl_flush_metrics (
        started_at, ended_at, rows_loaded, rows_inserted, rows_deduped, error_count,
        flush_type, status, notes
    ) VALUES (
        v_started_at, v_ended_at, v_rows_loaded, v_rows_inserted, v_rows_deduped, v_error_count,
        flush_type, v_status, v_notes
    );

    -- Update checkpoint/progress table with latest processed id
    INSERT INTO etl_progress (id, last_processed_id)
    VALUES (1, v_last_id)
    ON CONFLICT (id) DO UPDATE SET last_processed_id = EXCLUDED.last_processed_id;

    -- Remove only processed rows by id
    DELETE FROM raw_certificates
    WHERE id IN (SELECT id FROM tmp_batch);

    DROP TABLE IF EXISTS tmp_batch;
    DROP TABLE IF EXISTS tmp_domains;
    DROP TABLE IF EXISTS tmp_rootmap;
    DROP TABLE IF EXISTS tmp_certmap;

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
		certPartitionStmt := fmt.Sprintf(certificatesPartitionTemplate, year, year, year+1)
		_, err := db.Exec(certPartitionStmt)
		if err != nil {
			log.Printf("cert partition init failed: %s", err)
		}

		subdPartitionStmt := fmt.Sprintf(subdomainCertificatesPartitionTemplate, year, year, year+1)
		_, err = db.Exec(subdPartitionStmt)
		if err != nil {
			log.Printf("subdomain partition init failed: %s", err)
		}
	}

	_, err := db.Exec(flushCertsFunc)
	if err != nil {
		log.Printf("flush certs function init failed: %s", err)
	}

	for _, idx := range indexes {
		_, err := db.Exec(idx)
		if err != nil {
			log.Printf("index error (can ignore if already exists): %v\nSQL: %s", err, idx)
		}
	}

	return nil
}
