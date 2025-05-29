package main

import (
	"database/sql"
	"log"
	"strings"
)

const schemaSQL = `
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
CREATE TABLE IF NOT EXISTS certificates (
    id BIGSERIAL PRIMARY KEY,
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
    UNIQUE (
        subject, not_before, not_after
    )
);

-- Many-to-Many: Subdomain <-> Certificate
CREATE TABLE IF NOT EXISTS subdomain_certificates (
    subdomain_id BIGINT NOT NULL REFERENCES subdomains(id) ON DELETE CASCADE,
    certificate_id BIGINT NOT NULL REFERENCES certificates(id) ON DELETE CASCADE,
    first_seen TIMESTAMPTZ,
    last_seen TIMESTAMPTZ,  
    PRIMARY KEY (subdomain_id, certificate_id)
);
`

// Indexes are run outside transaction because "CONCURRENTLY" is not allowed inside a transaction block.
var indexes = []string{
	`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_subdomains_fqdn ON subdomains(LOWER(fqdn));`,
	`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_certificates_cn ON certificates(cn);`,
	`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_certificates_not_after ON certificates(not_after);`,
	`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_subdomain_certificates_subdomain_id ON subdomain_certificates(subdomain_id);`,
	`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_subdomain_certificates_certificate_id ON subdomain_certificates(certificate_id);`,
}

func runInitDB(db *sql.DB) error {
	for _, stmt := range strings.Split(schemaSQL, ";") {
		s := strings.TrimSpace(stmt)
		if s == "" {
			continue
		}
		_, err := db.Exec(s)
		if err != nil {
			return err
		}
	}
	for _, idx := range indexes {
		_, err := db.Exec(idx)
		if err != nil {
			log.Printf("index error (can ignore if already exists): %v\nSQL: %s", err, idx)
		}
	}
	return nil
}
