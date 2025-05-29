package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/chtzvt/certslurp/internal/extractor"
	"golang.org/x/net/publicsuffix"
)

type InsertJob struct {
	Name string // e.g. filename or upload-id (for logging)
	Path string // Full path to file
}

func insertBatch(
	ctx context.Context,
	db *sql.DB,
	batch []extractor.CertFieldsExtractorOutput,
	processedRecords *int64,
	errorCount *int64,
	logStatEvery int64,
	startTime time.Time,
) error {
	if len(batch) == 0 {
		return nil
	}

	rootDomainIDs := make(map[string]int64)
	for _, cert := range batch {
		domains := append(cert.DNSNames, cert.CommonName)
		for _, fqdn := range domains {
			fqdn = strings.ToLower(strings.TrimSpace(fqdn))
			fqdn = strings.TrimSuffix(fqdn, ".")
			if fqdn == "" {
				continue
			}
			root, err := publicsuffix.EffectiveTLDPlusOne(fqdn)
			if err == nil && root != "" {
				rootDomainIDs[root] = 0
			}
		}
	}

	for domain := range rootDomainIDs {
		var id int64
		err := db.QueryRowContext(ctx, `
            INSERT INTO root_domains (domain) VALUES ($1)
            ON CONFLICT (domain) DO UPDATE SET domain=EXCLUDED.domain
            RETURNING id
        `, domain).Scan(&id)
		if err != nil {
			log.Printf("root insert err (upsert outside tx): %v", err)
			continue
		}
		rootDomainIDs[domain] = id
	}

	const maxRetries = 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			log.Printf("batch tx begin error: %v", err)
			atomic.AddInt64(errorCount, 1)
			return err
		}

		batchFailed := false
		pendingCache := make(map[string]int64)

		for _, cert := range batch {
			domains := append(cert.DNSNames, cert.CommonName)
			for _, fqdn := range domains {
				fqdn = strings.ToLower(strings.TrimSpace(fqdn))
				fqdn = strings.TrimSuffix(fqdn, ".")
				if fqdn == "" {
					continue
				}
				root, err := publicsuffix.EffectiveTLDPlusOne(fqdn)
				if err != nil {
					continue
				}
				rootID, ok := rootDomainIDs[root]
				if !ok {
					continue
				}

				fqdnKey := fqdn + "|" + fmt.Sprint(rootID)
				var subID int64

				if cached, ok := fqdnCache.Get(fqdnKey); ok {
					subID = cached.(int64)
				} else if cached, ok := pendingCache[fqdnKey]; ok {
					subID = cached
				} else {
					err = tx.QueryRowContext(ctx, `
                        INSERT INTO subdomains (fqdn, root_domain_id) VALUES ($1, $2)
                        ON CONFLICT (fqdn, root_domain_id) DO UPDATE SET fqdn=EXCLUDED.fqdn
                        RETURNING id
                    `, fqdn, rootID).Scan(&subID)
					if err != nil {
						if err == sql.ErrNoRows || strings.Contains(err.Error(), "no rows in result set") {
							err = tx.QueryRowContext(ctx, `
                                SELECT id FROM subdomains WHERE fqdn = $1 AND root_domain_id = $2
                            `, fqdn, rootID).Scan(&subID)
						} else {
							log.Printf("subdomain upsert/select err: %v", err)
							batchFailed = true
							break
						}
					}
					pendingCache[fqdnKey] = subID
				}

				var certID int64
				err = tx.QueryRowContext(ctx, `
                    INSERT INTO certificates (
                        not_before, not_after, cn, organizational_unit, organization, locality,
                        province, country, street_address, postal_code,
                        email_addresses, ip_addresses, uris, subject, entry_number
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
                    ON CONFLICT (subject, not_before, not_after)
                        DO NOTHING
                    RETURNING id
                `,
					cert.NotBefore, cert.NotAfter, cert.CommonName, pqStringArray(cert.OrganizationalUnit),
					pqStringArray(cert.Organization), pqStringArray(cert.Locality), pqStringArray(cert.Province),
					pqStringArray(cert.Country), pqStringArray(cert.StreetAddress), pqStringArray(cert.PostalCode),
					pqStringArray(cert.EmailAddresses), pqStringArray(cert.IPAddresses),
					pqStringArray(cert.URIs), cert.Subject, cert.LogIndex,
				).Scan(&certID)
				if err != nil {
					if err == sql.ErrNoRows || strings.Contains(err.Error(), "no rows in result set") {
						err = tx.QueryRowContext(ctx, `
                            SELECT id FROM certificates
                            WHERE subject = $1 AND not_before = $2 AND not_after = $3
                        `, cert.Subject, cert.NotBefore, cert.NotAfter).Scan(&certID)
					}
				}
				if err != nil {
					log.Printf("cert upsert/select err: %v", err)
					batchFailed = true
					break
				}

				_, err = tx.ExecContext(ctx, `
                    INSERT INTO subdomain_certificates (subdomain_id, certificate_id, first_seen, last_seen)
                    VALUES ($1, $2, $3, $3)
                    ON CONFLICT (subdomain_id, certificate_id) DO UPDATE
                    SET last_seen = GREATEST(subdomain_certificates.last_seen, $3)
                `, subID, certID, time.Now())
				if err != nil {
					log.Printf("link err: %v", err)
					batchFailed = true
					break
				}
				if logStatEvery > 0 {
					n := atomic.AddInt64(processedRecords, 1)
					if n%logStatEvery == 0 {
						log.Printf("[progress] %d records processed, %d errors, elapsed: %v", n, atomic.LoadInt64(errorCount), time.Since(startTime).Truncate(time.Second))
					}
				}
			}
			if batchFailed {
				break
			}
		}

		if batchFailed {
			_ = tx.Rollback()
			if attempt < maxRetries {
				log.Printf("Deadlock detected during batch, retrying (%d/%d)", attempt, maxRetries)
				time.Sleep(time.Duration(100+rand.Intn(500)) * time.Millisecond)
				continue
			} else {
				log.Printf("Batch failed after %d attempts due to repeated deadlocks or errors", maxRetries)
				atomic.AddInt64(errorCount, 1)
			}
		} else {
			err = tx.Commit()
			if err != nil {
				log.Printf("batch tx commit error: %v", err)
				atomic.AddInt64(errorCount, 1)
			} else {
				for k, v := range pendingCache {
					fqdnCache.Add(k, v)
				}
			}
		}
		break
	}
	return nil
}
