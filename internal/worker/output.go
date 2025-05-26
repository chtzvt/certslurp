package worker

import (
	"encoding/json"
	"fmt"
	"os"

	ct "github.com/google/certificate-transparency-go"
)

// WriteOutput serializes results to a JSONL file at a configurable path.
// You can easily swap to CSV, Parquet, or another format as needed.
func (w *Worker) WriteOutput(entries []*ct.RawLogEntry, shardID int) (string, error) {
	os.MkdirAll("output", 0755)
	outputPath := fmt.Sprintf("output/worker-%s-shard-%d.jsonl", w.ID, shardID)
	f, err := os.Create(outputPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, entry := range entries {
		parsed, err := entry.ToLogEntry()
		rec := map[string]interface{}{"index": entry.Index}
		if err != nil {
			rec["parse_error"] = err.Error()
		} else if parsed.X509Cert != nil {
			rec["subject"] = parsed.X509Cert.Subject
			rec["issuer"] = parsed.X509Cert.Issuer
		} else if parsed.Precert != nil {
			rec["subject"] = parsed.Precert.TBSCertificate.Subject
			rec["issuer"] = parsed.Precert.TBSCertificate.Issuer
		} else {
			rec["note"] = "unknown entry type"
		}
		_ = enc.Encode(rec)
	}
	return outputPath, nil
}
