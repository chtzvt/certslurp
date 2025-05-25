package worker

import (
	"encoding/json"
	"fmt"
	"os"

	ct "github.com/google/certificate-transparency-go"
)

// WriteOutput serializes results to a JSONL file at a configurable path.
// You can easily swap to CSV, Parquet, or another format as needed.
func (w *Worker) WriteOutput(entries []*ct.RawLogEntry) (string, error) {
	outputPath := fmt.Sprintf("output/worker-%s.jsonl", w.ID)
	f, err := os.Create(outputPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, entry := range entries {
		parsed, _ := entry.ToLogEntry() // ignore errors for now
		// Write what you want: index, subject, issuer, notAfter, etc.
		rec := map[string]interface{}{
			"index":   entry.Index,
			"subject": parsed.X509Cert.Subject,
			"issuer":  parsed.X509Cert.Issuer,
		}
		enc.Encode(rec)
	}
	return outputPath, nil
}
