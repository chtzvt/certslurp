package testutil

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	ct "github.com/google/certificate-transparency-go"
)

func RawLogEntryForTestCert(t *testing.T, index int) *ct.RawLogEntry {
	t.Helper()
	ts := NewStubCTLogServer(t, CTLogFourEntrySTH, CTLogFourEntries)
	defer ts.Close()

	entries := fetchLeafEntriesFromStub(t, ts.URL, 0, 4)

	rle, err := ct.RawLogEntryFromLeaf(int64(index), entries[index])
	if err != nil {
		t.Fatalf("RawLogEntryFromLeaf: %v", err)
	}
	return rle
}

// fetchLeafEntriesFromStub returns LeafEntry (raw blobs) to be fed to RawLogEntryFromLeaf.
func fetchLeafEntriesFromStub(t *testing.T, baseURL string, start, end int) []*ct.LeafEntry {
	url := fmt.Sprintf("%s/ct/v1/get-entries?start=%d&end=%d", baseURL, start, end)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("fetching ct entries: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("unexpected status: %v", resp.Status)
	}
	var parsed struct {
		Entries []struct {
			LeafInput string `json:"leaf_input"`
			ExtraData string `json:"extra_data"`
		} `json:"entries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode entries: %v", err)
	}
	var out []*ct.LeafEntry
	for i, entry := range parsed.Entries {
		leaf, err := base64.StdEncoding.DecodeString(entry.LeafInput)
		if err != nil {
			t.Fatalf("base64 decode leaf_input at %d: %v", start+i, err)
		}
		extra, err := base64.StdEncoding.DecodeString(entry.ExtraData)
		if err != nil {
			t.Fatalf("base64 decode extra_data at %d: %v", start+i, err)
		}
		out = append(out, &ct.LeafEntry{
			LeafInput: leaf,
			ExtraData: extra,
		})
	}
	return out
}
