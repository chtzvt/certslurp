package main

import (
	"compress/gzip"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/dsnet/compress/bzip2"
)

// StartHTTPServer now takes inboxDir as an argument
func StartHTTPServer(addr, inboxDir string, jobs chan<- InsertJob) {
	mux := http.NewServeMux()
	mux.HandleFunc("/upload", uploadHandler(inboxDir, jobs))

	log.Printf("HTTP server listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func uploadHandler(inboxDir string, jobs chan<- InsertJob) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := handleUpload(w, r, inboxDir, jobs)
		if err != nil {
			http.Error(w, "upload error: "+err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func handleUpload(_ http.ResponseWriter, r *http.Request, inboxDir string, jobs chan<- InsertJob) error {
	// Guess compression and set extension
	ext := ".jsonl" // default

	ctype := strings.ToLower(r.Header.Get("Content-Type"))

	// decompression is based on file extension, not HTTP headers, after upload
	cenc := strings.ToLower(r.Header.Get("Content-Encoding"))
	switch {
	case strings.Contains(cenc, "gzip") || strings.Contains(ctype, "gzip"):
		ext = ".jsonl.gz"
	case strings.Contains(cenc, "bzip2") || strings.Contains(ctype, "bzip2"):
		ext = ".jsonl.bz2"
	}

	// Create temp file with correct extension in inboxDir
	tmp, err := os.CreateTemp(inboxDir, "upload-*"+ext)
	if err != nil {
		return err
	}
	defer tmp.Close()
	n, err := io.Copy(tmp, r.Body)
	if err != nil {
		os.Remove(tmp.Name())
		return err
	}

	// Move (rename) to final inbox path for watcher to pick up
	baseName := filepath.Base(tmp.Name())
	inboxPath := filepath.Join(inboxDir, baseName)
	if err := os.Rename(tmp.Name(), inboxPath); err != nil {
		os.Remove(tmp.Name())
		return err
	}

	log.Printf("[upload] received %s (%d bytes)", inboxPath, n)
	jobs <- InsertJob{Name: baseName, Path: inboxPath}
	return nil
}

// Detects compression from Content-Type and Content-Encoding, wraps decompressor if needed
func getBodyReader(r *http.Request) (io.Reader, error) {
	ctype := strings.ToLower(r.Header.Get("Content-Type"))
	cenc := strings.ToLower(r.Header.Get("Content-Encoding"))

	body := r.Body
	if strings.Contains(cenc, "gzip") || strings.Contains(ctype, "gzip") {
		gr, err := gzip.NewReader(body)
		if err != nil {
			return nil, err
		}
		return gr, nil
	}
	if strings.Contains(cenc, "bzip2") || strings.Contains(ctype, "bzip2") {
		br, err := bzip2.NewReader(body, nil)
		if err != nil {
			return nil, err
		}
		return br, nil
	}

	return body, nil
}
