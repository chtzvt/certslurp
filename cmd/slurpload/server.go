package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dsnet/compress/bzip2"
)

func StartHTTPServer(ctx context.Context, cfg *SlurploadConfig, metrics *SlurploadMetrics) {
	mux := http.NewServeMux()
	mux.HandleFunc("/upload", uploadHandler(cfg.Processing.InboxDir))
	mux.HandleFunc("/metrics", metricsHandler(metrics))

	server := &http.Server{
		Addr:    cfg.Server.ListenAddr,
		Handler: mux,
	}

	go func() {
		log.Printf("HTTP server listening on %s", cfg.Server.ListenAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	log.Println("Shutting down HTTP server gracefully...")
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}
}

func metricsHandler(metrics *SlurploadMetrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		processed, failed, elapsed := metrics.Snapshot()
		type status struct {
			Processed int64         `json:"processed"`
			Failed    int64         `json:"failed"`
			Elapsed   time.Duration `json:"elapsed"`
		}
		s := status{Processed: processed, Failed: failed, Elapsed: elapsed}
		_ = json.NewEncoder(w).Encode(s)
	}
}

func uploadHandler(inboxDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := handleUpload(w, r, inboxDir)
		if err != nil {
			http.Error(w, "upload error: "+err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func handleUpload(w http.ResponseWriter, r *http.Request, inboxDir string) error {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return fmt.Errorf("method not allowed")
	}

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

	// Create temp file in inboxDir with no extension to avoid triggering watcher
	tmp, err := os.CreateTemp(inboxDir, "upload-*")
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
	baseName := filepath.Base(tmp.Name()) + ext
	inboxPath := filepath.Join(inboxDir, baseName)
	if err := os.Rename(tmp.Name(), inboxPath); err != nil {
		os.Remove(tmp.Name())
		return err
	}

	log.Printf("[upload] received %s (%d bytes)", inboxPath, n)

	return nil
}

func jsonError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
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
