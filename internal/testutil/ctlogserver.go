package testutil

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// NewStubCTLogServer returns an httptest.Server that serves fixed STH/entries.
func NewStubCTLogServer(t *testing.T, sth, entries string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ct/v1/get-sth":
			w.Write([]byte(sth))
		case "/ct/v1/get-entries":
			w.Write([]byte(entries))
		default:
			t.Fatalf("unexpected CT log request: %s", r.URL.Path)
		}
	}))
}
