package sink

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/chtzvt/certslurp/internal/compression"
	"github.com/chtzvt/certslurp/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestHTTPSink_BasicPOST(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		gotBody = b
		w.WriteHeader(200)
	}))
	defer srv.Close()

	opts := map[string]interface{}{
		"endpoint": srv.URL,
	}
	sink, err := NewHTTPSink(opts, nil)
	require.NoError(t, err)
	w, err := sink.Open(context.Background(), "testfile")
	require.NoError(t, err)

	payload := "post this data"
	n, err := w.Write([]byte(payload))
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
	require.NoError(t, w.Close())
	require.Equal(t, payload, string(gotBody))
}

func TestHTTPSink_Compressions(t *testing.T) {
	compressions := []struct {
		name        string
		compression string
		header      string
	}{
		{"gzip", "gzip", "gzip"},
		{"bzip2", "bzip2", "x-bzip2"},
		{"zstd", "zstd", "zstd"},
	}
	for _, c := range compressions {
		t.Run(c.name, func(t *testing.T) {
			var gotBody []byte
			var gotHeader string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotHeader = r.Header.Get("Content-Encoding")
				b, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				gotBody = b
				w.WriteHeader(200)
			}))
			defer srv.Close()

			opts := map[string]interface{}{
				"endpoint":    srv.URL,
				"compression": c.compression,
			}
			sink, err := NewHTTPSink(opts, nil)
			require.NoError(t, err)
			w, err := sink.Open(context.Background(), "file")
			require.NoError(t, err)

			payload := "compressed payload"
			var compressed testutil.WriteCloserBuffer
			compWriter, err := compression.NewWriter(&compressed, c.compression)
			require.NoError(t, err)
			_, err = compWriter.Write([]byte(payload))
			require.NoError(t, err)
			require.NoError(t, compWriter.Close())

			n, err := w.Write(compressed.Bytes())
			require.NoError(t, err)
			require.Equal(t, compressed.Len(), n)
			require.NoError(t, w.Close())
			require.Equal(t, c.header, gotHeader)

			// decompress and check
			cr, err := compression.NewReader(bytes.NewReader(gotBody), c.compression)
			require.NoError(t, err)
			plain, err := io.ReadAll(cr)
			require.NoError(t, err)
			require.Equal(t, payload, string(plain))
		})
	}
}

func TestHTTPSink_CustomHeaders(t *testing.T) {
	var gotFoo string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotFoo = r.Header.Get("X-Foo")
		w.WriteHeader(200)
	}))
	defer srv.Close()

	opts := map[string]interface{}{
		"endpoint": srv.URL,
		"headers": map[string]interface{}{
			"X-Foo": "bar",
		},
	}
	sink, err := NewHTTPSink(opts, nil)
	require.NoError(t, err)
	w, err := sink.Open(context.Background(), "hdr")
	require.NoError(t, err)
	_, err = w.Write([]byte("abc"))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.Equal(t, "bar", gotFoo)
}

func TestHTTPSink_Retries(t *testing.T) {
	var count int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		if count < 3 {
			w.WriteHeader(500)
		} else {
			w.WriteHeader(200)
		}
	}))
	defer srv.Close()

	opts := map[string]interface{}{
		"endpoint":    srv.URL,
		"max_retries": 5.0,
	}
	sink, err := NewHTTPSink(opts, nil)
	require.NoError(t, err)
	w, err := sink.Open(context.Background(), "retry")
	require.NoError(t, err)
	_, err = w.Write([]byte("retry"))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.GreaterOrEqual(t, count, 3) // At least 3 tries (2 fail, 1 success)
}
