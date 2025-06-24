package sink

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/chtzvt/certslurp/internal/secrets"
)

type HTTPSink struct {
	endpoint    string
	compression string
	maxRetries  int
	headers     map[string]string
	client      *http.Client
}

func NewHTTPSink(opts map[string]interface{}, secrets *secrets.Store) (Sink, error) {
	endpoint, ok := opts["endpoint"].(string)
	if !ok || endpoint == "" {
		return nil, errors.New("http sink requires 'endpoint' option")
	}
	compression, _ := opts["compression"].(string)
	if compression == "" {
		compression = "none"
	}
	maxRetries := 3
	if v, ok := opts["max_retries"].(float64); ok && v > 0 {
		maxRetries = int(v)
	}
	headers := map[string]string{}
	if m, ok := opts["headers"].(map[string]interface{}); ok {
		for k, v := range m {
			if s, ok := v.(string); ok {
				headers[k] = s
			}
		}
	}
	return &HTTPSink{
		endpoint:    endpoint,
		compression: compression,
		maxRetries:  maxRetries,
		headers:     headers,
		client:      &http.Client{Timeout: 120 * time.Second},
	}, nil
}

type httpSinkWriter struct {
	sink   *HTTPSink
	ctx    context.Context
	buf    *bytes.Buffer
	closed bool
}

func (s *HTTPSink) Open(ctx context.Context, name string) (SinkWriter, error) {
	return &httpSinkWriter{
		sink: s,
		ctx:  ctx,
		buf:  &bytes.Buffer{},
	}, nil
}

func (w *httpSinkWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("sinkwriter closed")
	}
	return w.buf.Write(p)
}

func (w *httpSinkWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	// Retry logic
	for attempt := 1; attempt <= w.sink.maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(w.ctx, "POST", w.sink.endpoint, bytes.NewReader(w.buf.Bytes()))
		if err != nil {
			return err
		}
		for k, v := range w.sink.headers {
			req.Header.Set(k, v)
		}
		// Set compression headers for already-compressed content
		switch w.sink.compression {
		case "gzip":
			req.Header.Set("Content-Encoding", "gzip")
		case "bzip2":
			req.Header.Set("Content-Encoding", "x-bzip2")
		case "zstd":
			req.Header.Set("Content-Encoding", "zstd")
		}
		resp, err := w.sink.client.Do(req)
		if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			_ = resp.Body.Close()
			return nil
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		time.Sleep(time.Duration(attempt*200) * time.Millisecond)
	}
	return errors.New("all HTTP POST attempts failed")
}

func init() {
	Register("http", NewHTTPSink)
}
