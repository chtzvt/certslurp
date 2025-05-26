package compression

import (
	"compress/gzip"
	"fmt"
	"io"

	"github.com/dsnet/compress/bzip2"
)

// NewWriter returns an io.WriteCloser that wraps w with the requested compression.
// Supported: "gzip", "bzip2", or "" (no compression).
func NewWriter(w io.Writer, compression string) (io.WriteCloser, error) {
	switch compression {
	case "gzip":
		return gzip.NewWriter(w), nil
	case "bzip2":
		return bzip2.NewWriter(w, &bzip2.WriterConfig{Level: bzip2.BestCompression})
	case "", "none":
		return nopWriteCloser{w}, nil
	default:
		return nil, fmt.Errorf("unsupported compression: %s", compression)
	}
}

type nopWriteCloser struct{ io.Writer }

func (n nopWriteCloser) Write(p []byte) (int, error) { return n.Writer.Write(p) }
func (n nopWriteCloser) Close() error                { return nil }
