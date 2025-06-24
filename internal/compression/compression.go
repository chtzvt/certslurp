package compression

import (
	"compress/gzip"
	"fmt"
	"io"

	_ "embed"

	"github.com/dsnet/compress/bzip2"
	"github.com/klauspost/compress/zstd"
)

// NewWriter returns an io.WriteCloser that wraps w with the requested compression.
// Supported: "gzip", "bzip2", or "" (no compression).
func NewWriter(w io.WriteCloser, compression string) (io.WriteCloser, error) {
	var compressor io.WriteCloser
	var err error

	switch compression {
	case "gzip":
		compressor, err = gzip.NewWriter(w), nil
	case "bzip2":
		compressor, err = bzip2.NewWriter(w, &bzip2.WriterConfig{Level: bzip2.BestCompression})
	case "zstd":
		compressor, err = zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	case "", "none":
		compressor, err = nopWriteCloser{w}, nil
	}

	if err != nil || compressor == nil {
		return nil, fmt.Errorf("unsupported compression: %s", compression)
	}

	return &cascadeWriteCloser{compressor, w}, nil
}

// NewReader returns an io.Reader that wraps w with the requested compression.
// Supported: "gzip", "bzip2", or "" (no compression).
func NewReader(r io.Reader, compression string) (io.Reader, error) {
	switch compression {
	case "gzip":
		return gzip.NewReader(r)
	case "bzip2":
		return bzip2.NewReader(r, &bzip2.ReaderConfig{})
	case "zstd":
		return zstd.NewReader(r)
	case "", "none":
		return r, nil
	default:
		return nil, fmt.Errorf("unsupported compression: %s", compression)
	}
}
