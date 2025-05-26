package sink

import "io"

type pipeSinkWriter struct {
	io.Writer
	io.Closer
}
