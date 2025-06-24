package compression

import "io"

type nopWriteCloser struct{ io.Writer }

func (n nopWriteCloser) Write(p []byte) (int, error) { return n.Writer.Write(p) }
func (n nopWriteCloser) Close() error                { return nil }

type cascadeWriteCloser struct {
	compressor io.WriteCloser
	underlying io.Closer
}

func (c *cascadeWriteCloser) Write(p []byte) (int, error) {
	return c.compressor.Write(p)
}
func (c *cascadeWriteCloser) Close() error {
	err1 := c.compressor.Close()
	err2 := c.underlying.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
