package sink

import "io"

type pipeSinkWriter struct {
	io.Writer
	io.Closer
}

func toBool(val interface{}) bool {
	switch v := val.(type) {
	case bool:
		return v
	case int:
		return v != 0
	case string:
		return v == "1" || v == "true" || v == "on"
	default:
		return false
	}
}
