package cluster

import "encoding/json"

func mustJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}
