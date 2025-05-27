package cluster

import "encoding/json"

func mustJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func keyHasSuffix(key, suffix string) bool {
	return len(key) >= len(suffix) && key[len(key)-len(suffix):] == suffix
}
