package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/chtzvt/certslurp/internal/api"
)

func cliClient() *api.Client {
	c := api.NewClient(apiURL, apiToken)
	c.Client.Timeout = timeout
	return c
}

func outResult(v any, printer func(any)) {
	if outputJSON {
		b, _ := json.MarshalIndent(v, "", "  ")
		fmt.Println(string(b))
	} else {
		printer(v)
	}
}

func valOrDash(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format("2006-01-02 15:04:05")
}

func promptString(r *bufio.Reader, def string) string {
	text, _ := r.ReadString('\n')
	text = strings.TrimSpace(text)
	if text == "" {
		return def
	}
	return text
}

func promptInt(r *bufio.Reader, def int) int {
	s := promptString(r, fmt.Sprintf("%d", def))
	v, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return v
}

func promptInt64(r *bufio.Reader, def int64) int64 {
	s := promptString(r, fmt.Sprintf("%d", def))
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return def
	}
	return v
}

func promptBool(r *bufio.Reader, def bool) bool {
	s := strings.ToLower(promptString(r, ""))
	if s == "" {
		return def
	}
	return s[0] == 'y'
}

func parseOptions(str string) (map[string]interface{}, error) {
	if str == "" {
		return nil, nil
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(str), &m); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	return m, nil
}
