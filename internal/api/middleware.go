package api

import (
	"net/http"
	"strings"
)

func TokenAuthMiddleware(tokens []string, next http.Handler) http.Handler {
	allowed := make(map[string]struct{}, len(tokens))
	for _, t := range tokens {
		allowed[t] = struct{}{}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			http.Error(w, "Unauthorized: missing Bearer token", http.StatusUnauthorized)
			return
		}
		token := strings.TrimPrefix(auth, "Bearer ")
		token = strings.TrimSpace(token)

		if _, ok := allowed[token]; !ok {
			http.Error(w, "Unauthorized: invalid token", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
