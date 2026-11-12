package middleware

import (
	"net/http"
	"strings"
)

// CORS returns a middleware that sets CORS headers for allowed origins.
// Supports exact matches and wildcard suffix patterns like "*.example.com".
func CORS(allowedOrigins []string) func(http.Handler) http.Handler {
	exact := make(map[string]bool)
	var suffixes []string

	for _, o := range allowedOrigins {
		o = strings.TrimRight(o, "/")
		if strings.HasPrefix(o, "*.") {
			// *.example.com → match anything ending with .example.com
			suffixes = append(suffixes, o[1:]) // store ".example.com"
		} else {
			exact[o] = true
		}
	}

	isAllowed := func(origin string) bool {
		if exact[origin] {
			return true
		}
		// Check https://sub.example.com against .example.com suffix
		for _, s := range suffixes {
			// Extract the host part from origin (strip scheme)
			host := origin
			if idx := strings.Index(host, "://"); idx >= 0 {
				host = host[idx+3:]
			}
			if strings.HasSuffix(host, s) || host == s[1:] {
				return true
			}
		}
		return false
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if isAllowed(origin) {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Environment, Authorization")
				w.Header().Set("Access-Control-Max-Age", "86400")
			}

			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
