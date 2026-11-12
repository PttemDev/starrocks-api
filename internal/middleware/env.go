package middleware

import (
	"context"
	"net/http"

	"starrocks-api/internal/pool"
)

type contextKey string

const envContextKey contextKey = "environment"

// EnvFromContext extracts the environment name from the request context.
func EnvFromContext(ctx context.Context) string {
	v, _ := ctx.Value(envContextKey).(string)
	return v
}

// RequireEnv is a middleware that extracts X-Environment header and validates
// that the environment exists in the pool manager.
func RequireEnv(pm *pool.Manager) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			envName := r.Header.Get("X-Environment")
			if envName == "" {
				envName = r.URL.Query().Get("env")
			}
			if envName == "" {
				http.Error(w, `{"error":"X-Environment header or env query param is required"}`, http.StatusBadRequest)
				return
			}

			if _, err := pm.Get(envName); err != nil {
				http.Error(w, `{"error":"unknown environment: `+envName+`"}`, http.StatusBadRequest)
				return
			}

			ctx := context.WithValue(r.Context(), envContextKey, envName)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
