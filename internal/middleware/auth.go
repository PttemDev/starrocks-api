package middleware

import (
	"context"
	"net/http"
	"strings"

	"starrocks-api/internal/config"
)

type contextKeyRole struct{}

// RequireAuth validates the Bearer token from the Authorization header
// against the predefined token list in config. Sets role in context.
func RequireAuth(cfg *config.Config) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			auth := r.Header.Get("Authorization")
			if auth == "" {
				http.Error(w, `{"error":"authorization required"}`, http.StatusUnauthorized)
				return
			}

			token := strings.TrimPrefix(auth, "Bearer ")
			if token == auth {
				// No "Bearer " prefix found
				http.Error(w, `{"error":"invalid authorization format, use Bearer <token>"}`, http.StatusUnauthorized)
				return
			}

			_, role, ok := cfg.ValidateToken(token)
			if !ok {
				http.Error(w, `{"error":"invalid or expired token"}`, http.StatusUnauthorized)
				return
			}

			ctx := context.WithValue(r.Context(), contextKeyRole{}, role)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireAdmin is a middleware that rejects non-admin users with 403.
func RequireAdmin() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			role := RoleFromContext(r.Context())
			if role != "admin" {
				http.Error(w, `{"error":"admin access required"}`, http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// RoleFromContext extracts the user role from request context.
func RoleFromContext(ctx context.Context) string {
	if role, ok := ctx.Value(contextKeyRole{}).(string); ok {
		return role
	}
	return "viewer"
}
