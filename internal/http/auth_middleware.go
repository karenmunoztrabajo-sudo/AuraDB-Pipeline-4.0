package http

import (
	"context"
	"net/http"
	"strings"

	"auradb-pipeline/internal/ipc"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

type contextKey string

const IPCKey contextKey = "ipc"

func AuthMiddleware(jwtSecret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tokenStr := extractAuthToken(r)
			if tokenStr == "" {
				http.Error(w, "faltó Authorization", http.StatusUnauthorized)
				return
			}

			token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
				return []byte(jwtSecret), nil
			})
			if err != nil || !token.Valid {
				http.Error(w, "token inválido", http.StatusUnauthorized)
				return
			}

			claims, ok := token.Claims.(jwt.MapClaims)
			if !ok {
				http.Error(w, "claims inválidos", http.StatusUnauthorized)
				return
			}

			ctxIPC := ipc.Context{
				TenantID:      toString(claims["tenant_id"]),
				UserID:        toString(claims["user_id"]),
				Roles:         []string{toString(claims["role"])},
				PolicyVersion: "v1",
				RequestID:     uuid.New().String(),
				AuditNonce:    uuid.New().String(),
			}

			ctx := context.WithValue(r.Context(), IPCKey, ctxIPC)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func GetIPC(r *http.Request) (ipc.Context, bool) {
	val := r.Context().Value(IPCKey)
	if val == nil {
		return ipc.Context{}, false
	}

	ctxIPC, ok := val.(ipc.Context)
	return ctxIPC, ok
}

func toString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func extractAuthToken(r *http.Request) string {
	authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
	if authHeader != "" {
		parts := strings.Fields(authHeader)
		if len(parts) == 1 {
			return parts[0]
		}
		if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
			return parts[1]
		}
		return ""
	}

	for _, cookieName := range []string{"token", "auth_token", "auradb.authToken"} {
		cookie, err := r.Cookie(cookieName)
		if err == nil && strings.TrimSpace(cookie.Value) != "" {
			return strings.TrimSpace(cookie.Value)
		}
	}

	return ""
}
