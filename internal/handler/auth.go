package handler

import (
	"encoding/json"
	"net/http"

	"starrocks-api/internal/config"
	"starrocks-api/internal/stream"
)

type AuthHandler struct {
	cfg *config.Config
}

func NewAuthHandler(cfg *config.Config) *AuthHandler {
	return &AuthHandler{cfg: cfg}
}

// Login validates username/password and returns a session token with role.
func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	type loginReq struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	var req loginReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	role, ok := h.cfg.ValidateLogin(req.Username, req.Password)
	if !ok {
		stream.WriteJSONError(w, http.StatusUnauthorized, "invalid username or password")
		return
	}

	// Find a token that matches this role
	token, found := h.cfg.FindTokenByRole(role)
	if !found {
		stream.WriteJSONError(w, http.StatusInternalServerError, "no token configured for role: "+role)
		return
	}

	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"token":   token,
		"role":    role,
	})
}
