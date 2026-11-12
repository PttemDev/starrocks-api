package handler

import (
	"net/http"

	"starrocks-api/internal/pool"
	"starrocks-api/internal/stream"
)

type EnvironmentsHandler struct {
	pools *pool.Manager
}

func NewEnvironmentsHandler(pools *pool.Manager) *EnvironmentsHandler {
	return &EnvironmentsHandler{pools: pools}
}

type envInfo struct {
	Name                 string `json:"name"`
	DisplayName          string `json:"display_name"`
	ReadOnly             bool   `json:"read_only"`
	DefaultReplicationNum int   `json:"default_replication_num"`
}

func (h *EnvironmentsHandler) List(w http.ResponseWriter, r *http.Request) {
	envs := h.pools.Environments()
	result := make([]envInfo, len(envs))
	for i, env := range envs {
		result[i] = envInfo{
			Name:                  env.Name,
			DisplayName:           env.DisplayName,
			ReadOnly:              env.ReadOnly,
			DefaultReplicationNum: env.DefaultReplicationNum,
		}
	}
	stream.WriteJSON(w, http.StatusOK, result)
}
