package handler

import (
	"context"
	"net/http"
	"sync"
	"time"

	"starrocks-api/internal/pool"
	"starrocks-api/internal/stream"
)

// Kubernetes probes default to timeoutSeconds: 1, so the readiness check has to
// answer well inside a second. Each environment is pinged concurrently with its
// own budget, which keeps the wall time at one ping regardless of how many
// environments are configured or how many of them are blackholed.
const readyPingTimeout = 700 * time.Millisecond

type HealthHandler struct {
	pools   *pool.Manager
	startAt time.Time
}

func NewHealthHandler(pools *pool.Manager) *HealthHandler {
	return &HealthHandler{pools: pools, startAt: time.Now()}
}

// Live is the liveness probe: it answers 200 whenever the process is running and
// the HTTP server is accepting connections. It deliberately touches no database —
// a failing liveness probe restarts the pod, and restarting cannot fix a cluster
// that is unreachable.
func (h *HealthHandler) Live(w http.ResponseWriter, r *http.Request) {
	stream.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Startup is the startup probe. Connection pools are opened lazily (`sql.Open`
// does not dial), so the server is usable the moment it binds and this is
// equivalent to Live. It exists so the platform chart can enable a startup probe
// without a 404, not because the app has a slow boot.
func (h *HealthHandler) Startup(w http.ResponseWriter, r *http.Request) {
	stream.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type envStatus struct {
	Name  string `json:"name"`
	Ready bool   `json:"ready"`
	Error string `json:"error,omitempty"`
	LatMs int64  `json:"latency_ms"`
}

// Ready is the readiness probe: may this pod receive traffic?
//
// It answers 200 when at least one configured environment is reachable, and 503
// only when none are. Requiring *every* environment would be wrong on two counts:
// each config file carries both clusters (a prod pod also lists test), so a pod
// that serves prod perfectly would be pulled from the load balancer because the
// other cluster is unreachable; and because every replica shares one config, a
// blip on a shared cluster would take all replicas out at once and stall the
// rollout instead of shedding load. Per-environment detail stays in the body for
// dashboards and alerting, which is where a partial outage belongs.
func (h *HealthHandler) Ready(w http.ResponseWriter, r *http.Request) {
	envs := h.pools.Environments()
	results := make([]envStatus, len(envs))

	var wg sync.WaitGroup
	for i, env := range envs {
		wg.Add(1)
		go func(i int, name string) {
			defer wg.Done()
			results[i] = h.pingEnv(r.Context(), name)
		}(i, env.Name)
	}
	wg.Wait()

	anyReady := false
	for _, res := range results {
		if res.Ready {
			anyReady = true
			break
		}
	}

	status, statusStr := http.StatusOK, "ready"
	if !anyReady {
		status, statusStr = http.StatusServiceUnavailable, "not_ready"
	}

	stream.WriteJSON(w, status, map[string]interface{}{
		"status":       statusStr,
		"uptime":       time.Since(h.startAt).Round(time.Second).String(),
		"environments": results,
	})
}

func (h *HealthHandler) pingEnv(parent context.Context, name string) envStatus {
	db, err := h.pools.Get(name)
	if err != nil {
		return envStatus{Name: name, Ready: false, Error: err.Error()}
	}

	ctx, cancel := context.WithTimeout(parent, readyPingTimeout)
	defer cancel()

	start := time.Now()
	err = db.PingContext(ctx)
	res := envStatus{
		Name:  name,
		Ready: err == nil,
		LatMs: time.Since(start).Milliseconds(),
	}
	if err != nil {
		res.Error = err.Error()
	}
	return res
}
