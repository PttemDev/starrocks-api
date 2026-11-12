package pool

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"starrocks-api/internal/config"
)

// Manager holds per-environment database connection pools.
type Manager struct {
	mu    sync.RWMutex
	pools map[string]*sql.DB
	envs  map[string]*config.Environment
}

// NewManager creates connection pools for all configured environments.
func NewManager(envs []config.Environment) (*Manager, error) {
	m := &Manager{
		pools: make(map[string]*sql.DB, len(envs)),
		envs:  make(map[string]*config.Environment, len(envs)),
	}

	for i := range envs {
		env := &envs[i]
		db, err := openDB(env)
		if err != nil {
			m.Close()
			return nil, fmt.Errorf("opening pool for %s: %w", env.Name, err)
		}
		m.pools[env.Name] = db
		m.envs[env.Name] = env
		slog.Info("connection pool created",
			"env", env.Name,
			"hosts", env.Hosts,
			"max_open", env.MaxOpenConns,
		)
	}

	return m, nil
}

// openDB creates a *sql.DB with failover support.
//
// go-sql-driver/mysql does not natively support multi-host DSNs.
// We register a custom dialer per environment that tries each host
// in order, providing transparent failover without application-level
// retry logic. If the first FE node is down, it automatically
// connects to the next available one.
func openDB(env *config.Environment) (*sql.DB, error) {
	// Register a custom dialer that tries each host in order
	dialerName := fmt.Sprintf("failover-%s", env.Name)
	hosts := env.Hosts
	mysql.RegisterDialContext(dialerName, func(ctx context.Context, _ string) (net.Conn, error) {
		var lastErr error
		d := net.Dialer{Timeout: 5 * time.Second}
		for _, addr := range hosts {
			conn, err := d.DialContext(ctx, "tcp", addr)
			if err == nil {
				return conn, nil
			}
			lastErr = err
			slog.Warn("failover: host unreachable, trying next",
				"env", env.Name, "host", addr, "err", err)
		}
		return nil, fmt.Errorf("all hosts unreachable: %w", lastErr)
	})

	// Use the first host as a placeholder in the DSN (the custom dialer ignores it)
	dsn := fmt.Sprintf("%s:%s@%s(failover)/%s?parseTime=true&interpolateParams=true&readTimeout=300s&writeTimeout=300s",
		env.User, env.Password, dialerName, env.Database,
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(env.MaxOpenConns)
	db.SetMaxIdleConns(env.MaxIdleConns)
	db.SetConnMaxLifetime(time.Duration(env.ConnMaxLifetimeSeconds) * time.Second)

	return db, nil
}

// Get returns the connection pool for the given environment name.
func (m *Manager) Get(envName string) (*sql.DB, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	db, ok := m.pools[envName]
	if !ok {
		return nil, fmt.Errorf("unknown environment: %s", envName)
	}
	return db, nil
}

// GetEnv returns the environment config for the given name.
func (m *Manager) GetEnv(envName string) (*config.Environment, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	env, ok := m.envs[envName]
	return env, ok
}

// Environments returns all environment names.
func (m *Manager) Environments() []config.Environment {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]config.Environment, 0, len(m.envs))
	for _, env := range m.envs {
		result = append(result, *env)
	}
	return result
}

// Close shuts down all connection pools.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for name, db := range m.pools {
		if err := db.Close(); err != nil {
			slog.Error("closing pool", "env", name, "err", err)
		}
	}
}
