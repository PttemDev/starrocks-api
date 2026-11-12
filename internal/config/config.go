package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type ServerConfig struct {
	Host                string   `json:"host"`
	Port                int      `json:"port"`
	ReadTimeoutSeconds  int      `json:"read_timeout_seconds"`
	WriteTimeoutSeconds int      `json:"write_timeout_seconds"`
	CORSOrigins         []string `json:"cors_origins"`
}

type Environment struct {
	Name                   string   `json:"name"`
	DisplayName            string   `json:"display_name"`
	Host                   string   `json:"host"`
	Port                   int      `json:"port"`
	Hosts                  []string `json:"hosts"`
	User                   string   `json:"user"`
	Password               string   `json:"password"`
	Database               string   `json:"database"`
	MaxOpenConns           int      `json:"max_open_conns"`
	MaxIdleConns           int      `json:"max_idle_conns"`
	ConnMaxLifetimeSeconds int      `json:"conn_max_lifetime_seconds"`
	ReadOnly               bool     `json:"read_only"`
	DefaultReplicationNum  int      `json:"default_replication_num"`
	FEHttpPort             int      `json:"fe_http_port"`
}

type AuthToken struct {
	Tenant string `json:"tenant"`
	Token  string `json:"token"`
	Role   string `json:"role"` // "admin" or "viewer"
}

type LoginCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Role     string `json:"role"` // "admin" or "viewer"
}

type Config struct {
	Server       ServerConfig     `json:"server"`
	Environments []Environment    `json:"environments"`
	AuthTokens   []AuthToken      `json:"auth_tokens"`
	LoginUsers   []LoginCredentials `json:"login_users"`
}

// Load reads and parses the config file at the given path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	cfg.applyDefaults()
	return &cfg, nil
}

func (c *Config) validate() error {
	if len(c.Environments) == 0 {
		return fmt.Errorf("at least one environment is required")
	}
	seen := make(map[string]bool)
	for i, env := range c.Environments {
		if env.Name == "" {
			return fmt.Errorf("environment[%d]: name is required", i)
		}
		if seen[env.Name] {
			return fmt.Errorf("duplicate environment name: %s", env.Name)
		}
		seen[env.Name] = true
		// Must have either host+port or hosts[]
		if env.Host == "" && len(env.Hosts) == 0 {
			return fmt.Errorf("environment[%s]: host or hosts[] is required", env.Name)
		}
		if env.Host != "" && env.Port == 0 {
			return fmt.Errorf("environment[%s]: port is required when host is set", env.Name)
		}
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.Server.Host == "" {
		c.Server.Host = "127.0.0.1"
	}
	if c.Server.Port == 0 {
		c.Server.Port = 8080
	}
	if c.Server.ReadTimeoutSeconds == 0 {
		c.Server.ReadTimeoutSeconds = 30
	}
	if c.Server.WriteTimeoutSeconds == 0 {
		c.Server.WriteTimeoutSeconds = 300
	}

	for i := range c.Environments {
		env := &c.Environments[i]
		if env.DisplayName == "" {
			env.DisplayName = env.Name
		}
		// Database can be empty - StarRocks allows connection without a default db
		if env.MaxOpenConns == 0 {
			env.MaxOpenConns = 25
		}
		if env.MaxIdleConns == 0 {
			env.MaxIdleConns = 5
		}
		if env.ConnMaxLifetimeSeconds == 0 {
			env.ConnMaxLifetimeSeconds = 300
		}
		if env.DefaultReplicationNum == 0 {
			env.DefaultReplicationNum = 3
		}
		if env.FEHttpPort == 0 {
			env.FEHttpPort = 8030
		}
		// Normalize: if single host+port is set but hosts[] is empty, populate hosts[]
		if len(env.Hosts) == 0 && env.Host != "" {
			env.Hosts = []string{fmt.Sprintf("%s:%d", env.Host, env.Port)}
		}
	}
}

// ValidateToken checks if a token is valid and returns its tenant name and role.
func (c *Config) ValidateToken(token string) (tenant string, role string, ok bool) {
	for _, t := range c.AuthTokens {
		if t.Token == token {
			r := t.Role
			if r == "" {
				r = "admin"
			}
			return t.Tenant, r, true
		}
	}
	return "", "", false
}

// ValidateLogin checks username/password and returns the role if valid.
func (c *Config) ValidateLogin(username, password string) (string, bool) {
	for _, u := range c.LoginUsers {
		if u.Username == username && u.Password == password {
			role := u.Role
			if role == "" {
				role = "admin"
			}
			return role, true
		}
	}
	return "", false
}

// FindTokenByRole returns the first auth token matching the given role.
func (c *Config) FindTokenByRole(role string) (string, bool) {
	for _, t := range c.AuthTokens {
		r := t.Role
		if r == "" {
			r = "admin"
		}
		if r == role {
			return t.Token, true
		}
	}
	return "", false
}

// EnvByName returns the environment config by its name.
func (c *Config) EnvByName(name string) (*Environment, bool) {
	for i := range c.Environments {
		if c.Environments[i].Name == name {
			return &c.Environments[i], true
		}
	}
	return nil, false
}
