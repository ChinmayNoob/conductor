// Package config loads Conductor's settings from environment variables.
//
// Every component reads the same Config and uses the parts it needs, so one
// .env file can drive the whole cluster.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// DevPrefix marks the throwaway secrets used by docker-compose.yml. Components
// warn loudly when they see one.
const DevPrefix = "insecure-dev-"

type Config struct {
	DB DB

	// Network addresses.
	// CoordinatorAddr lists where clients reach the coordinators, comma
	// separated; they find the leader among them.
	CoordinatorAddr   string
	CoordinatorListen string
	// CoordinatorAdvertiseAddr is where callers are redirected when this
	// coordinator leads; empty means this machine's IP and listen port.
	CoordinatorAdvertiseAddr string
	APIListen                string
	WorkerListen             string

	Worker Worker

	// ClusterToken authenticates gRPC calls between components.
	ClusterToken string
	// APIKey is a bootstrap admin key for the HTTP API, stored hashed on startup.
	APIKey string
	TLS    TLS

	LogLevel  slog.Level
	LogFormat string // "text" or "json"

	ShutdownTimeout time.Duration
	MaxOutputBytes  int
	MaxRequestBytes int64
	// PriorityAging raises a waiting task's priority one level per interval
	// so low-priority work can't starve. Zero disables it.
	PriorityAging time.Duration
}

type DB struct {
	Host     string
	Port     string
	User     string
	Password string
	Name     string
	SSLMode  string
}

func (d DB) DSN() string {
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(d.User, d.Password),
		Host:     d.Host + ":" + d.Port,
		Path:     d.Name,
		RawQuery: "sslmode=" + url.QueryEscape(d.SSLMode),
	}
	return u.String()
}

type Worker struct {
	ID            uint32 // 0 means derive from the advertised address
	AdvertiseAddr string // host:port the coordinator dials; empty means auto-detect
	Slots         int    // tasks run at once
	Labels        map[string]string
	// PassEnv names worker environment variables that tasks may see.
	PassEnv      []string
	DockerSocket string // enables container tasks when Docker answers here
}

// TLS secures gRPC between components. All components share one certificate
// whose SAN matches ServerName, so dynamic worker IPs still verify.
type TLS struct {
	CertFile   string
	KeyFile    string
	CAFile     string
	ServerName string
}

func (t TLS) Enabled() bool { return t.CertFile != "" }

// Load reads the configuration from the environment.
func Load() (*Config, error) {
	c := &Config{
		DB: DB{
			Host:     env("POSTGRES_HOST", "localhost"),
			Port:     env("POSTGRES_PORT", "5432"),
			User:     env("POSTGRES_USER", "postgres"),
			Password: env("POSTGRES_PASSWORD", "postgres"),
			Name:     env("POSTGRES_DB", "taskscheduler"),
			SSLMode:  env("POSTGRES_SSLMODE", "disable"),
		},
		CoordinatorAddr:          env("CONDUCTOR_COORDINATOR_ADDR", "localhost:8080"),
		CoordinatorListen:        env("CONDUCTOR_COORDINATOR_LISTEN", ":8080"),
		CoordinatorAdvertiseAddr: os.Getenv("CONDUCTOR_COORDINATOR_ADVERTISE_ADDR"),
		APIListen:                env("CONDUCTOR_API_LISTEN", ":8081"),
		WorkerListen:             env("CONDUCTOR_WORKER_LISTEN", ":9000"),
		Worker: Worker{
			AdvertiseAddr: os.Getenv("CONDUCTOR_WORKER_ADVERTISE_ADDR"),
			DockerSocket:  env("CONDUCTOR_DOCKER_SOCKET", "/var/run/docker.sock"),
			PassEnv:       splitList(os.Getenv("CONDUCTOR_WORKER_PASS_ENV")),
		},
		ClusterToken: os.Getenv("CONDUCTOR_CLUSTER_TOKEN"),
		APIKey:       os.Getenv("CONDUCTOR_API_KEY"),
		TLS: TLS{
			CertFile:   os.Getenv("CONDUCTOR_TLS_CERT"),
			KeyFile:    os.Getenv("CONDUCTOR_TLS_KEY"),
			CAFile:     os.Getenv("CONDUCTOR_TLS_CA"),
			ServerName: env("CONDUCTOR_TLS_SERVER_NAME", "conductor"),
		},
		LogFormat: env("CONDUCTOR_LOG_FORMAT", "text"),
	}

	var errs []error
	collect := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}

	if id := os.Getenv("CONDUCTOR_WORKER_ID"); id != "" {
		n, err := strconv.ParseUint(id, 10, 32)
		collect(wrap("CONDUCTOR_WORKER_ID", err))
		c.Worker.ID = uint32(n)
	}
	collect(wrap("CONDUCTOR_LOG_LEVEL", c.LogLevel.UnmarshalText([]byte(env("CONDUCTOR_LOG_LEVEL", "info")))))

	var err error
	c.ShutdownTimeout, err = time.ParseDuration(env("CONDUCTOR_SHUTDOWN_TIMEOUT", "25s"))
	collect(wrap("CONDUCTOR_SHUTDOWN_TIMEOUT", err))
	c.MaxOutputBytes, err = strconv.Atoi(env("CONDUCTOR_MAX_OUTPUT_BYTES", "1048576"))
	collect(wrap("CONDUCTOR_MAX_OUTPUT_BYTES", err))
	c.MaxRequestBytes, err = strconv.ParseInt(env("CONDUCTOR_MAX_REQUEST_BYTES", "1048576"), 10, 64)
	collect(wrap("CONDUCTOR_MAX_REQUEST_BYTES", err))
	c.PriorityAging, err = time.ParseDuration(env("CONDUCTOR_PRIORITY_AGING", "60s"))
	collect(wrap("CONDUCTOR_PRIORITY_AGING", err))
	c.Worker.Slots, err = strconv.Atoi(env("CONDUCTOR_WORKER_SLOTS", "2"))
	collect(wrap("CONDUCTOR_WORKER_SLOTS", err))
	if err == nil && c.Worker.Slots < 1 {
		collect(errors.New("CONDUCTOR_WORKER_SLOTS must be at least 1"))
	}
	c.Worker.Labels, err = parseLabels(os.Getenv("CONDUCTOR_WORKER_LABELS"))
	collect(wrap("CONDUCTOR_WORKER_LABELS", err))

	if c.LogFormat != "text" && c.LogFormat != "json" {
		collect(fmt.Errorf("CONDUCTOR_LOG_FORMAT must be text or json, got %q", c.LogFormat))
	}
	if c.ClusterToken == "" {
		collect(errors.New("CONDUCTOR_CLUSTER_TOKEN is required: it authenticates traffic between components"))
	}
	if (c.TLS.CertFile == "") != (c.TLS.KeyFile == "") {
		collect(errors.New("CONDUCTOR_TLS_CERT and CONDUCTOR_TLS_KEY must be set together"))
	}
	if c.TLS.CAFile != "" && !c.TLS.Enabled() {
		collect(errors.New("CONDUCTOR_TLS_CA requires CONDUCTOR_TLS_CERT and CONDUCTOR_TLS_KEY"))
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("invalid configuration: %w", errors.Join(errs...))
	}
	return c, nil
}

// WarnInsecure logs a warning for each setting that is fine for local
// development but unsafe in production.
func (c *Config) WarnInsecure(log *slog.Logger) {
	if strings.HasPrefix(c.ClusterToken, DevPrefix) {
		log.Warn("Using the development cluster token; set CONDUCTOR_CLUSTER_TOKEN in production")
	}
	if strings.HasPrefix(c.APIKey, DevPrefix) {
		log.Warn("Using the development API key; set CONDUCTOR_API_KEY in production")
	}
	if !c.TLS.Enabled() {
		log.Warn("gRPC traffic is not encrypted; set CONDUCTOR_TLS_CERT and CONDUCTOR_TLS_KEY in production")
	}
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

// parseLabels parses "k1=v1,k2=v2".
func parseLabels(s string) (map[string]string, error) {
	labels := make(map[string]string)
	for _, pair := range splitList(s) {
		k, v, ok := strings.Cut(pair, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			return nil, fmt.Errorf("label %q must look like key=value", pair)
		}
		if strings.HasPrefix(k, "type.") {
			return nil, fmt.Errorf("label %q: the type. prefix is reserved", k)
		}
		labels[k] = strings.TrimSpace(v)
	}
	return labels, nil
}

// splitList splits a comma-separated list, dropping empty items.
func splitList(s string) []string {
	var out []string
	for _, item := range strings.Split(s, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

func wrap(key string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", key, err)
}
