// Package config loads and validates application configuration.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

// Config contains all runtime configuration for hopper.
type Config struct {
	App      AppConfig      `mapstructure:"app" validate:"required"`
	Server   ServerConfig   `mapstructure:"server" validate:"required"`
	Database DatabaseConfig `mapstructure:"database" validate:"required"`
	Worker   WorkerConfig   `mapstructure:"worker" validate:"required"`
	Reaper   ReaperConfig   `mapstructure:"reaper" validate:"required"`
	Retry    RetryConfig    `mapstructure:"retry" validate:"required"`
	Log      LogConfig      `mapstructure:"log" validate:"required"`
	Metrics  MetricsConfig  `mapstructure:"metrics" validate:"required"`
}

// AppConfig defines application-level settings.
type AppConfig struct {
	Name            string        `mapstructure:"name" validate:"required"`
	Env             string        `mapstructure:"env" validate:"required,oneof=development staging production test"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout" validate:"gt=0"`
}

// ServerConfig defines HTTP server settings.
type ServerConfig struct {
	Host         string        `mapstructure:"host" validate:"required"`
	Port         int           `mapstructure:"port" validate:"required,min=1,max=65535"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout" validate:"gt=0"`
	WriteTimeout time.Duration `mapstructure:"write_timeout" validate:"gt=0"`
	BodyLimit    string        `mapstructure:"body_limit" validate:"required"`
}

// DatabaseConfig defines Postgres connection pool settings.
type DatabaseConfig struct {
	Host            string        `mapstructure:"host" validate:"required"`
	Port            int           `mapstructure:"port" validate:"required,min=1,max=65535"`
	User            string        `mapstructure:"user" validate:"required"`
	Password        string        `mapstructure:"password" validate:"required"`
	Name            string        `mapstructure:"name" validate:"required"`
	SSLMode         string        `mapstructure:"sslmode" validate:"required,oneof=disable require verify-ca verify-full"`
	MaxConns        int32         `mapstructure:"max_conns" validate:"gt=0"`
	MinConns        int32         `mapstructure:"min_conns" validate:"gte=0"`
	MaxConnLifetime time.Duration `mapstructure:"max_conn_lifetime" validate:"gt=0"`
}

// WorkerConfig defines worker process settings.
type WorkerConfig struct {
	ID                string        `mapstructure:"id"`
	Queues            []string      `mapstructure:"queues" validate:"required,min=1,dive,required"`
	Concurrency       int           `mapstructure:"concurrency" validate:"gt=0"`
	BatchSize         int           `mapstructure:"batch_size" validate:"gt=0"`
	PollInterval      time.Duration `mapstructure:"poll_interval" validate:"gt=0"`
	LeaseTTL          time.Duration `mapstructure:"lease_ttl" validate:"gt=0"`
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval" validate:"gt=0"`
	JobTimeout        time.Duration `mapstructure:"job_timeout" validate:"gt=0"`
}

// ReaperConfig defines lease reaper settings.
type ReaperConfig struct {
	Interval  time.Duration `mapstructure:"interval" validate:"gt=0"`
	BatchSize int           `mapstructure:"batch_size" validate:"gt=0"`
}

// MetricsConfig defines the standalone /metrics listener used by processes
// without their own HTTP server (the worker; the API serves /metrics on its
// main port instead).
type MetricsConfig struct {
	Port int `mapstructure:"port" validate:"required,min=1,max=65535"`
}

// RetryConfig defines retry and backoff settings.
type RetryConfig struct {
	MaxAttempts int           `mapstructure:"max_attempts" validate:"gt=0"`
	BaseDelay   time.Duration `mapstructure:"base_delay" validate:"gt=0"`
	MaxDelay    time.Duration `mapstructure:"max_delay" validate:"gtefield=BaseDelay"`
	Jitter      float64       `mapstructure:"jitter" validate:"gte=0,lte=1"`
}

// LogConfig defines structured logging settings.
type LogConfig struct {
	Level  string `mapstructure:"level" validate:"required,oneof=debug info warn error"`
	Format string `mapstructure:"format" validate:"required,oneof=json text"`
}

// Load parses configuration once at startup and validates all fields.
func Load(configPath string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(configPath)
	v.SetEnvPrefix("HOPPER")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshalling config: %w", err)
	}

	if cfg.Worker.ID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("reading hostname for worker id default: %w", err)
		}
		cfg.Worker.ID = hostname
	}

	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return &cfg, nil
}

func validateConfig(cfg Config) error {
	validate := validator.New()
	if err := validate.Struct(cfg); err != nil {
		return fmt.Errorf("struct validation failed: %w", err)
	}

	if cfg.Database.MinConns > cfg.Database.MaxConns {
		return fmt.Errorf("database.min_conns must be <= database.max_conns")
	}

	if cfg.Worker.HeartbeatInterval >= cfg.Worker.LeaseTTL {
		return fmt.Errorf("worker.heartbeat_interval must be < worker.lease_ttl")
	}

	if cfg.Worker.BatchSize > cfg.Worker.Concurrency {
		return fmt.Errorf("worker.batch_size must be <= worker.concurrency")
	}

	return nil
}
