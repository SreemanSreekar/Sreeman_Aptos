package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Port                  string
	ReplicaID             string
	DatabaseURL           string
	AdminDatabaseURL      string
	AllowSimulatedClock   bool
	MaxFutureSkewSeconds  int
	MaxEventLatenessHours int
}

func Load() (*Config, error) {
	cfg := &Config{
		Port:                  envOr("PORT", "8080"),
		ReplicaID:             envOr("REPLICA_ID", "api-1"),
		DatabaseURL:           os.Getenv("DATABASE_URL"),
		AdminDatabaseURL:      os.Getenv("ADMIN_DATABASE_URL"),
		AllowSimulatedClock:   envBool("ALLOW_SIMULATED_CLOCK", false),
		MaxFutureSkewSeconds:  envInt("MAX_FUTURE_SKEW_SECONDS", 300),
		MaxEventLatenessHours: envInt("MAX_EVENT_LATENESS_HOURS", 48),
	}
	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.AdminDatabaseURL == "" {
		return nil, fmt.Errorf("ADMIN_DATABASE_URL is required")
	}
	return cfg, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}
