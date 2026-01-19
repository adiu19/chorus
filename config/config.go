package config

import (
	"fmt"
	"net"
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for a chorus node.
type Config struct {
	// Seeds is a list of addresses (host:port) to contact when joining the cluster.
	Seeds []string `yaml:"seeds"`
}

// Load reads a YAML config file from the given path and returns the parsed Config.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// validate checks that all config values are valid.
func (c *Config) validate() error {
	for _, seed := range c.Seeds {
		host, port, err := net.SplitHostPort(seed)
		if err != nil {
			return fmt.Errorf("invalid seed %q: %w", seed, err)
		}
		if host == "" {
			return fmt.Errorf("invalid seed %q: host cannot be empty", seed)
		}
		if port == "" {
			return fmt.Errorf("invalid seed %q: port cannot be empty", seed)
		}
	}
	return nil
}
