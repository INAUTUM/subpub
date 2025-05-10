package config

import (
	"errors"
	"fmt"

	"github.com/spf13/viper"
)

type Config struct {
    GRPCPort   int    `mapstructure:"GRPC_PORT"`
    LogLevel   string `mapstructure:"LOG_LEVEL"`
    LogFormat  string `mapstructure:"LOG_FORMAT"`
    ServiceName string `mapstructure:"SERVICE_NAME"`
}

func Load() (*Config, error) {
    viper.SetConfigFile("config.yaml")
    viper.AutomaticEnv()
    
    if err := viper.ReadInConfig(); err != nil {
        return nil, fmt.Errorf("failed to read config: %w", err)
    }
    
    var cfg Config
    if err := viper.Unmarshal(&cfg); err != nil {
        return nil, fmt.Errorf("failed to unmarshal config: %w", err)
    }
    
    if err := cfg.Validate(); err != nil {
        return nil, fmt.Errorf("config validation failed: %w", err)
    }
    
    return &cfg, nil
}

func (c *Config) Validate() error {
    if c.GRPCPort < 1 || c.GRPCPort > 65535 {
        return errors.New("GRPC_PORT must be between 1 and 65535")
    }

    validLevels := map[string]bool{
        "debug": true,
        "info":  true,
        "warn":  true,
        "error": true,
    }
    if !validLevels[c.LogLevel] {
        return errors.New("invalid LOG_LEVEL, must be one of: debug, info, warn, error")
    }

    if c.LogFormat != "json" && c.LogFormat != "console" {
        return errors.New("LOG_FORMAT must be either 'json' or 'console'")
    }

    return nil
}