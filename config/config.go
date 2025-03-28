package config

import (
	"os"

	"github.com/BurntSushi/toml"
	"github.com/uopensail/ulib/commonconfig"
	"go.uber.org/zap"
)

// AppConfig holds the application configuration, including server and database settings.
type AppConfig struct {
	commonconfig.ServerConfig `json:"server" toml:"server"` // Common server configuration
	DataBaseConfig            string                        `json:"db_config" toml:"db_config"` // Path to database configuration file
}

// Init initializes the AppConfig instance by loading configuration from the specified path.
// It returns an error if the configuration cannot be loaded or parsed.
func (config *AppConfig) Init(configPath string) error {
	// Read the configuration file
	data, err := os.ReadFile(configPath)
	if err != nil {
		// Log error and return
		zap.L().Error("Failed to read config file", zap.String("path", configPath), zap.Error(err))
		return err
	}

	// Decode TOML data into the config structure
	if _, err := toml.Decode(string(data), config); err != nil {
		// Log error and return
		zap.L().Error("Failed to parse TOML config", zap.String("path", configPath), zap.Error(err))
		return err
	}

	return nil
}

// AppConfigInstance is the singleton instance for application configuration.
var AppConfigInstance AppConfig
