package model

import (
	"os"

	"github.com/BurntSushi/toml"
	"go.uber.org/zap"
)

// DataBase represents a database configuration, including its name, working directory, and associated tables.
type DataBase struct {
	Name    string  `json:"name" toml:"name" yaml:"name"`          // Database name
	Workdir string  `json:"workdir" toml:"workdir" yaml:"workdir"` // Directory where the database operates
	Tables  []Table `json:"tables" toml:"tables" yaml:"tables"`    // List of tables in the database
}

// Table represents a single table in the database, including its name, data directory, and version.
type Table struct {
	Name    string `json:"name" toml:"name" yaml:"name"`          // Table name
	DataDir string `json:"data" toml:"data" yaml:"data"`          // Directory where table data is stored
	Version string `json:"version" toml:"version" yaml:"version"` // Table version
}

// LoadDataBaseConfig reads a TOML configuration file and unmarshals it into a DataBase struct.
// Returns the DataBase instance and an error if any occurred.
func LoadDataBaseConfig(configPath string) (*DataBase, error) {
	// Read the configuration file
	data, err := os.ReadFile(configPath)
	if err != nil {
		// Log error and return
		zap.L().Error("Failed to read config file", zap.String("path", configPath), zap.Error(err))
		return nil, err
	}

	// Parse the configuration data
	return parseDataBaseConfig(string(data), configPath)
}

// parseDataBaseConfig parses the TOML configuration string into a DataBase instance.
// It separates the parsing logic to make testing and debugging easier.
func parseDataBaseConfig(configData, configPath string) (*DataBase, error) {
	var config DataBase

	// Decode TOML data into the config structure
	if _, err := toml.Decode(configData, &config); err != nil {
		// Log error and return
		zap.L().Error("Failed to parse TOML config", zap.String("path", configPath), zap.Error(err))
		return nil, err
	}

	// Log success
	zap.L().Info("Successfully loaded database configuration", zap.String("path", configPath))

	return &config, nil
}
