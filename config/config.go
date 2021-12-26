package config

import (
	"encoding/json"
	"github.com/BurntSushi/toml"
	fconfig "github.com/uopensail/fuku-core/config"
	"github.com/uopensail/ulib/commonconfig"
	"github.com/uopensail/ulib/zlog"
	"go.uber.org/zap"
	"io/ioutil"
)

type MagicDBConfig struct {
	Name       string                     `json:"name" toml:"name"`
	Partitions []string                   `json:"partitions" toml:"partitions"`
	Version    int64                      `json:"versions" toml:"versions"`
	Features   map[string]fconfig.Feature `json:"features" toml:"features"`
}

func (mc *MagicDBConfig) Init(filepath string) bool {
	fileData, err := ioutil.ReadFile(filepath)
	if err != nil {
		zlog.LOG.Error("MagicDBConfig.Init", zap.String("filepath", filepath), zap.Error(err))
		return false
	}
	err = json.Unmarshal(fileData, mc)
	if err != nil {
		zlog.LOG.Error("MagicDBConfig.Init", zap.String("data", string(fileData)), zap.Error(err))
		return false
	}
	return true
}

func (mc *MagicDBConfig) Dump(filepath string) bool {
	data, err := json.Marshal(mc)
	if err != nil {
		zlog.LOG.Error("MagicDBConfig.Dump", zap.String("data", string(data)), zap.Error(err))
		return false
	}
	err = ioutil.WriteFile(filepath, data, 0644)
	if err != nil {
		zlog.LOG.Error("MagicDBConfig.Dump", zap.String("filepath", filepath), zap.Error(err))
		return false
	}
	return true
}

type AppConfig struct {
	commonconfig.ServerConfig `json:",inline" toml:",inline"`
	Sources                   map[string]commonconfig.DownloaderConfig `json:"sources" toml:"sources"`
	TTL                       int                                      `json:"cache_ttl" toml:"cache_ttl"`
	CacheSize                 int                                      `json:"cache_size" toml:"cache_size"`
	LogPath                   string                                   `json:"log_path" toml:"log_path"`
}

func (config *AppConfig) Init(configPath string) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		panic(err)
	}
	if _, err = toml.Decode(string(data), config); err != nil {
		panic(err)
	}
}

var AppConfigImp AppConfig
