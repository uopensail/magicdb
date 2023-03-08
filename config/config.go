package config

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
	"github.com/uopensail/ulib/commonconfig"
)

type EtdcConfig struct {
	Address []string `json:"address" toml:"address"`
	TTL     int      `json:"ttl" toml:"ttl"`
}

type AppConfig struct {
	commonconfig.ServerConfig `json:"server" toml:"server"`
	WorkDir                   string `json:"work_dir" toml:"work_dir"`
	CacheSize                 int    `json:"cache_size" toml:"cache_size"`
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

var AppConfigInstance AppConfig
