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
	commonconfig.ServerConfig `json:",inline" toml:",inline"`
	WorkDir                   string     `json:"work_dir" toml:"work_dir"`
	LogDir                    string     `json:"log_dir" toml:"log_dir"`
	UseCache                  bool       `json:"use_cache" toml:"use_cache"`
	CacheTTL                  int64      `json:"cache_ttl" toml:"cache_ttl"`
	CacheSize                 int        `json:"cache_size" toml:"cache_size"`
	Etcdconfig                EtdcConfig `json:"etcd" toml:"etcd"`
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
