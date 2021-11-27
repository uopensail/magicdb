package config

import (
	"encoding/json"
	"github.com/BurntSushi/toml"
	"github.com/uopensail/ulib/commonconfig"
	"github.com/uopensail/ulib/zlog"
	"go.uber.org/zap"
	"io/ioutil"
)

type DataType int

const (
	StringType DataType = iota + 1
	StringListType
	Int64Type
	Int64ListType
	Float32Type
	Float32ListType
)

//ServiceConfig 定义服务的一些配置结构
type ServiceConfig struct {
	PProfPort   int    `json:"pprof_port" yaml:"pprof_port" toml:"pprof_port"`
	PromePort   int    `json:"prome_port" yaml:"prome_port" toml:"prome_port"`
	Debug       bool   `json:"debug" yaml:"debug" toml:"debug"`
	ServiceName string `json:"service_name" yaml:"service_name" toml:"service_name"`
	GRPCPort    int    `json:"grpc_port" yaml:"grpc_port" toml:"grpc_port"`
	HTTPPort    int    `json:"http_port" yaml:"http_port" toml:"http_port"`
	LogPath     string `json:"log_path" yaml:"log_path" toml:"log_path"`
}

type Feature struct {
	Type DataType `json:"type" toml:"type"`
	Sep  string   `json:"sep" toml:"sep"`
}
type MagicDBConfig struct {
	Name       string             `json:"name" toml:"name"`
	Partitions []string           `json:"partitions" toml:"partitions"`
	Version    int64              `json:"versions" toml:"versions"`
	Features   map[string]Feature `json:"features" toml:"features"`
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
	RockDBPath                string                                   `json:"rocksdb" toml:"rocksdb"`
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
