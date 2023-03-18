package model

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/uopensail/ulib/commonconfig"
	"github.com/uopensail/ulib/zlog"
	"go.uber.org/zap"
)

type DataType int
type StoreType int

const (
	StringListType DataType = iota + 1
	Int64ListType
	Float32ListType
)

const (
	TextType StoreType = iota + 1
	IntegerType
	RealType
)

type Feature struct {
	Column    string    `json:"column" toml:"column"`
	DataType  DataType  `json:"dtype" toml:"dtype"`
	StoreType StoreType `json:"stype" toml:"stype"`
}

type Machine struct {
	Namespace string `json:"namespace" toml:"namespace"`
	DataBase  string `json:"database" toml:"database"`
}

type DataBase struct {
	Machines  []string `json:"machines" toml:"machines"`
	Name      string   `json:"name" toml:"name"`
	Bucket    string   `json:"bucket" toml:"bucket"`
	Tables    []string `json:"tables" toml:"tables"`
	Endpoint  string   `json:"endpoint" toml:"endpoint"`
	Region    string   `json:"region" toml:"region"`
	AccessKey string   `json:"access_key" toml:"access_key"`
	SecretKey string   `json:"secret_key" toml:"secret_key"`
}

func (dbInfo *DataBase) MakeFinderConfig() commonconfig.FinderConfig {
	c := commonconfig.FinderConfig{
		Timeout:   600,
		Endpoint:  dbInfo.Endpoint,
		Region:    dbInfo.Region,
		AccessKey: dbInfo.AccessKey,
		SecretKey: dbInfo.SecretKey,
	}
	if strings.HasPrefix(dbInfo.Bucket, "s3") {
		c.Type = "s3"
	} else if strings.HasPrefix(dbInfo.Bucket, "oss") {
		c.Type = "oss"
	}
	return c
}

type Table struct {
	Name       string                 `json:"name" toml:"name"`
	DataBase   string                 `json:"database" toml:"database"`
	DataDir    string                 `json:"data" toml:"data"`
	MetaDir    string                 `json:"meta" toml:"meta"`
	Versions   []string               `json:"versions" toml:"versions"`
	Current    string                 `json:"current_version" toml:"current_version"`
	Partitions int                    `json:"partitions" toml:"partitions"`
	Key        string                 `json:"key" toml:"key"`
	Properties map[string]interface{} `json:"properties" toml:"properties"`
}

type Meta struct {
	Name       string             `json:"name" toml:"name"`
	Partitions []string           `json:"partitions" toml:"partitions"`
	Version    string             `json:"version" toml:"version"`
	Features   map[string]Feature `json:"features" toml:"features"`
	Key        string             `json:"key" toml:"key"`
}

func NewMeta(filepath string) *Meta {
	fileData, err := ioutil.ReadFile(filepath)
	if err != nil {
		zlog.LOG.Error("NewMeta", zap.String("filepath", filepath), zap.Error(err))
		return nil
	}
	meta := &Meta{}
	err = json.Unmarshal(fileData, meta)
	if err != nil {
		zlog.LOG.Error("NewMeta", zap.String("data", string(fileData)), zap.Error(err))
		return nil
	}
	return meta
}

func (meta *Meta) Dump(filepath string) error {
	data, err := json.Marshal(meta)
	if err != nil {
		zlog.LOG.Error("Meta.Dump", zap.String("data", string(data)), zap.Error(err))
		return err
	}
	err = ioutil.WriteFile(filepath, data, 0644)
	if err != nil {
		zlog.LOG.Error("Meta.Dump", zap.String("filepath", filepath), zap.Error(err))
		return err
	}
	return nil
}

func GetMachineKey(url string) string {

	return fmt.Sprintf("/magicdb/storage/machines/%s", url)
}

func GetDataBaseKey(namespace, database string) string {
	return fmt.Sprintf("/magicdb/%s/storage/databases/%s", namespace, database)
}

func GetTableKey(namespace, database string, table string) string {
	return fmt.Sprintf("/magicdb/%s/storage/databases/%s/%s", namespace, database, table)
}
