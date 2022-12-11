package sqlite

import (
	"fmt"
	"magicdb/model"
	"os"
	"path/filepath"
	"time"

	"github.com/uopensail/ulib/commonconfig"
	"github.com/uopensail/ulib/finder"
	"github.com/uopensail/ulib/utils"
	"github.com/uopensail/ulib/zlog"
	"go.uber.org/zap"
)

const (
	workdir = "/data/data"
	metaDir = "/data/meta"
)

type Handler struct {
	finder   finder.IFinder
	database *model.DataBase
	table    *model.Table
	dataDir  string
	metaDir  string
	metaPath string
	markPath string
}

func NewHandler(database *model.DataBase, table *model.Table) *Handler {
	fconfig := commonconfig.FinderConfig{
		Type:      database.Cloud,
		Timeout:   60,
		Endpoint:  database.Endpoint,
		Region:    database.Region,
		AccessKey: database.AccessKey,
		SecretKey: database.SecretKey,
	}
	dataDir := filepath.Join(workdir, database.Name, table.Name, table.Current)
	if err := os.MkdirAll(dataDir, os.ModePerm); err != nil {
		zlog.LOG.Error(err.Error())
		panic(err)
	}
	metaDir := filepath.Join(metaDir, database.Name, table.Name)
	if err := os.MkdirAll(metaDir, os.ModePerm); err != nil {
		zlog.LOG.Error(err.Error())
		panic(err)
	}
	return &Handler{
		finder:   finder.GetFinder(&fconfig),
		database: database,
		table:    table,
		dataDir:  dataDir,
		metaDir:  metaDir,
		metaPath: filepath.Join(metaDir, table.Current),
		markPath: filepath.Join(dataDir, "_SUCCESS"),
	}
}

func (handler *Handler) getLocal() *Client {
	status := utils.FilePathExists(handler.markPath)
	if !status {
		zlog.LOG.Info(fmt.Sprintf("success file: %s not exists", handler.markPath))
		return nil
	}

	status = utils.FilePathExists(handler.metaPath)
	if !status {
		zlog.LOG.Info(fmt.Sprintf("meta file: %s not exists", handler.metaPath))
		return nil
	}

	meta := model.NewMeta(handler.metaPath)
	if meta == nil {
		zlog.LOG.Info(fmt.Sprintf("create meta from file: %s error", handler.metaPath))
		return nil
	}

	return NewClient(meta)
}

func (handler *Handler) markSuccess() {
	fileName := filepath.Join(handler.dataDir, "_SUCCESS")
	_, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		file, err := os.Create(fileName)
		if err == nil {
			file.Close()
		}
	} else {
		currentTime := time.Now().Local()
		os.Chtimes(fileName, currentTime, currentTime)
	}
}

func (handler *Handler) getRemote() *Client {
	// download meta file
	remoteMetaFile := filepath.Join(handler.database.Cloud,
		handler.database.Bucket, handler.table.MetaDir, handler.table.Current)

	var err error
	_, err = handler.finder.Download(remoteMetaFile, handler.metaPath)
	if err != nil {
		zlog.LOG.Error(fmt.Sprintf("download meta from remote: %s error", remoteMetaFile))
		return nil
	}

	meta := model.NewMeta(handler.metaPath)
	if meta == nil {
		zlog.LOG.Info(fmt.Sprintf("create meta from file: %s error", handler.metaPath))
		return nil
	}

	partitions := make([]string, len(meta.Partitions))

	for i := 0; i < len(meta.Partitions); i++ {
		baseName := filepath.Base(meta.Partitions[i])
		partitions[i] = filepath.Join(handler.dataDir, baseName)
		_, err = handler.finder.Download(meta.Partitions[i], partitions[i])
		if err != nil {
			zlog.LOG.Error("Handler.Download",
				zap.String("remotePath", meta.Partitions[i]),
				zap.String("localPath", partitions[i]),
				zap.Error(err))
			return nil
		}
	}

	handler.markSuccess()

	//把meta文件写到本地磁盘
	meta.Partitions = partitions
	meta.Dump(handler.metaPath)

	return NewClient(meta)
}

func (handler *Handler) Get() *Client {
	client := handler.getLocal()
	if client == nil {
		client = handler.getRemote()
	}
	return client
}
