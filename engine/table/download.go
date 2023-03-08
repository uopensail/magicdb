package table

import (
	"fmt"
	"magicdb/engine/model"

	"os"
	"path/filepath"
	"time"

	"github.com/uopensail/ulib/commonconfig"
	"github.com/uopensail/ulib/finder"
	"github.com/uopensail/ulib/zlog"
	"go.uber.org/zap"
)

func Download(workDir string, dbInfo *model.DataBase, tableKey string, tableInfo *model.Table) (string, error) {
	// download meta file
	fconfig := commonconfig.FinderConfig{
		Type:      dbInfo.Cloud,
		Timeout:   600,
		Endpoint:  dbInfo.Endpoint,
		Region:    dbInfo.Region,
		AccessKey: dbInfo.AccessKey,
		SecretKey: dbInfo.SecretKey,
	}
	remoteMetaFile := fconfig.Type + "://" + filepath.Join(dbInfo.Bucket, tableInfo.MetaDir, tableInfo.Current)
	dw := finder.GetFinder(&fconfig)
	tempDir := os.TempDir()
	tmpFileName := fmt.Sprintf("%s_%d_%s", tableInfo.Name, time.Now().Unix(), tableInfo.Current)
	tmpMetaPath := filepath.Join(tempDir, tmpFileName)
	var err error
	_, err = dw.Download(remoteMetaFile, tmpMetaPath)
	if err != nil {
		zlog.LOG.Error("download meta from remote error ", zap.String("remoteMetaFile", remoteMetaFile))
		return "", err
	}

	meta := model.NewMeta(tmpMetaPath)
	if meta == nil {
		zlog.LOG.Error("create meta from fileerror ", zap.String("tmpMetaPath", tmpMetaPath))

		return "", fmt.Errorf("tmpMetaPath read error")
	}
	tableDir := FormatLoclTableDir(workDir, tableKey, tableInfo.Current)
	for i := 0; i < len(meta.Partitions); i++ {
		baseName := filepath.Base(meta.Partitions[i])
		localDBFile := filepath.Join(tableDir, baseName)

		_, err = dw.Download(meta.Partitions[i], localDBFile)
		if err != nil {
			zlog.LOG.Error("Download",
				zap.String("remotePath", meta.Partitions[i]),
				zap.String("localPath", localDBFile),
				zap.Error(err))
			return "", err
		}
		meta.Partitions[i] = localDBFile
	}
	localMetaFilePath := FormatLocalMetaFilePath(workDir, tableKey, tableInfo.Current)
	//写本地meta 代表下载成功
	err = meta.Dump(localMetaFilePath)
	if err != nil {
		return "", err
	}
	return localMetaFilePath, nil
}

func FormatLoclTableDir(workDir, tableKey, tableVersion string) string {
	return filepath.Join(workDir, tableKey, tableVersion)
}
func FormatLocalMetaFilePath(workDir, tableKey, tableVersion string) string {
	return filepath.Join(workDir, tableKey, "table.markmeta")
}

func GetLocalMeta(workDir, tableKey, tableVersion string) *model.Meta {
	localMetaFile := FormatLocalMetaFilePath(workDir, tableKey, tableVersion)
	return model.NewMeta(localMetaFile)
}
