package updater

import (
	"fmt"
	"github.com/uopensail/ulib/commonconfig"
	"github.com/uopensail/ulib/finder"
	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/zlog"
	"go.uber.org/zap"
	"magicdb/config"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type ITable interface{}

type CreateFunc func(*config.MagicDBConfig, interface{}) ITable

type ReleaseFunc func(ITable, interface{})

type Manager struct {
	Locker *sync.RWMutex
	JobMap map[string]*Job
	Center *CacheCenter
}

type Job struct {
	Key            string
	Finder         finder.IFinder
	Interval       int
	DownloadConfig commonconfig.DownloaderConfig
	Table          ITable
	Dir            string
}

func init() {
	ManagerImp = &Manager{
		Locker: new(sync.RWMutex),
		JobMap: make(map[string]*Job),
		Center: NewCacheCenter(),
	}
}

//Register 注册任务
func Register(cfg commonconfig.DownloaderConfig,
	factory CreateFunc, release ReleaseFunc,
	createParams, releaseParams interface{}) string {
	interval := 30
	if cfg.Interval > 0 {
		interval = cfg.Interval
	}
	myFinder := finder.GetFinder(&cfg.FinderConfig)
	magicConfig := checkLocalDataCompleteness(cfg.LocalPath)
	if magicConfig == nil {
		magicConfig = tryDownload(myFinder, cfg)
	}

	if magicConfig == nil {
		zlog.LOG.Error("Register.magicConfig is nil")
		return ""
	}

	table := factory(magicConfig, createParams)
	job := &Job{
		Key:            magicConfig.Name,
		Interval:       interval,
		Finder:         myFinder,
		DownloadConfig: cfg,
		Table:          table,
		Dir:            filepath.Join(cfg.LocalPath, fmt.Sprintf("%d", magicConfig.Version)),
	}
	ManagerImp.Locker.Lock()
	ManagerImp.JobMap[magicConfig.Name] = job
	ManagerImp.Locker.Unlock()

	go func(name string) {
		oldJob := getJob(name)
		if oldJob == nil {
			return
		}
		ticker := time.NewTicker(time.Second * time.Duration(job.Interval))
		defer ticker.Stop()
		for {
			<-ticker.C
			oldJob = getJob(name)
			localMagicCfg := tryDownloadIfNeed(name, job.Finder, job.DownloadConfig)
			if localMagicCfg == nil {
				continue
			}

			newTable := factory(localMagicCfg, createParams)

			if newTable == nil {
				zlog.LOG.Error("create table nil")
				continue
			}
			newJob := &Job{
				Key:            localMagicCfg.Name,
				Interval:       oldJob.Interval,
				Finder:         oldJob.Finder,
				DownloadConfig: cfg,
				Table:          newTable,
			}
			ManagerImp.Locker.Lock()
			ManagerImp.JobMap[name] = newJob
			ManagerImp.Locker.Unlock()

			//延迟释放
			go func(job *Job, params interface{}) {
				time.Sleep(time.Second)
				if release != nil && job != nil {
					release(job.Table, params)
				}
				os.RemoveAll(job.Dir)
				zlog.LOG.Info("updater.release.RemoveAll", zap.String("dir", job.Dir))
			}(oldJob, releaseParams)
		}
	}(magicConfig.Name)
	return magicConfig.Name
}

func GetTable(key string) ITable {
	ManagerImp.Locker.RLock()
	defer ManagerImp.Locker.RUnlock()
	if v, ok := ManagerImp.JobMap[key]; ok {
		return v.Table
	}
	return nil
}

func getJob(key string) *Job {
	ManagerImp.Locker.RLock()
	defer ManagerImp.Locker.RUnlock()
	if v, ok := ManagerImp.JobMap[key]; ok {
		return v
	}
	return nil
}

//checkLocalDataCompleteness 检查本地数据的完整性
func checkLocalDataCompleteness(localPath string) *config.MagicDBConfig {
	localMagicMeta := &config.MagicDBConfig{}
	status := localMagicMeta.Init(filepath.Join(localPath, "local"))
	if !status {
		return nil
	}
	isFileExistFunc := func(path string) bool {
		_, err := os.Lstat(path)
		return !os.IsNotExist(err)
	}

	for i := 0; i < len(localMagicMeta.Partitions); i++ {
		if !isFileExistFunc(localMagicMeta.Partitions[i]) {
			return nil
		}
	}
	return localMagicMeta
}

//tryDownload 下载文件
func tryDownload(finder finder.IFinder, cfg commonconfig.DownloaderConfig) *config.MagicDBConfig {
	stat := prome.NewStat("updater.tryDownload")
	defer stat.End()
	tmpPath := filepath.Join(cfg.LocalPath, "remote")
	size, err := finder.Download(cfg.SourcePath, tmpPath)
	if size == 0 || err != nil {
		stat.MarkErr()
		zlog.LOG.Error("tryDownload.Download",
			zap.String("remotePath", cfg.SourcePath), zap.Error(err))
		return nil
	}
	remoteMagicConfig := &config.MagicDBConfig{}
	status := remoteMagicConfig.Init(tmpPath)
	if !status {
		stat.MarkErr()
		zlog.LOG.Error("tryDownload.MagicDBConfig.Init",
			zap.String("tmpPath", tmpPath))
		return nil
	}
	localMagicMeta := &config.MagicDBConfig{
		Partitions: make([]string, len(remoteMagicConfig.Partitions)),
		Version:    remoteMagicConfig.Version,
		Name:       remoteMagicConfig.Name,
		Features:   remoteMagicConfig.Features,
	}
	localDataPath := filepath.Join(cfg.LocalPath, fmt.Sprintf("%d", localMagicMeta.Version))
	err = os.MkdirAll(localDataPath, 0644)
	if err != nil {
		zlog.LOG.Error("tryDownload.MkdirAll", zap.String("localDir", localDataPath))
		return nil
	}
	for i := 0; i < len(remoteMagicConfig.Partitions); i++ {
		localMagicMeta.Partitions[i] = filepath.Join(localDataPath, fmt.Sprintf("%d.db", i))
		size, err = finder.Download(remoteMagicConfig.Partitions[i], localMagicMeta.Partitions[i])
		if size == 0 || err != nil {
			zlog.LOG.Error("tryDownload.Download",
				zap.String("remotePath", remoteMagicConfig.Partitions[i]),
				zap.String("localPath", localMagicMeta.Partitions[i]),
				zap.Error(err))
			return nil
		}
	}
	status = localMagicMeta.Dump(filepath.Join(cfg.LocalPath, "local"))
	if !status {
		zlog.LOG.Error("tryDownload.MagicDBConfig.Dump",
			zap.String("localMataPath", filepath.Join(cfg.LocalPath, "local")))
		return nil
	}
	return localMagicMeta
}

//tryDownloadIfNeed 下载文件
func tryDownloadIfNeed(key string, finder finder.IFinder, cfg commonconfig.DownloaderConfig) *config.MagicDBConfig {
	info := ManagerImp.Center.Get(key)
	remoteEtag := finder.GetETag(cfg.SourcePath)
	if len(remoteEtag) == 0 {
		zlog.LOG.Error("tryDownloadIfNeed.GetETag", zap.String("remotePath", cfg.SourcePath))
		return nil
	}
	remoteUpdateTime := finder.GetUpdateTime(cfg.SourcePath)
	if remoteUpdateTime == 0 {
		zlog.LOG.Error("tryDownloadIfNeed.GetUpdateTime", zap.String("remotePath", cfg.SourcePath))
		return nil
	}
	//以远程的数据为准
	if info == nil || remoteEtag != info.Etag || remoteUpdateTime > info.UpdateTime {
		return tryDownload(finder, cfg)
	}
	return nil
}

var ManagerImp *Manager
