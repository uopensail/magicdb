package updater

import (
	"fmt"
	"magicdb/config"
	"magicdb/monitor"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/uopensail/ulib/commonconfig"
	"github.com/uopensail/ulib/finder"
	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/zlog"
	"go.uber.org/zap"
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
func Register(name string, cfg commonconfig.DownloaderConfig,
	factory CreateFunc, release ReleaseFunc,
	createParams, releaseParams interface{}) bool {
	interval := 30
	if cfg.Interval > 0 {
		interval = cfg.Interval
	}
	myFinder := finder.GetFinder(&cfg.FinderConfig)
	magicConfig := checkLocalDataCompleteness(cfg.LocalPath)
	//注册的时候不会去下载数据线创建任务
	var table ITable
	table = nil
	if magicConfig != nil {
		table = factory(magicConfig, createParams)
	}
	job := &Job{
		Key:            name,
		Interval:       interval,
		Finder:         myFinder,
		DownloadConfig: cfg,
		Table:          table,
		Dir:            filepath.Join(cfg.LocalPath, fmt.Sprintf("%d", magicConfig.Version)),
	}

	ManagerImp.Locker.Lock()
	ManagerImp.JobMap[name] = job
	ManagerImp.Locker.Unlock()

	//开启更新的协程
	go update(name, interval, factory, release, createParams, releaseParams)
	return true
}

//更新数据
func update(name string, interval int, factory CreateFunc, release ReleaseFunc,
	createParams, releaseParams interface{}) {
	ticker := time.NewTicker(time.Second * time.Duration(interval))
	defer ticker.Stop()
	for {
		<-ticker.C
		oldJob := getJob(name)
		if oldJob == nil {
			continue
		}
		localMagicCfg := tryDownloadIfNeed(name, oldJob.Finder, oldJob.DownloadConfig)
		if localMagicCfg == nil {
			continue
		}

		newTable := factory(localMagicCfg, createParams)

		if newTable == nil {
			zlog.LOG.Error("create table nil")
			continue
		}
		newJob := &Job{
			Key:            name,
			DownloadConfig: oldJob.DownloadConfig,
			Interval:       oldJob.Interval,
			Finder:         oldJob.Finder,
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
	//先检查本地的local的meta信息
	status := localMagicMeta.Init(filepath.Join(localPath, "local"))
	if !status {
		zlog.LOG.Error("checkLocalDataCompleteness.MagicDBConfig.Init",
			zap.String("localPath", filepath.Join(localPath, "local")))
		return nil
	}

	info := ManagerImp.Center.Get(localMagicMeta.Name)
	if info == nil {
		zlog.LOG.Error("checkLocalDataCompleteness.Center.Get")
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
func tryDownload(myfinder finder.IFinder, cfg commonconfig.DownloaderConfig) *config.MagicDBConfig {
	stat := prome.NewStat("updater.tryDownload")
	defer stat.End()
	tmpPath := filepath.Join(cfg.LocalPath, "remote")
	size, err := myfinder.Download(cfg.SourcePath, tmpPath)
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
		size, err = myfinder.Download(remoteMagicConfig.Partitions[i], localMagicMeta.Partitions[i])
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
	//如果线上的状态不是服务状态，就不更新下载
	status := monitor.GetStatus()
	if status != monitor.ServiceServingStatus {
		zlog.LOG.Info("tryDownloadIfNeed.GetStatus", zap.Int32("serverStatus", status))
		return nil
	}
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
		ok := monitor.TrySuspend()
		if !ok {
			zlog.LOG.Error("tryDownloadIfNeed.TrySuspend Failed")
			return nil
		}
		newInfo := Info{
			Key:        key,
			Etag:       remoteEtag,
			UpdateTime: remoteUpdateTime,
		}

		ret := tryDownload(finder, cfg)

		if ret != nil {
			//更新新的info
			ManagerImp.Center.Upset(key, &newInfo)
		}
		//更新到需要预热缓存的状态
		monitor.SetStatus(monitor.ServiceSuspendStatus, monitor.ServiceNeedWarmUpCacheStatus)
		return ret
	}
	return nil
}

var ManagerImp *Manager
