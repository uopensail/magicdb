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

func init() {
	ManagerImp = &Manager{
		Locker: new(sync.RWMutex),
		JobMap: make(map[string]*Job),
		Center: NewCacheCenter(),
	}
	go run()
}

//Register 注册任务
func Register(name string, cfg commonconfig.DownloaderConfig,
	factory CreateFunc, release ReleaseFunc,
	createParam, releaseParam interface{}) bool {
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
		table = factory(magicConfig, createParam)
	}
	job := &Job{
		Key:             name,
		Interval:        interval,
		Finder:          myFinder,
		DownloadConfig:  cfg,
		Table:           table,
		Dir:             filepath.Join(cfg.LocalPath, fmt.Sprintf("%d", magicConfig.Version)),
		CreateFunction:  factory,
		ReleaseFunction: release,
		CreateParam:     createParam,
		ReleaseParam:    releaseParam,
	}

	ManagerImp.Locker.Lock()
	ManagerImp.JobMap[name] = job
	ManagerImp.Locker.Unlock()

	//开启更新的协程
	return true
}

func run() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		<-ticker.C
		check()
	}
}

//检查状态，需要更新就更新
func check() {
	jobNames := make([]string, 0, 10)
	ManagerImp.Locker.RLock()
	for name := range ManagerImp.JobMap {
		jobNames = append(jobNames, name)
	}
	ManagerImp.Locker.RUnlock()
	needUpdateJobInfos := make([]*Info, 0, 10)
	for i := 0; i < len(jobNames); i++ {
		job := getJob(jobNames[i])
		info := WhetherNeedUpdate(job)
		if info != nil {
			needUpdateJobInfos = append(needUpdateJobInfos, info)
		}
	}

	if len(needUpdateJobInfos) == 0 {
		return
	}
	ok := monitor.TrySuspend()
	if !ok {
		zlog.LOG.Error("tryDownloadIfNeed.TrySuspend Failed")
		return
	}

	for i := 0; i < len(needUpdateJobInfos); i++ {
		job := getJob(needUpdateJobInfos[i].Key)
		Update(job, needUpdateJobInfos[i])
	}
	//更新到需要预热缓存的状态
	monitor.SetStatus(monitor.ServiceSuspendStatus, monitor.ServiceNeedWarmUpCacheStatus)
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

var ManagerImp *Manager
