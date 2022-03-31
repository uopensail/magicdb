package updater

import (
	"os"
	"time"

	"github.com/uopensail/ulib/commonconfig"
	"github.com/uopensail/ulib/finder"
	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/zlog"
	"go.uber.org/zap"
)

//这里定义一个任务所需要的参数
type Job struct {
	Key             string
	Finder          finder.IFinder
	Interval        int
	DownloadConfig  commonconfig.DownloaderConfig
	Table           ITable
	Dir             string
	CreateFunction  CreateFunc
	ReleaseFunction ReleaseFunc
	CreateParam     interface{}
	ReleaseParam    interface{}
}

//WhetherNeedUpdate 判断是不是需要更新
func WhetherNeedUpdate(job *Job) *Info {
	stat := prome.NewStat("updater.WhetherNeedUpdate")
	defer stat.End()
	info := ManagerImp.Center.Get(job.Key)
	remoteEtag := job.Finder.GetETag(job.DownloadConfig.SourcePath)
	if len(remoteEtag) == 0 {
		zlog.LOG.Error("WhetherNeedUpdate.GetETag", zap.String("remotePath", job.DownloadConfig.SourcePath))
		return nil
	}
	remoteUpdateTime := job.Finder.GetUpdateTime(job.DownloadConfig.SourcePath)
	if remoteUpdateTime == 0 {
		zlog.LOG.Error("WhetherNeedUpdate.GetUpdateTime", zap.String("remotePath", job.DownloadConfig.SourcePath))
		return nil
	}
	//以远程的数据为准
	if info == nil || remoteEtag != info.Etag || remoteUpdateTime > info.UpdateTime {
		return &Info{
			Key:        job.Key,
			Etag:       remoteEtag,
			UpdateTime: remoteUpdateTime,
		}
	}
	return nil
}

//更新数据
func Update(job *Job, info *Info) bool {
	if job == nil || info == nil || job.CreateFunction == nil {
		return false
	}

	localMagicCfg := tryDownload(job.Finder, job.DownloadConfig)

	if localMagicCfg == nil {
		return false
	}

	newTable := job.CreateFunction(localMagicCfg, job.CreateParam)

	if newTable == nil {
		zlog.LOG.Error("create table nil")
		return false
	}
	newJob := &Job{
		Key:             job.Key,
		DownloadConfig:  job.DownloadConfig,
		Interval:        job.Interval,
		Finder:          job.Finder,
		Table:           newTable,
		Dir:             job.Dir,
		CreateFunction:  job.CreateFunction,
		ReleaseFunction: job.ReleaseFunction,
		CreateParam:     job.CreateParam,
		ReleaseParam:    job.ReleaseParam,
	}
	ManagerImp.Locker.Lock()
	ManagerImp.JobMap[job.Key] = newJob
	ManagerImp.Locker.Unlock()
	//更新新的info
	ManagerImp.Center.Upset(job.Key, info)
	//延迟释放
	go func(j *Job) {
		if j == nil || j.ReleaseFunction == nil || j.Table == nil {
			return
		}
		time.Sleep(time.Second)
		j.ReleaseFunction(j.Table, j.ReleaseParam)
		os.RemoveAll(j.Dir)
		zlog.LOG.Info("updater.release.RemoveAll", zap.String("dir", job.Dir))
	}(job)
	return true
}
