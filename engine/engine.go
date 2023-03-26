package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"magicdb/config"
	"magicdb/engine/model"
	"magicdb/engine/table"
	"os"
	"path/filepath"
	"strings"
	"time"

	etcd "github.com/go-kratos/kratos/contrib/registry/etcd/v2"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/uopensail/ulib/utils"
	"github.com/uopensail/ulib/zlog"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	etcdclient "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	MachineEmptyError error = errors.New("machine database empty")
)

type engineMeta struct {
	model.Machine
	model.DataBase
	tables map[string]model.Table
}

type Engine struct {
	*DataBase
	etcdCli *etcdclient.Client
	*utils.MetuxJobUtil
}

func NewEngine(etcdCli *etcdclient.Client, instance registry.ServiceInstance) *Engine {
	eng := Engine{
		DataBase: NewDataBase(),
		etcdCli:  etcdCli,
	}
	eng.sync(etcdCli, instance)

	return &eng
}

func (eng *Engine) getEtcdValue(key string, decode func(kv *mvccpb.KeyValue) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(10))
	defer cancel()
	resp, err := eng.etcdCli.Get(ctx, key)
	if err != nil {
		return err
	}
	if len(resp.Kvs) <= 0 {
		return errors.New("kvs empty error")
	}
	if decode != nil {
		return decode(resp.Kvs[0])
	}

	return nil
}

func (eng *Engine) getEtcdPrefixValue(key string, decode func(kv *mvccpb.KeyValue) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(10))
	defer cancel()
	resp, err := eng.etcdCli.Get(ctx, key, etcdclient.WithPrefix())
	if err != nil {
		return err
	}
	if len(resp.Kvs) <= 0 {
		return errors.New("kvs empty error")
	}
	for i := 0; i < len(resp.Kvs); i++ {
		if decode != nil {
			err := decode(resp.Kvs[i])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (eng *Engine) getAllMeta(url string) (*engineMeta, error) {
	//
	machineKey := model.GetMachineKey(url)
	machineMeta := model.Machine{}
	err := eng.getEtcdValue(machineKey, func(kv *mvccpb.KeyValue) error {
		return json.Unmarshal(kv.Value, &machineMeta)
	})
	if err == rpctypes.ErrEmptyKey {
		return nil, MachineEmptyError
	} else if err != nil {
		return nil, err
	}
	databaseMeta := model.DataBase{}
	tableMetas := make(map[string]model.Table, 0)
	databseKey := model.GetDataBaseKey(machineMeta.Namespace, machineMeta.DataBase)
	err = eng.getEtcdPrefixValue(databseKey, func(kv *mvccpb.KeyValue) error {
		if bytes.Equal([]byte(databseKey), kv.Key) {
			return json.Unmarshal(kv.Value, &databaseMeta)
		} else {
			tableMeta := model.Table{}
			err := json.Unmarshal(kv.Value, &tableMeta)
			if err == nil {
				key := kv.Key
				tableMetas[string(key)] = tableMeta

			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return &engineMeta{
		machineMeta,
		databaseMeta,
		tableMetas,
	}, nil
}

func registerServices(etcdCli *etcdclient.Client, instance registry.ServiceInstance,
	timeout int) (registry.Registrar, context.CancelFunc, error) {
	if etcdCli != nil {
		ctx1, cancel := context.WithCancel(context.Background())
		reg := etcd.New(etcdCli, etcd.Context(ctx1))
		if timeout <= 0 {
			timeout = 10
		}
		ctx2, _ := context.WithTimeout(ctx1, time.Duration(timeout)*time.Second)
		err := reg.Register(ctx2, &instance)
		return reg, cancel, err
	}
	return nil, nil, nil
}

func deregisterServices(cancel context.CancelFunc, reg registry.Registrar, instance registry.ServiceInstance,
	timeout int) error {

	if cancel != nil {
		cancel()
	}
	if reg != nil {
		if timeout <= 0 {
			timeout = 10
		}
		ctx2, _ := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
		return reg.Deregister(ctx2, &instance)
	}
	return nil
}

func (eng *Engine) sync(etcdCli *etcdclient.Client, instance registry.ServiceInstance) {
	workDir := config.AppConfigInstance.WorkDir
	cacheSize := config.AppConfigInstance.CacheSize
	job, meta := eng.genJob(workDir, cacheSize, config.AppConfigInstance.HTTPPort)
	if job != nil {
		job()
	}

	//注册服务
	var reg registry.Registrar
	var regCancel context.CancelFunc
	if etcdCli != nil && meta != nil {
		var err error
		instance.Name = model.GetDataBaseKey(meta.Namespace, meta.Machine.DataBase)
		reg, regCancel, err = registerServices(etcdCli, instance, 10)
		if err != nil {
			panic(err)
		}
	}

	go func() (string, *table.Table, error) {

		ticker := time.NewTicker(time.Minute * 5)
		defer ticker.Stop()
		lockKey := instance.Name
		if meta != nil {
			lockKey = model.GetDataBaseKey(meta.Namespace, meta.Machine.DataBase)
		}

		eng.MetuxJobUtil = utils.NewMetuxJobUtil(lockKey, nil, etcdCli, 10, -1)
		for {
			<-ticker.C
			job, meta := eng.genJob(workDir, cacheSize, config.AppConfigInstance.HTTPPort)
			if job == nil {
				continue
			}

			registerJob := job
			//如果有etcd 就需要注册和反注册
			if etcdCli != nil {

				registerJob = func() {
					// 反注册
					deregisterServices(regCancel, reg, instance, 20)
					defer func() {
						var err error
						instance.Name = model.GetDataBaseKey(meta.Namespace, meta.Machine.DataBase)
						reg, regCancel, err = registerServices(etcdCli, instance, 20)
						if err != nil {
							zlog.LOG.Error("registerServices", zap.Error(err))
						}
					}()

					//加载job
					job()

				}
			}

			if job != nil {
				eng.MetuxJobUtil.TryRun(registerJob)
			}

		}
	}()

}

func (eng *Engine) genJob(workDir string, cacheSize int, httpPort int) (func(), *engineMeta) {

	ip, _ := utils.GetLocalIp()
	meta, err := eng.getAllMeta(fmt.Sprintf("%s:%d", ip, httpPort))
	cloneTable := eng.DataBase.CloneTable()
	if err == MachineEmptyError {
		//清理内存里的
		eng.doUpdateTable(nil, nil, cloneTable)
		return nil, nil
	} else if err != nil {
		zlog.LOG.Error("get meta error", zap.Error(err))
		return nil, nil
	}

	jobs := checkLoaderJob(workDir, cacheSize, &meta.DataBase, meta.tables, cloneTable)
	if len(jobs) == 0 {
		holdDirs := eng.doUpdateTable(meta.tables, nil, cloneTable)
		doCleanTableDir(workDir, holdDirs)
		return nil, meta
	}

	return func() {
		upsertTables := make(map[string]*table.Table)
		for i := 0; i < len(jobs); i++ {
			tableKey, table, err := jobs[i]()
			if err != nil {
				zlog.LOG.Error("loader table error", zap.Error(err), zap.String("table_key", tableKey))
				continue
			}
			upsertTables[tableKey] = table
		}

		holdDirs := eng.doUpdateTable(meta.tables, upsertTables, cloneTable)
		doCleanTableDir(workDir, holdDirs)
	}, meta
}

func (eng *Engine) doUpdateTable(lastestTablesInfo map[string]model.Table,
	upsertTable map[string]*table.Table, cloneTable Tables) []string {

	freeList := make([]*table.Table, 0, len(cloneTable.M))

	holdTableKey := make([]string, 0, len(cloneTable.M))
	for k, newTable := range upsertTable {
		if oldTable, ok := cloneTable.M[k]; ok {
			// update
			freeList = append(freeList, oldTable)
		}
		cloneTable.M[k] = newTable

		holdTableKey = append(holdTableKey, filepath.Join(k, newTable.Meta.Version))
	}

	for k, v := range cloneTable.M {
		if _, ok := lastestTablesInfo[k]; !ok {
			// remove
			delete(cloneTable.M, k)
			freeList = append(freeList, v)
		}
		holdTableKey = append(holdTableKey, filepath.Join(k, v.Meta.Version))
	}
	eng.DataBase.StoreTable(&cloneTable)

	for i := 0; i < len(freeList); i++ {
		oldTable := freeList[i]
		oldTable.LazyFree(1)
	}
	return holdTableKey
}

type loadJob func() (string, *table.Table, error)
type loadJobs []loadJob

func checkLoaderJob(workDir string, cacheSize int, dbInfo *model.DataBase, tablesInfo map[string]model.Table,
	clone Tables) loadJobs {
	tableCacheSize := cacheSize / len(tablesInfo)
	jobs := make(loadJobs, 0)
	for k, v := range tablesInfo {
		remoteMeta := v
		localMeta := table.GetLocalMeta(workDir, k, remoteMeta.Current)
		tableKey := k

		if localMeta == nil || localMeta.Version != remoteMeta.Current {
			//Download load Job

			job := func() (string, *table.Table, error) {
				tableMetaPath, err := table.Download(workDir, dbInfo, tableKey, &remoteMeta)
				if err != nil {
					return tableKey, nil, err
				}
				table := table.NewTable(tableMetaPath, tableCacheSize)
				if table == nil {
					return tableKey, nil, fmt.Errorf("NewTable error path: %s", tableMetaPath)
				}
				return tableKey, table, nil
			}
			jobs = append(jobs, job)
		} else {
			if mv, ok := clone.M[k]; !ok || mv.Meta.Version != remoteMeta.Current {
				// open job
				job := func() (string, *table.Table, error) {
					localMetaFilePath := table.FormatLocalMetaFilePath(workDir, tableKey, remoteMeta.Current)
					table := table.NewTable(localMetaFilePath, tableCacheSize)
					if table == nil {
						return tableKey, nil, fmt.Errorf("NewTable error path: %s", localMetaFilePath)
					}
					return tableKey, table, nil
				}
				jobs = append(jobs, job)
			}
		}
	}
	return jobs
}
func doCleanTableDir(rootDir string, holdDirList []string) {
	holdDirs := make(map[string]bool, len(holdDirList))
	for _, v := range holdDirList {
		path := filepath.Join(rootDir, v)
		holdDirs[path] = true
	}
	filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() == false {
			if strings.HasSuffix(path, "meta.mark.json") {
				baseDir := filepath.Dir(path)
				if _, ok := holdDirs[baseDir]; !ok {
					zlog.LOG.Info("os.RemoveAll", zap.String("path", baseDir))
					os.RemoveAll(baseDir)
				}
			}

		}
		return nil
	})
}
