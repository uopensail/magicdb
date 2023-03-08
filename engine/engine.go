package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"magicdb/engine/model"
	"magicdb/engine/table"
	"os"
	"path/filepath"
	"time"

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
	model.DataBase
	tables map[string]model.Table
}

type Engine struct {
	*DataBase
	etcdCli *etcdclient.Client
	*utils.MetuxJobUtil
}

func NewEngine(workDir string, cacheSize int,
	etcdCli *etcdclient.Client, reg utils.Register) *Engine {
	eng := Engine{
		DataBase: NewDataBase(),
		etcdCli:  etcdCli,
	}
	eng.sync(workDir, cacheSize, etcdCli, reg)

	eng.MetuxJobUtil = utils.NewMetuxJobUtil("TODO:", reg, etcdCli, 10, -1)
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

func (eng *Engine) getAllMeta(ip string) (*engineMeta, error) {
	//
	machineKey := model.GetMachineKey()
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
		databaseMeta,
		tableMetas,
	}, nil
}

func (eng *Engine) sync(workDir string, cacheSize int,
	etcdCli *etcdclient.Client, reg utils.Register) {

	job := eng.genSyncJob(workDir, cacheSize)
	if job != nil {
		job()
	}

	go func() (string, *table.Table, error) {

		ticker := time.NewTicker(time.Minute * 5)
		defer ticker.Stop()
		for {
			<-ticker.C
			job := eng.genSyncJob(workDir, cacheSize)
			if job != nil {
				eng.MetuxJobUtil.TryRun(job)
			}

		}
	}()

}

func (eng *Engine) genSyncJob(workDir string, cacheSize int) func() {

	ip, _ := utils.GetLocalIp()
	meta, err := eng.getAllMeta(ip)

	if err == MachineEmptyError {
		//清理内存里的
		eng.doUpdateTable(nil, nil)
		return nil
	} else if err != nil {
		zlog.LOG.Error("get meta error", zap.Error(err))
		return nil
	}

	jobs := checkLoaderJob(workDir, cacheSize, &meta.DataBase, meta.tables)
	if len(jobs) == 0 {
		holdDirs := eng.doUpdateTable(meta.tables, nil)
		doCleanTableDir(workDir, holdDirs)
		return nil
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

		holdDirs := eng.doUpdateTable(meta.tables, upsertTables)
		doCleanTableDir(workDir, holdDirs)
	}
}

func (eng *Engine) doUpdateTable(lastestTablesInfo map[string]model.Table,
	upsertTable map[string]*table.Table) []string {
	cloneTable := eng.DataBase.CloneTable()
	freeList := make([]*table.Table, 0, len(cloneTable.M))

	holdTableKey := make([]string, 0, len(cloneTable.M))
	for k, newTable := range upsertTable {
		if oldTable, ok := cloneTable.M[k]; ok {
			// update
			freeList = append(freeList, oldTable)
		}
		cloneTable.M[k] = newTable

		holdTableKey = append(holdTableKey, k)
	}

	for k, v := range cloneTable.M {
		if _, ok := lastestTablesInfo[k]; !ok {
			// remove
			delete(cloneTable.M, k)
			freeList = append(freeList, v)
		}
		holdTableKey = append(holdTableKey, k)
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

func checkLoaderJob(workDir string, cacheSize int, dbInfo *model.DataBase, tablesInfo map[string]model.Table) loadJobs {
	tableCacheSize := cacheSize / len(tablesInfo)
	jobs := make(loadJobs, 0)
	for k, v := range tablesInfo {
		localMeta := table.GetLocalMeta(workDir, k, v.Current)
		if localMeta == nil || localMeta.Version != localMeta.Version {
			//Download load Job
			tableKey := k
			remoteMeta := v
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
		if info.IsDir() {
			if _, ok := holdDirs[path]; ok {
				zlog.LOG.Info("os.RemoveAll", zap.String("path", path))
				os.RemoveAll(path)
			}
		}
		return nil
	})
}
