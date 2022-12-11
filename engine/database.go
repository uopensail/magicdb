package engine

import (
	"encoding/json"
	"fmt"
	"magicdb/model"
	"sync"
	"time"

	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/sample"
	"github.com/uopensail/ulib/zlog"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type DataBase struct {
	name         string
	client       *clientv3.Client
	status       *EngineStatus
	remoteLocker *Locker
	localLocker  *sync.RWMutex
	tables       map[string]*Table
	version      int64
	stop         bool
}

func NewDataBase(name string, client *clientv3.Client) *DataBase {
	db := DataBase{
		name:         name,
		client:       client,
		tables:       make(map[string]*Table),
		status:       NewEngineStatus(),
		version:      0,
		remoteLocker: NewLocker(client, name),
		localLocker:  new(sync.RWMutex),
		stop:         false,
	}

	go db.run()
	return &db
}

func (db *DataBase) getDataBaseInfo() (*model.DataBase, int64) {
	key := model.GetDataBaseKey(db.name)
	msg, version := getEtcdValueAndVersion(key, db.client)

	if msg != nil {
		info := &model.DataBase{}
		err := json.Unmarshal(msg, info)
		if err != nil {
			return nil, 0
		} else {
			return info, version
		}
	}
	return nil, 0
}

func (db *DataBase) getTableInfo(name string) (*model.Table, int64) {
	key := model.GetTableKey(db.name, name)
	msg, version := getEtcdValueAndVersion(key, db.client)

	if msg != nil {
		info := &model.Table{}
		err := json.Unmarshal(msg, info)
		if err != nil {
			return nil, 0
		} else {
			return info, version
		}
	}
	return nil, 0
}

func (db *DataBase) addTable(table string) {
	db.status.AddTable(table)
	db.localLocker.Lock()
	defer db.localLocker.Unlock()
	db.tables[table] = nil
}

func (db *DataBase) getName() string {
	return db.name
}

func (db *DataBase) delTable(table string) {
	db.status.DelTable(table)
	db.localLocker.Lock()
	t, ok := db.tables[table]
	if ok {
		delete(db.tables, table)
	}
	db.localLocker.Unlock()
	if t != nil {
		t.close()
	}
}

func (db *DataBase) close() {
	db.stop = true
}

func (db *DataBase) isServing() bool {
	return db.status.IsServing()
}

func (db *DataBase) run() {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for !db.stop {
		<-timer.C
		dbInfo, version := db.getDataBaseInfo()
		db.updateDataBase(dbInfo, version)

		for i := 0; i < len(dbInfo.Tables); i++ {
			go db.updateTable(dbInfo.Tables[i])
		}
	}
}

func (db *DataBase) Get(key string, tables []string) *sample.Features {
	tableResult := make(map[string]*sample.Features, len(tables))
	db.localLocker.RLocker()
	for i := 0; i < len(tables); i++ {
		if table, ok := db.tables[tables[i]]; ok {
			if table != nil {
				tableResult[tables[i]] = table.get(key)
			}
		}
	}
	db.localLocker.RUnlock()
	ret := sample.Features{}
	ret.Feature = make(map[string]*sample.Feature)
	for tableName, features := range tableResult {
		for featureName, feature := range features.Feature {
			ret.Feature[fmt.Sprintf("%s/%s", tableName, featureName)] = feature
		}
	}
	return &ret
}

func (db *DataBase) GetAll(key string) *sample.Features {
	tableResult := make(map[string]*sample.Features)
	db.localLocker.RLocker()
	for tableName, table := range db.tables {
		if table != nil {
			tableResult[tableName] = table.get(key)
		}
	}
	db.localLocker.RUnlock()
	ret := sample.Features{}
	ret.Feature = make(map[string]*sample.Feature)
	for tableName, features := range tableResult {
		for featureName, feature := range features.Feature {
			ret.Feature[fmt.Sprintf("%s/%s", tableName, featureName)] = feature
		}
	}
	return &ret
}

func (db *DataBase) updateTable(name string) {
	stat := prome.NewStat("DataBase.updateTable")
	defer stat.End()
	tableInfo, tableVersion := db.getTableInfo(name)

	if tableVersion == KEY_NOT_EXISTS {
		db.delTable(name)
		zlog.LOG.Info(fmt.Sprintf("delete table: %s", name))
		stat.MarkErr()
		return
	}

	db.localLocker.RLocker()
	table, ok := db.tables[name]
	db.localLocker.RUnlock()
	if !ok {
		zlog.LOG.Info(fmt.Sprintf("table: %s not found", name))
		stat.MarkErr()
		return
	}
	if table != nil && table.getVersion() == tableVersion {
		zlog.LOG.Info(fmt.Sprintf("table:%s version: %d not changed", name, tableVersion))
		return
	}
	dbInfo, _ := db.getDataBaseInfo()
	err := db.remoteLocker.Lock()
	if err != nil {
		stat.MarkErr()
		zlog.LOG.Info(fmt.Sprintf("get remote lock err:%s", err.Error()))
		return
	}
	defer db.remoteLocker.Unlock()
	db.status.SetTableStatus(name, TableStatus_Loading)
	table = NewTable(dbInfo, tableInfo, tableVersion)

	if table != nil {
		db.localLocker.Lock()
		db.tables[name] = table
		db.localLocker.Unlock()
	} else {
		zlog.LOG.Error(fmt.Sprintf("create sqlite table:%s error", name))
	}
	db.status.SetTableStatus(name, TableStatus_Serving)
}

func (db *DataBase) updateDataBase(dbInfo *model.DataBase, version int64) {
	stat := prome.NewStat("DataBase.updateDataBase")
	defer stat.End()
	if version == KEY_NOT_EXISTS {
		db.stop = true
		zlog.LOG.Info("DataBase.updateDataBase.stop")
		return
	}

	if db.version == version {
		return
	}
	db.version = version
	addKeys := make([]string, 0, len(dbInfo.Tables))
	delKeys := make([]string, 0, len(dbInfo.Tables))
	keyMap := make(map[string]bool, len(dbInfo.Tables))
	for i := 0; i < len(dbInfo.Tables); i++ {
		keyMap[dbInfo.Tables[i]] = true
	}

	db.localLocker.RLocker()
	for i := 0; i < len(dbInfo.Tables); i++ {
		if _, ok := db.tables[dbInfo.Tables[i]]; !ok {
			addKeys = append(addKeys, dbInfo.Tables[i])
		}
	}

	for table := range db.tables {
		if _, ok := keyMap[table]; !ok {
			delKeys = append(delKeys, table)
		}
	}
	db.localLocker.RUnlock()
	for i := 0; i < len(addKeys); i++ {
		db.addTable(addKeys[i])
	}

	for i := 0; i < len(delKeys); i++ {
		db.delTable(delKeys[i])
	}
}
