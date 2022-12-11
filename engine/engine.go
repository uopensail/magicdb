package engine

import (
	"context"
	"encoding/json"
	"magicdb/model"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/uopensail/ulib/sample"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const KEY_NOT_EXISTS int64 = -1

type Engine struct {
	client   *clientv3.Client
	database unsafe.Pointer
	version  int64
	stop     bool
}

var EngineInstance *Engine

func Init(client *clientv3.Client) {
	EngineInstance = NewEngine(client)
}

func NewEngine(client *clientv3.Client) *Engine {
	engine := &Engine{
		client:   client,
		database: nil,
		version:  0,
		stop:     false,
	}
	go engine.run()
	return engine
}

func (engine *Engine) getDataBase() *DataBase {
	db := atomic.LoadPointer(&engine.database)
	if db != nil {
		return (*DataBase)(db)
	}
	return nil
}

func (engine *Engine) getEngineInfo() (*model.Machine, int64) {
	key := model.GetMachineKey()
	msg, version := getEtcdValueAndVersion(key, engine.client)
	if msg != nil {
		info := &model.Machine{}
		err := json.Unmarshal(msg, info)
		if err != nil {
			return nil, 0
		} else {
			return info, version
		}
	}
	return nil, 0
}

func (engine *Engine) IsServing() bool {
	db := atomic.LoadPointer(&engine.database)
	if db != nil {
		return (*DataBase)(db).isServing()
	}
	return false
}

func (engine *Engine) GetAll(key string) *sample.Features {
	db := engine.getDataBase()
	if db != nil {
		return db.GetAll(key)
	}
	return &sample.Features{}
}

func (engine *Engine) Get(key string, tables []string) *sample.Features {
	db := engine.getDataBase()
	if db != nil {
		return db.Get(key, tables)
	}
	return &sample.Features{}
}

func (engine *Engine) run() {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for !engine.stop {
		<-timer.C
		engine.update()
	}
}

func (engine *Engine) Stop() {
	engine.stop = true
}

func (engine *Engine) update() {
	info, version := engine.getEngineInfo()
	db := engine.getDataBase()
	if version == KEY_NOT_EXISTS {
		if db != nil {
			db.close()
			return
		}
		atomic.StorePointer(&engine.database, nil)
	}
	if engine.version == version {
		return
	}

	engine.version = version
	if db != nil && db.getName() == info.DataBase {
		return
	}

	newDB := unsafe.Pointer(NewDataBase(info.DataBase, engine.client))
	atomic.StorePointer(&engine.database, newDB)
	if db != nil {
		db.close()
		return
	}
}

func getEtcdValueAndVersion(key string, client *clientv3.Client) ([]byte, int64) {
	resp, err := client.Get(context.TODO(), key)
	if err == nil {
		return resp.Kvs[0].Value, resp.Kvs[0].Version
	} else if err == rpctypes.ErrEmptyKey {
		return nil, KEY_NOT_EXISTS
	} else {
		return nil, 0
	}
}
