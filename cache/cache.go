package cache

import (
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/linxGnu/grocksdb"
	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/sample"
	"github.com/uopensail/ulib/zlog"
	"go.uber.org/zap"
	"time"
)

const batch = 2000

type Cache struct {
	db   *grocksdb.DB
	path string
}

//metaInfo 记录特征的meta信息
type metaInfo struct {
	versions   map[string]int64 `json:"versions"`    //记录每一个client的version
	updateTime int64            `json:"update_time"` //上次更新的时间戳
}

func (m *metaInfo) getClientVersion(key string) int64 {
	if version, ok := m.versions[key]; ok {
		return version
	}
	return 0
}

//Features 内部的特征结构
type Features struct {
	values *sample.Features
	meta   *metaInfo
}

func (f *Features) GetClientVersion(key string) int64 {
	return f.meta.getClientVersion(key)
}

func (f *Features) GetValues() *sample.Features {
	return f.values
}

func (f *Features) GetUpdateTime() int64 {
	return f.meta.updateTime
}

func (f *Features) UpdateFeature(key string, feature *sample.Feature) {
	f.values.Feature[key] = feature
}

func (f *Features) UpdateVersions(clientVersions map[string]int64) {
	f.meta.versions = clientVersions
}

func (f *Features) Marshal() []byte {
	stat := prome.NewStat("Features.Marshal")
	defer stat.End()
	f.meta.updateTime = time.Now().Unix()
	data, err := json.Marshal(f.meta)
	if err != nil {
		stat.MarkErr()
		zlog.LOG.Error("Features.Marshal", zap.Error(err))
		return nil
	}

	f.values.Feature["__meta__"] = sample.MakeStringList([]string{string(data)})

	data, err = proto.Marshal(f.values)
	if err != nil {
		stat.MarkErr()
		zlog.LOG.Error("Features.Marshal", zap.Error(err))
		data = nil
	}
	delete(f.values.Feature, "__meta__")
	return data
}

func NewCache(path string) *Cache {
	opt := grocksdb.NewDefaultBlockBasedTableOptions()
	opt.SetBlockCache(grocksdb.NewLRUCache(100000))
	opts := grocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(opt)
	opts.SetCreateIfMissing(true)
	db, err := grocksdb.OpenDb(opts, path)
	if err != nil {
		panic(err)
	}
	return &Cache{db: db, path: path}
}

func (c *Cache) Get(key string) *Features {
	stat := prome.NewStat("Cache.Get")
	defer stat.End()
	opt := grocksdb.NewDefaultReadOptions()
	defer opt.Destroy()
	val, err := c.db.GetBytes(opt, []byte(key))
	if err != nil {
		stat.MarkErr()
		return nil
	}
	features := newFeatures(val)
	if features == nil {
		stat.MarkErr()

		return nil
	}
	return features
}

func (c *Cache) NewFeatures(features *sample.Features, clientVersions map[string]int64) *Features {
	return &Features{
		values: features,
		meta:   &metaInfo{versions: clientVersions, updateTime: time.Now().Unix()},
	}
}

func (c *Cache) Save(key string, data []byte) {
	stat := prome.NewStat("Cache.Save")
	defer stat.End()
	if data == nil {
		stat.MarkErr()
		return
	}
	opt := grocksdb.NewDefaultWriteOptions()
	defer opt.Destroy()
	err := c.db.Put(opt, []byte(key), data)

	if err != nil {
		stat.MarkErr()
		return
	}
}

func (c *Cache) Delete() {
	stat := prome.NewStat("Cache.Delete")
	defer stat.End()
	ch := make(chan []byte, 1000)

	go func(channel chan []byte) {
		ropt := grocksdb.NewDefaultReadOptions()
		defer ropt.Destroy()
		defer close(channel)
		it := c.db.NewIterator(ropt)
		defer it.Close()
		expireTime := time.Now().Unix() - 30*86400
		count := 0
		for ; it.Valid(); it.Next() {
			key := it.Key()
			val := it.Value()
			keyData := make([]byte, len(key.Data()))
			copy(keyData, key.Data())
			features := newFeatures(val.Data())
			if expireTime > features.GetUpdateTime() {
				channel <- keyData
			}
			key.Free()
			val.Free()
			count++
		}
		prome.NewStat(fmt.Sprintf("cache.count.%s", c.path)).SetCounter(count).End()
	}(ch)

	go func(channel chan []byte) {
		wb := grocksdb.NewWriteBatch()
		wopt := grocksdb.NewDefaultWriteOptions()
		defer wopt.Destroy()
		defer wb.Destroy()
		count := 0
		for key := range channel {
			count++
			wb.Delete(key)
			if wb.Count() >= batch {
				err := c.db.Write(wopt, wb)
				if err != nil {
					zlog.LOG.Error("cache.delete", zap.Error(err))
					prome.NewStat(fmt.Sprintf("cache.deleteCount.%s", c.path)).SetCounter(wb.Count()).MarkErr().End()
				}
				wb.Destroy()
				wb = grocksdb.NewWriteBatch()
			}
		}
		if wb.Count() > 0 {
			err := c.db.Write(wopt, wb)
			if err != nil {
				zlog.LOG.Error("cache.delete", zap.Error(err))
				prome.NewStat(fmt.Sprintf("cache.deleteCount.%s", c.path)).SetCounter(wb.Count()).MarkErr().End()
			}
			wb.Destroy()
		}
		prome.NewStat(fmt.Sprintf("cache.deleteCount.%s", c.path)).SetCounter(count).End()
	}(ch)
}

func newFeatures(data []byte) *Features {
	stat := prome.NewStat("newFeatures")
	defer stat.End()

	features := &sample.Features{}
	err := proto.Unmarshal(data, features)
	if err != nil {
		stat.MarkErr()
		zlog.LOG.Error("newFeatures", zap.Error(err))
		return nil
	}

	//获得meta信息
	metaData := sample.GetStringByKey(features, "__meta__")
	if len(metaData) == 0 {
		stat.MarkErr()
		zlog.LOG.Error("newFeatures get meta info error")
		return nil
	}

	meta := &metaInfo{}
	err = json.Unmarshal([]byte(metaData), meta)
	if err != nil {
		stat.MarkErr()
		zlog.LOG.Error("newFeatures get meta info error", zap.Error(err))
		return nil
	}

	ret := &Features{
		values: features,
		meta:   meta,
	}

	delete(ret.values.Feature, "__meta__")

	return ret
}
