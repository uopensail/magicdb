package manager

import (
	"github.com/bluele/gcache"
	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/sample"
	"magicdb/config"
	"magicdb/sqlite"
	"sync"
	"time"
)

type Manager struct {
	cache    gcache.Cache
	ttl      int
	capacity int
	clients  map[string]*sqlite.Client
}

//cacheRecord 内部的特征结构
type cacheRecord struct {
	values   *sample.Features
	versions map[string]int64
}

func Init() {
	Implementation.clients = make(map[string]*sqlite.Client)
	Implementation.ttl = config.AppConfigImp.TTL
	Implementation.capacity = config.AppConfigImp.CacheSize
	Implementation.cache = gcache.New(Implementation.capacity).LRU().Build()

	for key, cfg := range config.AppConfigImp.Sources {
		Implementation.clients[key] = sqlite.NewClient(cfg)
	}
}

//getFromAllSqlites 从所有的sqlite中获取信息
func (m *Manager) getFromAllSqlites(userID string) *sample.Features {
	stat := prome.NewStat("Manager.getAll")
	defer stat.End()
	stat.SetCounter(len(m.clients))
	var wg sync.WaitGroup
	wg.Add(len(m.clients))
	retChannel := make(chan *sample.Features, len(m.clients))

	for k := range m.clients {
		go func(ch chan *sample.Features, name string) {
			defer wg.Done()
			ret := m.clients[name].Get(userID)
			if ret != nil {
				retChannel <- ret
			}
		}(retChannel, k)
	}
	wg.Wait()
	close(retChannel)
	ret := &sample.Features{Feature: make(map[string]*sample.Feature, len(m.clients))}
	for feature := range retChannel {
		for k, v := range feature.Feature {
			ret.Feature[k] = v
		}
	}
	return ret
}

func (m *Manager) getFromPartialSqlites(userID string, names []string) *sample.Features {
	stat := prome.NewStat("Manager.getFromPartialSqlites")
	defer stat.End()
	stat.SetCounter(len(names))
	var wg sync.WaitGroup
	wg.Add(len(m.clients))
	retChannel := make(chan *sample.Features, len(names))

	for _, k := range names {
		go func(ch chan *sample.Features, name string) {
			defer wg.Done()
			ret := m.clients[name].Get(userID)
			if ret != nil {
				retChannel <- ret
			}
		}(retChannel, k)
	}
	wg.Wait()
	close(retChannel)
	ret := &sample.Features{Feature: make(map[string]*sample.Feature, len(m.clients))}
	for feature := range retChannel {
		for k, v := range feature.Feature {
			ret.Feature[k] = v
		}
	}
	return ret
}

func (m *Manager) Get(userID string) *sample.Features {
	stat := prome.NewStat("Manager.Get")
	hit := prome.NewStat("Manager.Cache.Hit")
	defer stat.End()
	defer hit.End()

	//获得每一个client的版本号
	clientVersions := make(map[string]int64)
	for name, client := range Implementation.clients {
		clientVersions[name] = client.GetVersion()
	}

	value, err := Implementation.cache.Get(userID)

	if err != nil {
		hit.MarkErr()
		features := m.getFromAllSqlites(userID)
		record := &cacheRecord{
			values:   features,
			versions: clientVersions,
		}
		m.cache.SetWithExpire(userID, record, time.Duration(m.ttl)*time.Second)
		return features
	}

	//获得需要更新的特征列表
	needUpdateClients := make([]string, 0, 10)
	for name, version := range clientVersions {
		tmpVersion, ok := value.(*cacheRecord).versions[name]
		if !ok {
			needUpdateClients = append(needUpdateClients, name)
		}
		if tmpVersion < version {
			needUpdateClients = append(needUpdateClients, name)
		}
	}

	if len(needUpdateClients) > 0 {
		//部分更新特征
		partialFeatures := m.getFromPartialSqlites(userID, needUpdateClients)
		ret := &sample.Features{Feature: make(map[string]*sample.Feature)}
		for k, v := range value.(*cacheRecord).values.Feature {
			ret.Feature[k] = v
		}

		if partialFeatures != nil {
			for k, v := range partialFeatures.Feature {
				ret.Feature[k] = v
			}
		}
		record := &cacheRecord{
			values:   ret,
			versions: clientVersions,
		}
		m.cache.SetWithExpire(userID, record, time.Duration(m.ttl)*time.Second)
		return ret
	} else {
		return value.(*cacheRecord).values
	}
}

var Implementation Manager
