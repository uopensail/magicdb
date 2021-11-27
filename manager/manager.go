package manager

import (
	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/sample"
	"magicdb/cache"
	"magicdb/config"
	"magicdb/sqlite"
	"sync"
)

type Manager struct {
	cache   *cache.Cache
	clients map[string]*sqlite.Client
}

func Init() {
	Implementation.clients = make(map[string]*sqlite.Client)
	Implementation.cache = cache.NewCache(config.AppConfigImp.RockDBPath)
	for key, cfg := range config.AppConfigImp.Sources {
		Implementation.clients[key] = sqlite.NewClient(cfg)
	}
}

func (m *Manager) Clean() {
	stat := prome.NewStat("Manager.Clean")
	defer stat.End()
	m.cache.Delete()
}

func (m *Manager) getAll(userID string) *sample.Features {
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

func (m *Manager) getPartial(userID string, names []string) *sample.Features {
	stat := prome.NewStat("Manager.getPartial")
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
	defer stat.End()

	//获得每一个client的版本号
	clientVersions := make(map[string]int64)
	for name, client := range Implementation.clients {
		clientVersions[name] = client.GetVersion()
	}

	cachedFeatures := Implementation.cache.Get(userID)
	if cachedFeatures == nil {
		//全部去查询
		features := m.getAll(userID)
		cachedFeatures = m.cache.NewFeatures(features, clientVersions)
		m.cache.Save(userID, cachedFeatures.Marshal())
		return features
	}

	//获得需要更新的特征列表
	needUpdateClients := make([]string, 0, 10)
	for name, version := range clientVersions {
		tmpVersion := cachedFeatures.GetClientVersion(name)
		if tmpVersion < version {
			needUpdateClients = append(needUpdateClients, name)
		}
	}

	if len(needUpdateClients) > 0 {
		//部分更新特征
		partialFeatures := m.getPartial(userID, needUpdateClients)
		if partialFeatures != nil {
			for k, v := range partialFeatures.Feature {
				cachedFeatures.UpdateFeature(k, v)
			}
		}
		//cache里面的值需要更新
		cachedFeatures.UpdateVersions(clientVersions)
		m.cache.Save(userID, cachedFeatures.Marshal())
	}

	return cachedFeatures.GetValues()
}

var Implementation Manager
