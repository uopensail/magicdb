package sqlite

import (
	"fmt"
	"magicdb/config"
	"magicdb/model"
	"time"

	"github.com/bluele/gcache"
	"github.com/golang/protobuf/proto"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spaolacci/murmur3"
	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/sample"
	"github.com/uopensail/ulib/zlog"
	"go.uber.org/zap"
)

type Client struct {
	dbs   []*sqlx.DB
	meta  *model.Meta
	cache gcache.Cache
}

func NewClient(meta *model.Meta) *Client {
	stat := prome.NewStat("sqlite.Client.NewClient")
	defer stat.End()
	client := &Client{
		dbs:  make([]*sqlx.DB, len(meta.Partitions)),
		meta: meta,
	}

	if config.AppConfigImp.UseCache {
		client.cache = gcache.New(config.AppConfigImp.CacheSize).LRU().
			Expiration(time.Duration(config.AppConfigImp.CacheTTL) * time.Second).Build()
	}
	for i := 0; i < len(meta.Partitions); i++ {
		db, err := sqlx.Connect("sqlite3",
			fmt.Sprintf("file:%s?mode=ro&nolock=1&_query_only=1&_mutex=no", meta.Partitions[i]))
		if err != nil {
			zlog.LOG.Error(err.Error())
			stat.MarkErr()
			return nil
		}
		client.dbs[i] = db
	}
	return client
}

func (client *Client) Close() {
	if client != nil {
		for i := 0; i < len(client.dbs); i++ {
			client.dbs[i].Close()
		}
	}
}

func (client *Client) get(key string) *sample.Features {
	tableIndex := murmur3.Sum64([]byte(key)) % uint64(len(client.dbs))
	stat := prome.NewStat(fmt.Sprintf("sqlite.Client.%d.get", tableIndex))
	defer stat.End()

	db := client.dbs[tableIndex]
	dest := make(map[string]interface{})
	sql := fmt.Sprintf("select * from features where %s = '%s';", client.meta.Key, key)
	err := db.QueryRowx(sql).MapScan(dest)
	if err != nil {
		stat.MarkErr()
		zlog.LOG.Error(fmt.Sprintf("sqlite.Client.%d.get", tableIndex), zap.Error(err))
		return nil
	}

	ret := &sample.Features{}
	ret.Feature = make(map[string]*sample.Feature, len(dest))
	for col, value := range dest {
		if info, ok := client.meta.Features[col]; ok {
			feature := getSampleFeature(value, &info)
			if feature != nil {
				ret.Feature[col] = feature
			}
		}
	}
	return ret
}

func (client *Client) Get(key string) *sample.Features {
	stat := prome.NewStat("sqlite.Client.Get")
	defer stat.End()

	if config.AppConfigImp.UseCache {
		value, err := client.cache.Get(key)
		if err == nil && value != nil {
			ret := &sample.Features{}
			proto.Unmarshal(value.([]byte), ret)
			return ret
		}
	}

	ret := client.get(key)
	if config.AppConfigImp.UseCache {
		value, _ := proto.Marshal(ret)
		client.cache.SetWithExpire(key, value, time.Duration(config.AppConfigImp.CacheTTL)*time.Second)
	}
	return ret
}
