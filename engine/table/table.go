package table

import (
	"fmt"
	"magicdb/engine/model"

	"github.com/bluele/gcache"
	"github.com/golang/protobuf/proto"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spaolacci/murmur3"
	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/sample"
	"github.com/uopensail/ulib/utils"
	"github.com/uopensail/ulib/zlog"
	"go.uber.org/zap"
)

type Table struct {
	utils.Reference
	dbs   []*sqlx.DB
	Meta  model.Meta
	cache gcache.Cache
}

func NewTable(metaFile string, cacheSize int) *Table {
	stat := prome.NewStat("sqlite.table.NewTable")
	defer stat.End()
	meta := model.NewMeta(metaFile)

	tb := &Table{
		dbs:  make([]*sqlx.DB, len(meta.Partitions)),
		Meta: *meta,
	}

	if cacheSize > 0 {
		tb.cache = gcache.New(cacheSize).LRU().Build()
	}

	for i := 0; i < len(meta.Partitions); i++ {
		db, err := sqlx.Connect("sqlite3",
			fmt.Sprintf("file:%s?mode=ro&nolock=1&_query_only=1&_mutex=no", meta.Partitions[i]))
		if err != nil {
			zlog.LOG.Error("open sqlite3", zap.Error(err))
			stat.MarkErr()
			return nil
		}
		tb.dbs[i] = db
	}
	tb.CloseHandler = tb.close
	return tb
}

func (tb *Table) close() {
	if tb != nil {
		for i := 0; i < len(tb.dbs); i++ {
			tb.dbs[i].Close()
		}
	}
}

func (tb *Table) get(key string) *sample.Features {
	stat := prome.NewStat(fmt.Sprintf("sqlite.table.%s.get", tb.Meta.Name))
	defer stat.End()

	tableIndex := murmur3.Sum64([]byte(key)) % uint64(len(tb.dbs))
	db := tb.dbs[tableIndex]
	dest := make(map[string]interface{})
	sql := fmt.Sprintf("select * from %s where %s = '%s';", tb.Meta.Name, tb.Meta.Key, key)
	err := db.QueryRowx(sql).MapScan(dest)
	if err != nil {
		stat.MarkErr()
		zlog.LOG.Error(fmt.Sprintf("sqlite.table.%d.get", tableIndex), zap.Error(err))
		return nil
	}

	ret := &sample.Features{}
	ret.Feature = make(map[string]*sample.Feature, len(dest))
	for col, value := range dest {
		if info, ok := tb.Meta.Features[col]; ok {
			feature := getSampleFeature(value, &info)
			if feature != nil {
				ret.Feature[col] = feature
			}
		}
	}
	return ret
}

func (tb *Table) Get(key string) *sample.Features {
	stat := prome.NewStat("sqlite.table.Get")
	defer stat.End()

	tb.Retain()
	defer tb.Release()
	if tb.cache != nil {
		value, err := tb.cache.Get(key)
		if err == nil && value != nil {
			ret := &sample.Features{}
			proto.Unmarshal(value.([]byte), ret)
			return ret
		}
	}

	ret := tb.get(key)
	if tb.cache != nil {
		value, _ := proto.Marshal(ret)
		tb.cache.Set(key, value)
	}
	return ret
}

func (tb *Table) CacheWarmup() {

}
