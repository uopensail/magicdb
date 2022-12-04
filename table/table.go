package table

import (
	"fmt"
	"magicdb/config"
	"magicdb/updater"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spaolacci/murmur3"
	"github.com/uopensail/ulib/commonconfig"
	"github.com/uopensail/ulib/loader"
	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/sample"
	"github.com/uopensail/ulib/zlog"
	"go.uber.org/zap"
)

type Table struct {
	dbs         []*sqlx.DB
	partitions []string
	cache   gcache.Cache
}

func NewTable(partitions[]string) *Table {
	t := &Table{
		dbs:         make([]*sqlx.DB, len(partitions))
	}

	for i := 0; i < len(partitions); i++ {
		db, err := sqlx.Connect("sqlite3",
			fmt.Sprintf("file:%s?mode=ro&nolock=1&_query_only=1&_mutex=no",partitions[i]))
		if err != nil {
			panic(err)
		}
		t.dbs[i] = db
	}
	return t
}

func (t *Table) close() {
	if t != nil {
		for i := 0; i < len(c.dbs); i++ {
			c.dbs[i].Close()
		}
	}
	if config.AppConfigImp.UseCache {
		t.cache.
	}
}

func (t *Table) get(key string) *sample.Features {
	tableIndex := murmur3.Sum64([]byte(key)) % uint64(len(c.dbs))
	stat := prome.NewStat(fmt.Sprintf("table.Table.%d.get", tableIndex))
	defer stat.End()

	//sqlite把所有的特征都取出来
	db := t.dbs[tableIndex]
	dest := make(map[string]interface{})
	sql := fmt.Sprintf("select * from features where key = '%s';", key)
	err := db.QueryRowx(sql).MapScan(dest)
	if err != nil {
		stat.MarkErr()
		zlog.LOG.Error(fmt.Sprintf("table.Table.%d.get", tableIndex), zap.Error(err))
		return nil
	}

	ret := &sample.Features{}
	ret.Feature = make(map[string]*sample.Feature)

	for col, value := range dest {
		info, ok := c.magicConfig.Features[col]
		if !ok {
			continue
		}
		feature := getSampleFeature(value, &info)
		if feature != nil {
			ret.Feature[col] = feature
		}
	}
	return ret
}

func (t *Table) Get(key string) *sample.Features {
	// first get from cache
	stat := prome.NewStat("table.Table.Get")
	defer stat.End()

	value, err := t.cache.Get(userID)
	if err != nil{
		stat
	}

	tableIndex := murmur3.Sum64([]byte(key)) % uint64(len(c.dbs))
	stat := prome.NewStat(fmt.Sprintf("table.Table.%d.get", tableIndex))
	defer stat.End()

	//sqlite把所有的特征都取出来
	db := c.dbs[tableIndex]
	dest := make(map[string]interface{})
	sql := fmt.Sprintf("select * from features where key = '%s';", key)
	err := db.QueryRowx(sql).MapScan(dest)
	if err != nil {
		stat.MarkErr()
		zlog.LOG.Error(fmt.Sprintf("table.Table.%d.get", tableIndex), zap.Error(err))
		return nil
	}

	ret := &sample.Features{}
	ret.Feature = make(map[string]*sample.Feature)

	for col, value := range dest {
		info, ok := c.magicConfig.Features[col]
		if !ok {
			continue
		}
		feature := getSampleFeature(value, &info)
		if feature != nil {
			ret.Feature[col] = feature
		}
	}
	return ret
}

