package sqlite

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spaolacci/murmur3"
	"github.com/uopensail/ulib/commonconfig"
	"github.com/uopensail/ulib/loader"
	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/sample"
	"github.com/uopensail/ulib/zlog"
	"go.uber.org/zap"
	"magicdb/config"
	"magicdb/updater"
)

type client struct {
	dbs         []*sqlx.DB
	magicConfig *config.MagicDBConfig
}

func createClient(cfg *config.MagicDBConfig) *client {
	c := &client{
		dbs:         make([]*sqlx.DB, len(cfg.Partitions)),
		magicConfig: cfg,
	}

	for i := 0; i < len(cfg.Partitions); i++ {
		db, err := sqlx.Connect("sqlite3",
			fmt.Sprintf("file:%s?mode=ro&nolock=1&_query_only=1&_mutex=no", cfg.Partitions[i]))
		if err != nil {
			panic(err)
		}
		c.dbs[i] = db
	}
	return c
}

func (c *client) close() {
	if c != nil {
		for i := 0; i < len(c.dbs); i++ {
			c.dbs[i].Close()
		}
	}
}

func (c *client) get(key string) *sample.Features {
	tableIndex := murmur3.Sum64([]byte(key)) % uint64(len(c.dbs))
	stat := prome.NewStat(fmt.Sprintf("sqilte.client.%d.get", tableIndex))
	defer stat.End()

	//sqlite把所有的特征都取出来
	db := c.dbs[tableIndex]
	dest := make(map[string]interface{})
	sql := fmt.Sprintf("select * from features where key = '%s';", key)
	err := db.QueryRowx(sql).MapScan(dest)
	if err != nil {
		stat.MarkErr()
		zlog.LOG.Error(fmt.Sprintf("sqilte.client.%d.get", tableIndex), zap.Error(err))
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

type Client struct {
	Name string
}

func NewClient(cfg commonconfig.DownloaderConfig) *Client {
	createFunc := func(dbConfig *config.MagicDBConfig, params interface{}) updater.ITable {
		return createClient(dbConfig)
	}

	releaseFunc := func(table updater.ITable, params interface{}) {
		table.(*client).close()
	}

	name := updater.Register(cfg, createFunc, releaseFunc, nil, nil)
	if len(name) == 0 {
		return nil
	}
	return &Client{Name: name}
}

func (c *Client) Close() {
	myClient := updater.GetTable(c.Name).(*client)
	if myClient != nil {
		myClient.close()
	}
}

func (c *Client) GetVersion() int64 {
	stat := prome.NewStat(fmt.Sprintf("Client.%s.GetVersion", c.Name))
	defer stat.End()
	myClient := updater.GetTable(c.Name).(*client)
	if myClient == nil {
		stat.MarkErr()
		return 0
	}
	return myClient.magicConfig.Version
}

func (c *Client) Get(key string) *sample.Features {
	stat := prome.NewStat(fmt.Sprintf("Client.%s.Get", c.Name))
	defer stat.End()
	myClient := loader.GetTable(c.Name).(*client)
	if myClient == nil {
		stat.MarkErr()
		return nil
	}
	return myClient.get(key)
}
