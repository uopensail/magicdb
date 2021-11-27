package updater

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/uopensail/ulib/prome"
)

const LocalCachePath = ".updater.db"

//CacheCenter 记录本地的缓存数据
type CacheCenter struct {
	Conn *sqlx.DB
}

type Info struct {
	Key        string
	Etag       string
	UpdateTime int64
}

func NewCacheCenter() *CacheCenter {
	conn, err := sqlx.Open("sqlite3", LocalCachePath)
	if err != nil {
		panic(err)
	}

	sql := `CREATE TABLE IF NOT EXISTS BASIC_INFO (
			KEY STRING  PRIMARY KEY, 
			ETAG STRING NOT NULL, 
			UPDATETIME BIGINT NOT NULL
			);`
	_, err = conn.Exec(sql)
	if err != nil {
		panic(err)
	}
	return &CacheCenter{Conn: conn}
}

func (c *CacheCenter) Get(key string) *Info {
	stat := prome.NewStat("CacheCenter.Get")
	defer stat.End()
	sql := fmt.Sprintf(`SELECT ETAG, UPDATETIME FROM BASIC_INFO WHERE KEY = '%s';`, key)
	row := c.Conn.QueryRow(sql)

	if row != nil {
		info := &Info{Key: key}
		row.Scan(&info.Etag, &info.UpdateTime)
		return info
	}
	return nil
}

func (c *CacheCenter) Close() {
	c.Conn.Close()
}

func (c *CacheCenter) Upset(key string, info *Info) {
	stat := prome.NewStat("CacheCenter.Set")
	defer stat.End()

	sql := fmt.Sprintf("INSERT OR REPLACE INTO BASIC_INFO (KEY, ETAG, UPDATETIME) VALUES ('%s', '%s', %d);", key, info.Etag, info.UpdateTime)
	_, err := c.Conn.Exec(sql)
	if err != nil {
		panic(err)
	}
}
