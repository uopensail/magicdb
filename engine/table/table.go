package table

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spaolacci/murmur3"
	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/zlog"
	"go.uber.org/zap"
)

// Table represents a sharded SQLite table handler.
// It maintains connections to multiple database shards and distributes queries using murmur3 hash.
type Table struct {
	Name string     // Name of the table
	Dir  string     // Data Dir of the table
	dbs  []*sqlx.DB // Slice of database connections for shards
}

// NewTable creates a new Table instance with connections to all SQLite shards in the specified directory.
// It automatically discovers .db files in the directory and creates read-only connections to them.
func NewTable(name, dir string) *Table {
	stat := prome.NewStat("sqlite.table.NewTable")
	defer stat.End()

	entries, err := os.ReadDir(dir)
	if err != nil {
		zlog.LOG.Error("Failed to read directory", zap.String("directory", dir), zap.Error(err))
		return nil
	}

	var dbPaths []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if strings.HasSuffix(path, extension) {
			dbPaths = append(dbPaths, path)
		}
	}

	tbl := &Table{
		Name: name,
		Dir:  dir,
		dbs:  make([]*sqlx.DB, len(dbPaths)),
	}

	// Open connections to all database shards
	for i, path := range dbPaths {
		db, err := sqlx.Connect("sqlite3",
			fmt.Sprintf("file:%s?mode=ro&nolock=1&_query_only=1&_mutex=no", path))
		if err != nil {
			zlog.LOG.Error("Failed to connect to SQLite",
				zap.String("path", path),
				zap.Error(err))

			// Close any previously opened connections
			for j := 0; j < i; j++ {
				if tbl.dbs[j] != nil {
					tbl.dbs[j].Close()
				}
			}
			stat.MarkErr()
			return nil
		}
		tbl.dbs[i] = db
	}

	return tbl
}

// Get retrieves a value from the table by key using consistent hashing for shard selection
func (tbl *Table) Get(key string) ([]byte, error) {
	stat := prome.NewStat(fmt.Sprintf("sqlite.table.%s.get", tbl.Name))
	defer stat.End()

	if len(tbl.dbs) == 0 {
		return nil, fmt.Errorf("no database shards available")
	}

	// Select shard using murmur3 hash
	shardIndex := murmur3.Sum64([]byte(key)) % uint64(len(tbl.dbs))
	db := tbl.dbs[shardIndex]

	var value string
	// Use table name from struct and proper SQL escaping
	query := fmt.Sprintf("SELECT value FROM `%s` WHERE key = ? LIMIT 1", tbl.Name)
	err := db.QueryRow(query, key).Scan(&value)
	if err != nil {
		zlog.LOG.Error("Query failed",
			zap.String("table", tbl.Name),
			zap.String("key", key),
			zap.Error(err))
		return nil, err
	}

	// Zero-copy conversion from string to byte slice using unsafe.
	// Safe in this context because we immediately return the result and don't retain references.
	return unsafe.Slice(unsafe.StringData(value), len(value)), nil
}
