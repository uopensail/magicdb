package engine

import (
	"magicdb/model"
	"magicdb/sqlite"

	"github.com/uopensail/ulib/sample"
)

type Table struct {
	table   *sqlite.Table
	version int64
}

func NewTable(database *model.DataBase, table *model.Table, version int64) *Table,error {
	if database == nil || table == nil {
		return nil
	}
	
	sqliteTable := sqlite.NewTable(database, table)
	if sqliteTable == nil {
		return nil
	}
	return &Table{
		table:   sqliteTable,
		version: version,
	}
}

func (table *Table) get(key string) *sample.Features {
	if table != nil && table.table != nil {
		return table.table.Get(key)
	}
	return &sample.Features{}
}

func (table *Table) close() {
	if table != nil && table.table != nil {
		table.table.Close()
	}
}

func (table *Table) getVersion() int64 {
	if table != nil {
		return table.version
	}
	return 0
}
