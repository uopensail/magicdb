package sqlite

import (
	"magicdb/model"

	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/sample"
	"github.com/uopensail/ulib/zlog"
)

type Table struct {
	handler *Handler
	client  *Client
}

func NewTable(database *model.DataBase, table *model.Table) *Table {
	stat := prome.NewStat("sqlite.NewTable")
	defer stat.End()
	handler := NewHandler(database, table)
	if handler == nil {
		zlog.LOG.Error("handler is nil")
		stat.MarkErr()
		return nil
	}
	client := handler.Get()
	if client == nil {
		zlog.LOG.Error("client is nil")
		stat.MarkErr()
		return nil
	}

	return &Table{handler: handler, client: client}
}

func (table *Table) Get(key string) *sample.Features {
	if table != nil && table.client != nil {
		return table.client.Get(key)
	} else {
		return &sample.Features{}
	}
}

func (table *Table) Close() {
	if table != nil && table.client != nil {
		table.client.Close()
	}
}
