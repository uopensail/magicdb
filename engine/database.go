package engine

import (
	"magicdb/engine/table"
	"sync/atomic"
	"unsafe"
)

const TableCurrentVersionIsNil = "nil"

type Tables struct {
	M map[string]*table.Table
}

type DataBase struct {
	TBs *Tables
}

func NewDataBase() *DataBase {
	db := DataBase{
		TBs: &Tables{M: map[string]*table.Table{}},
	}
	return &db
}

func (db *DataBase) CloneTable() Tables {
	tbs := db.TBs
	ret := make(map[string]*table.Table, len(db.TBs.M))
	for k, v := range tbs.M {
		ret[k] = v
	}
	return Tables{
		M: ret,
	}
}

func (db *DataBase) StoreTable(tables *Tables) {
	var unsafepL = (*unsafe.Pointer)(unsafe.Pointer(&db.TBs))
	// Storing value to the pointer
	atomic.StorePointer(unsafepL, unsafe.Pointer(tables))

}

func (db *DataBase) Get(key string, tables []string) []Fields {
	tbs := db.TBs

	rets := make([]Fields, 0, len(tables))
	for i := 0; i < len(tables); i++ {

		tableKey := tables[i]
		fields := Fields{
			TableName: tableKey,
		}

		rets = append(rets, fields)
		if table, ok := tbs.M[tableKey]; ok {
			fields.Column = table.Column
			if table != nil {
				fields.FieldsValue = table.Get(key)
			}
		}
	}

	return rets
}

type Fields struct {
	TableName string
	Column    []string
	table.FieldsValue
}

func (db *DataBase) GetAll(key string) []Fields {
	tbs := db.TBs
	rets := make([]Fields, 0, len(tbs.M))

	for tableKey, table := range tbs.M {
		fields := Fields{
			TableName: tableKey,
			Column:    table.Column,
		}
		if table != nil {
			fields.FieldsValue = table.Get(key)
		}
		rets = append(rets, fields)
	}

	return rets
}
