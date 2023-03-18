package engine

import (
	"fmt"
	"magicdb/engine/table"
	"sync/atomic"
	"unsafe"

	"github.com/uopensail/ulib/sample"
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

func (db *DataBase) Get(key string, tables []string) *sample.Features {
	tbs := db.TBs

	tableResult := make(map[string]*sample.Features, len(tables))

	for i := 0; i < len(tables); i++ {
		tableKey := tables[i]
		if table, ok := tbs.M[tableKey]; ok {
			if table != nil {
				tableResult[tableKey] = table.Get(key)
			}
		}
	}

	ret := sample.Features{}
	ret.Feature = make(map[string]*sample.Feature)
	for tableKey, features := range tableResult {
		for featureName, feature := range features.Feature {
			ret.Feature[fmt.Sprintf("%s/%s", tableKey, featureName)] = feature
		}
	}
	return &ret
}

func (db *DataBase) GetAll(key string) *sample.Features {
	tbs := db.TBs

	tableResult := make(map[string]*sample.Features)
	for tableKey, table := range tbs.M {
		if table != nil {
			tableResult[tableKey] = table.Get(key)
		}
	}

	ret := sample.Features{}
	ret.Feature = make(map[string]*sample.Feature)
	for tableKey, features := range tableResult {
		for featureName, feature := range features.Feature {
			ret.Feature[fmt.Sprintf("%s/%s", tableKey, featureName)] = feature
		}
	}
	return &ret
}
