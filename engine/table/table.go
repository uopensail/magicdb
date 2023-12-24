package table

import (
	"encoding/binary"
	"fmt"
	"magicdb/engine/model"
	"math"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spaolacci/murmur3"
	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/utils"
	"github.com/uopensail/ulib/zlog"
	"go.uber.org/zap"
)

type Table struct {
	utils.Reference
	dbs    []*sqlx.DB
	Meta   model.Meta
	Column []string
	cache  *fastcache.Cache
}

type FieldType int8

const (
	NoneFieldType    FieldType = 0
	BytesFieldType   FieldType = 1 // string
	Int64FieldType   FieldType = 2
	Float32FieldType FieldType = 3
)

type FieldsValue []byte

func FieldValueReader(fv FieldsValue, onField func(ft FieldType, v []byte)) {

	col := binary.LittleEndian.Uint32(fv)
	dataCur := 4 + ((col + 3) >> 2)
	for i := 0; i < int(col); i += 4 {
		tv := fv[4+(i>>2)]
		for j := 0; j < 4; j++ {
			ft := FieldType((tv >> (j << 1)) & 3)
			switch ft {

			case NoneFieldType:
				onField(ft, nil)
			case BytesFieldType:
				bLen := binary.LittleEndian.Uint32(fv[dataCur:])
				onField(ft, fv[dataCur+4:dataCur+4+bLen])
				dataCur += (4 + bLen)
			case Int64FieldType:
				onField(ft, fv[dataCur:dataCur+8])
				dataCur += 8
			case Float32FieldType:
				onField(ft, fv[dataCur:dataCur+4])
				dataCur += 4

			default:
				continue
			}

		}
	}
}

type FieldValueEditor struct {
	FieldsValue
	cur int
	col int
}

func makeFieldValueEditor(col int) FieldValueEditor {
	edit := FieldValueEditor{
		FieldsValue: make([]byte, 4+((col+3)>>2)),
		col:         col,
	}
	binary.LittleEndian.PutUint32(edit.FieldsValue[0:], uint32(col))
	return edit
}
func (fv *FieldValueEditor) AppendNone() {
	fv.cur++

}

func (fv *FieldValueEditor) AppendBytes(v []byte) {
	fv.FieldsValue[4+(fv.cur>>2)] |= 1 << ((fv.cur & 3) << 1)
	ub := make([]byte, 4)
	binary.LittleEndian.PutUint32(ub, uint32(len(v)))
	fv.FieldsValue = append(fv.FieldsValue, ub...)
	fv.FieldsValue = append(fv.FieldsValue, v...)
	fv.cur++
}

func (fv *FieldValueEditor) AppendInt64(v int64) {
	fv.FieldsValue[4+(fv.cur>>2)] |= 2 << ((fv.cur & 3) << 1)
	ub := make([]byte, 8)
	binary.LittleEndian.PutUint64(ub, uint64(v))
	fv.FieldsValue = append(fv.FieldsValue, ub...)
	fv.cur++

}

func (fv *FieldValueEditor) AppendFloat32(v float32) {
	fv.FieldsValue[4+(fv.cur>>2)] |= 3 << ((fv.cur & 3) << 1)
	u := math.Float32bits(v)
	ub := make([]byte, 4)
	binary.LittleEndian.PutUint32(ub, u)
	fv.FieldsValue = append(fv.FieldsValue, ub...)
	fv.cur++
}

func NewTable(metaFile string, cacheSize int) *Table {
	stat := prome.NewStat("sqlite.table.NewTable")
	defer stat.End()
	meta := model.NewMeta(metaFile)

	tb := &Table{
		dbs:    make([]*sqlx.DB, len(meta.Partitions)),
		Meta:   *meta,
		Column: make([]string, len(meta.Features)),
	}
	for i, v := range meta.Features {
		tb.Column[i] = v.Column
	}
	if cacheSize > 0 {
		tb.cache = fastcache.New(cacheSize)
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

func (tb *Table) get(key string) (FieldsValue, error) {
	stat := prome.NewStat(fmt.Sprintf("sqlite.table.%s.get", tb.Meta.Name))
	defer stat.End()

	tableIndex := murmur3.Sum64([]byte(key)) % uint64(len(tb.dbs))
	db := tb.dbs[tableIndex]

	sql := fmt.Sprintf("select * from %s where %s = '%s';", tb.Meta.Name, tb.Meta.Key, key)
	rows := db.QueryRowx(sql)
	dest := make(map[string]interface{}, len(tb.Meta.Features))
	err := rows.MapScan(dest)
	if err != nil {
		stat.MarkErr()
		zlog.LOG.Error(fmt.Sprintf("sqlite.table.%d.get", tableIndex), zap.Error(err))
		return FieldsValue{}, err
	}

	editor := makeFieldValueEditor(len(tb.Meta.Features))
	for _, featureMeta := range tb.Meta.Features {

		if v, ok := dest[featureMeta.Column]; ok {
			switch featureMeta.DataType {
			case model.Float32ListType:

				if v, ok := (v.(float32)); ok {
					editor.AppendFloat32(v)
				} else {
					editor.AppendFloat32(0)
				}

			case model.Int64ListType:
				if v, ok := (v.(int64)); ok {
					editor.AppendInt64(v)
				} else {
					editor.AppendInt64(0)
				}
			case model.StringListType:
				if v, ok := (v.(string)); ok {
					editor.AppendBytes([]byte(v))
				} else {
					editor.AppendBytes([]byte{})
				}

			}

		} else {
			editor.AppendNone()
		}

	}

	return editor.FieldsValue, nil
}

func (tb *Table) Get(key string) FieldsValue {
	stat := prome.NewStat("sqlite.table.Get")
	defer stat.End()

	tb.Retain()
	defer tb.Release()
	if tb.cache != nil {
		value := tb.cache.Get(nil, []byte(key))
		if len(value) != 0 {
			return value
		}
	}

	ret, err := tb.get(key)
	if tb.cache != nil {

		tb.cache.Set([]byte(key), ret)
	}
	if err != nil {
		return nil
	}
	return ret
}

func (tb *Table) CacheWarmup() {

}
