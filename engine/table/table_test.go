package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
)

func Test_FieldsValue(t *testing.T) {

	vals := []interface{}{nil, 1, 0.1, nil, "string t", 30000000.0, -2, 4, 5, "string222", -0.1, -4}
	targetV := make([]interface{}, len(vals))
	edit := makeFieldValueEditor(len(vals))
	for i := 0; i < len(vals); i++ {
		val := vals[i]
		if val == nil {
			edit.AppendNone()
			continue
		}
		switch val.(type) {
		case int:
			v := int64(val.(int))
			edit.AppendInt64(v)
			targetV[i] = v
		case float64:
			v := float32(val.(float64))
			edit.AppendFloat32(v)
			targetV[i] = v
		case string:
			v := []byte(val.(string))
			edit.AppendBytes(v)
			targetV[i] = v
		}
	}
	i := 0
	FieldValueReader(edit.FieldsValue, func(ft FieldType, v []byte) {
		switch ft {
		case Int64FieldType:
			rv := int64(binary.LittleEndian.Uint64(v))
			if rv != targetV[i] {
				t.Fatalf("Int64FieldType read:%v except: %v", v, targetV[i])
			}
			fmt.Println(rv)
		case Float32FieldType:
			u := binary.LittleEndian.Uint32(v)
			rv := math.Float32frombits(u)
			if rv != targetV[i] {
				t.Fatalf("Float32FieldType read:%v except: %v", v, targetV[i])
			}
			fmt.Println(rv)
		case BytesFieldType:
			rv := v
			if bytes.Equal(rv, targetV[i].([]byte)) == false {
				t.Fatalf("BytesFieldType read:%v except: %v", v, targetV[i])
			}
			fmt.Println(string(rv))
		case NoneFieldType:
			if len(v) != 0 {
				t.Fatalf("NoneFieldType error")
			}
			fmt.Println("nil")
		}
		i++
	})
}
