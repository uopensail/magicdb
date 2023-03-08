package table

import (
	"magicdb/engine/model"

	"github.com/uopensail/ulib/sample"
)

func getSampleFeature(v interface{}, fea *model.Feature) *sample.Feature {
	var foo func(v interface{}) *sample.Feature

	switch fea.DataType {
	case model.Float32ListType:
		foo = getFloat32FromFloat32
	case model.Int64ListType:
		foo = getInt64FromInt64
	case model.StringListType:
		foo = getStrFromStr
	default:
		foo = nil
	}
	if foo != nil {
		return foo(v)
	}
	return nil
}

func getStrFromStr(v interface{}) *sample.Feature {
	if (len(v.(string))) == 0 {
		return nil
	}
	return getSampleFeatureFromString(v.(string))
}

func getInt64FromInt64(v interface{}) *sample.Feature {
	return getSampleFeatureFromInt64(v.(int64))
}

func getFloat32FromFloat32(v interface{}) *sample.Feature {
	return getSampleFeatureFromFloat32(v.(float32))
}

func getSampleFeatureFromFloat32(v float32) *sample.Feature {
	return &sample.Feature{
		Kind: &sample.Feature_FloatList{
			FloatList: &sample.FloatList{Value: []float32{v}},
		},
	}
}

func getSampleFeatureFromInt64(v int64) *sample.Feature {
	return &sample.Feature{
		Kind: &sample.Feature_Int64List{
			Int64List: &sample.Int64List{Value: []int64{v}},
		},
	}
}

func getSampleFeatureFromString(v string) *sample.Feature {
	return &sample.Feature{
		Kind: &sample.Feature_BytesList{
			BytesList: &sample.BytesList{Value: [][]byte{[]byte(v)}},
		},
	}
}
