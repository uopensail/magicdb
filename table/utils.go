package table

import (
	"magicdb/model"
)

func getSampleFeature(v interface{}, fea *model.Feature) *sample.Feature {
	var foo func(v interface{}, sep string) *sample.Feature
	needSplitBySep := false
	if len(fea.Sep) == 0 {
		needSplitBySep = true
	}

	switch fea.DataType {
	case model.Float32ListType:
		if needSplitBySep {
			foo = getFloat32ListFromStr
		}else{
			foo = getFloat32FromFloat32
		}
		break
	case model.Int64ListType:
		if needSplitBySep {
			foo = getInt64ListFromStr
		}else{
			foo = getInt64ListFromInt64
		}
		break
	case model.StringListType:
		if needSplitBySep {
			foo = getStrListFromStr
		}else{
			foo = getStrFromStr
		}
		break
	default:
		foo = nil
	}
	if foo != nil {
		return foo(v, fea.Sep)
	}
	return nil
}

func getStrFromStr(v interface{}, sep string) *sample.Feature {
	if (len(v.(string))) == 0 {
		return nil
	}
	return getSampleFeatureFromString(v.(string))
}

func getInt64FromInt64(v interface{}, sep string) *sample.Feature {
	return getSampleFeatureFromInt64(v.(int64))
}

func getFloat32FromFloat32(v interface{}, sep string) *sample.Feature {
	return getSampleFeatureFromFloat32(v.(float32))
}

func getStrListFromStr(v interface{}, sep string) *sample.Feature {
	if (len(v.(string))) == 0 {
		return nil
	}
	return getSampleFeatureFromStringList(strings.Split(v.(string), sep))
}

func getFloat32ListFromStr(v interface{}, sep string) *sample.Feature {
	if (len(v.(string))) == 0 {
		return nil
	}
	items := strings.Split(v.(string), sep)
	values := make([]float32, len(items))
	for i := 0; i < len(items); i++ {
		values[i] = utils.String2Float32(items[i])
	}
	return getSampleFeatureFromFloat32List(values)
}

func getInt64ListFromStr(v interface{}, sep string) *sample.Feature {
	if (len(v.(string))) == 0 {
		return nil
	}
	items := strings.Split(v.(string), sep)
	values := make([]int64, len(items))
	for i := 0; i < len(items); i++ {
		values[i] = utils.String2Int64(items[i])
	}
	return getSampleFeatureFromInt64List(values)
}

func getSampleFeatureFromFloat32(v float32) *sample.Feature {
	return &sample.Feature{
		Kind: &sample.Feature_FloatList{
			FloatList: &sample.FloatList{Value: []float32{v}},
		},
	}
}

func getSampleFeatureFromFloat32List(v []float32) *sample.Feature {
	return &sample.Feature{
		Kind: &sample.Feature_FloatList{
			FloatList: &sample.FloatList{Value: v},
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

func getSampleFeatureFromInt64List(v []int64) *sample.Feature {
	return &sample.Feature{
		Kind: &sample.Feature_Int64List{
			Int64List: &sample.Int64List{Value: v},
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

func getSampleFeatureFromStringList(v []string) *sample.Feature {
	values := make([][]byte, len(v))
	for i := 0; i < len(v); i++ {
		values[i] = []byte(v[i])
	}
	return &sample.Feature{
		Kind: &sample.Feature_BytesList{
			BytesList: &sample.BytesList{Value: values},
		},
	}
}
