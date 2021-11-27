package sqlite

import (
	"github.com/uopensail/ulib/sample"
	"github.com/uopensail/ulib/utils"
	"magicdb/config"
	"strings"
)

func getSampleFeature(v interface{}, fea *config.Feature) *sample.Feature {
	var foo func(v interface{}, sep string) *sample.Feature
	switch fea.Type {
	case config.Float32ListType:
		foo = getFloat32ListFromStr
		break
	case config.Int64ListType:
		foo = getInt64ListFromStr
		break
	case config.Float32Type:
		foo = getFloat32FromStr
		break
	case config.Int64Type:
		foo = getInt64FromStr
		break
	case config.StringListType:
		foo = getStrListFromStr
		break
	case config.StringType:
		foo = getStrFromStr
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

func getFloat32FromStr(v interface{}, sep string) *sample.Feature {
	if (len(v.(string))) == 0 {
		return nil
	}
	return getSampleFeatureFromFloat32(utils.String2Float32(v.(string)))
}

func getInt64FromStr(v interface{}, sep string) *sample.Feature {
	if (len(v.(string))) == 0 {
		return nil
	}
	return getSampleFeatureFromInt64(utils.String2Int64(v.(string)))
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
