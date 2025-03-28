package table

import (
	"fmt"
	"testing"
)

func Test_Merge(t *testing.T) {
	left := `{
		"a": 1,
		"b": "2"
	}`
	right := `{
		"c": 1,
		"d": "2"
	}`

	var m JSONMergeOperator
	ret := m.Merge([]byte(left), []byte(right))
	fmt.Printf("%s\n", string(ret))
}
