package helper

import (
	"math/rand"
	"reflect"
	"sort"
)

// mimic `?:` operator
// Need type assertion to convert output to expected type
func Ternary(IF bool, THEN interface{}, ELSE interface{}) interface{} {
	if IF {
		return THEN
	} else {
		return ELSE
	}
}

// Get keys of a map, i.e.
// map[string]interface{} -> []string
// Note that some type checks are omitted for efficiency, you need to ensure them yourself,
// otherwise your program should panic
func Keys(v interface{}) []string {
	rv := reflect.ValueOf(v)
	result := make([]string, 0, rv.Len())
	for _, kv := range rv.MapKeys() {
		result = append(result, kv.String())
	}
	return result
}

//Get keys of a map and sort.
//If desc_order = true, the results are in descending order.
//If desc_order =  false, the results are in ascending order
func SortKeys(v interface{}, desc_order bool) []string {
	result := Keys(v)
	if desc_order {
		sort.Sort(sort.Reverse(sort.StringSlice(result)))
		return result
	}
	sort.Strings(result)
	return result
}

// Static alphaNumeric table used for generating unique request ids
var alphaNumericTable = []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")

func GenerateRandomId() []byte {
	alpha := make([]byte, 16, 16)
	for i := 0; i < 16; i++ {
		n := rand.Intn(len(alphaNumericTable))
		alpha[i] = alphaNumericTable[n]
	}
	return alpha
}
