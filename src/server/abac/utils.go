package abac

import (
	"github.com/fatih/structs"
	"reflect"
	"strings"
)

func GetAttributeByPath(obj interface{}, path string) (interface{}, bool) {
	parts := strings.SplitN(path, ".", 2)

	var current interface{}
	var ok bool

	switch obj.(type) {
	case map[string]interface{}:
		current, ok = obj.(map[string]interface{})[parts[0]]
	case struct{}, interface{}:
		structs.DefaultTagName = "json"
		current, ok = structs.Map(obj)[parts[0]]
	}

	if ok && len(parts) == 1 {
		return current, true
	} else if ok && len(parts) == 2 {
		return GetAttributeByPath(current, parts[1])
	}

	return nil, false
}

func SetAttributeByPath(obj map[string]interface{}, path string, value interface{}) map[string]interface{} {
	parts := strings.SplitN(path, ".", 2)
	current, ok := obj[parts[0]]
	if ok && len(parts) == 1 {
		obj[parts[0]] = value
		return obj
	} else if ok && len(parts) == 2  {
		obj[parts[0]] = SetAttributeByPath(current.(map[string]interface{}), parts[1], value)
	}

	return obj
}


func RemoveMapAttributeByPath(obj map[string]interface{}, path string, set_null bool) map[string]interface{} {
	parts := strings.SplitN(path, ".", 2)

	current, ok := obj[parts[0]]

	if ok && len(parts) == 1 {
		if set_null {
			obj[parts[0]] = nil
		} else {
			delete(obj, parts[0])
		}
		return obj
	} else if ok && len(parts) == 2 {
		obj[parts[0]] = RemoveMapAttributeByPath(current.(map[string]interface{}), parts[1], set_null)
	}

	return obj
}

func CheckMask(obj map[string]interface{}, mask []interface{})  []string {
	var restricted []string
	if mask != nil {
		for _, path := range mask {
			path := path.(string)
			_, matched := GetAttributeByPath(obj, path)
			if matched {
				restricted = append(restricted, path)
			}
		}
	}
	return restricted

}

func cleanupType(value interface{}) interface{} {
	v := reflect.ValueOf(value)

	floatType := reflect.TypeOf(float64(0))
	if v.Type().ConvertibleTo(floatType) {
		return v.Convert(floatType).Float()
	}

	return value
}

func str_in (list []string, item string) bool {
	for i := range list {
		if list[i] == item {
			return true
		}
	}

	return false
}