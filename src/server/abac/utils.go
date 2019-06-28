package abac

import (
	"github.com/fatih/structs"
	"strings"
)

func GetAttributeByPath(obj interface{}, path string) interface{} {
	attributes := strings.Split(path, ".")
	for _, key := range attributes {
		switch obj.(type) {
		case map[string]interface{}:
			obj = obj.(map[string]interface{})[key]
		case struct{}, interface{}:
			structs.DefaultTagName = "json"
			obj = structs.Map(obj)[key]
		default:
			return obj
		}
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
