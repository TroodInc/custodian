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

func RemoveMapAttributeByPath(obj map[string]interface{}, path string) map[string]interface{} {
	attributes := strings.Split(path, ".")

	if len(attributes) == 1 {
		delete(obj, attributes[0])
		return obj
	} else {
		next := obj[attributes[0]].(map[string]interface{})
		obj = RemoveMapAttributeByPath(next, strings.Join(attributes[1:], "."))
	}

	return obj
}
