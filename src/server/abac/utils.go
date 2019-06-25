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

func RemoveMapAttributeByPath(obj interface{}, path string) map[string]interface{} {
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
