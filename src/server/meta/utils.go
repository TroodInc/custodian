package meta

import (
	"fmt"
	"strings"
)

func MetaListDiff(aList, bList []*Meta) []*Meta {
	diff := make([]*Meta, 0)
	for _, aMeta := range aList {
		metaNotFound := true
		for _, bMeta := range bList {
			if bMeta.Name == aMeta.Name {
				metaNotFound = false
			}
		}
		if metaNotFound {
			diff = append(diff, aMeta)
		}
	}
	return diff
}

const (
	genericFieldColumnTypeSuffix = "__type"
	genericFieldColumnKeySuffix  = "__key"
)

func GetGenericFieldTypeColumnName(fieldName string) string {
	return fmt.Sprintf("%s%s", fieldName, genericFieldColumnTypeSuffix)
}

func GetGenericFieldKeyColumnName(fieldName string) string {
	return fmt.Sprintf("%s%s", fieldName, genericFieldColumnKeySuffix)
}

func IsGenericFieldTypeColumn(columnName string) bool {
	return strings.HasSuffix(columnName, genericFieldColumnTypeSuffix)
}

func IsGenericFieldKeyColumn(columnName string) bool {
	return strings.HasSuffix(columnName, genericFieldColumnKeySuffix)
}

func IsGenericFieldColumn(columnName string) bool {
	return IsGenericFieldTypeColumn(columnName) || IsGenericFieldKeyColumn(columnName)
}

func ReverseGenericFieldName(columnName string) string {
	if IsGenericFieldTypeColumn(columnName) {
		return strings.TrimSuffix(columnName, genericFieldColumnTypeSuffix)
	}
	if IsGenericFieldKeyColumn(columnName) {
		return strings.TrimSuffix(columnName, genericFieldColumnKeySuffix)
	}
	return ""
}
