package meta

import (
	"fmt"
	"strings"
)

const (
	genericFieldColumnTypeSuffix = "__type"
	genericFieldColumnKeySuffix  = "__key"
	reverseInnerLinkSuffix       = "_set"
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

func ReverseInnerLinkName(metaName string) string {
	return metaName + reverseInnerLinkSuffix
}
