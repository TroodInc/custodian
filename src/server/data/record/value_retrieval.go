package record

import (
	"server/object/description"
	"strings"
	"server/transactions"
	"server/data/types"
)

//TODO: Current interface looks a bit ugly because of "getRecordCallback" argument, it should be replaced somehow
//Also it should handle errors of getting values by wrong attributes
func (record *Record) GetValue(getterConfig interface{}, transaction transactions.DbTransaction, getRecordCallback func(transaction transactions.DbTransaction, objectClass, key string, ip []string, ep []string, depth int, omitOuters bool) (*Record, error)) interface{} {
	switch getterValue := getterConfig.(type) {
	case map[string]interface{}:
		return getGenericValue(record, getterValue, transaction, getRecordCallback)
	case string:
		return getSimpleValue(record, strings.Split(getterValue, "."), transaction, getRecordCallback)
	}
	return ""
}

//get key value traversing down if needed
func getSimpleValue(targetRecord *Record, keyParts []string, transaction transactions.DbTransaction, getRecordCallback func(transaction transactions.DbTransaction, objectClass, key string, ip []string, ep []string, depth int, omitOuters bool) (*Record, error)) interface{} {
	if len(keyParts) == 1 {
		return targetRecord.Data[keyParts[0]]
	} else {
		keyPart := keyParts[0]
		rawKeyValue := targetRecord.Data[keyPart]
		nestedObjectField := targetRecord.Meta.FindField(keyPart)

		//case of retrieving value or PK of generic field
		if nestedObjectField.Type == description.FieldTypeGeneric && len(keyParts) == 2 {
			if genericFieldValue, ok := rawKeyValue.(map[string]interface{}); ok {
				return genericFieldValue[keyParts[1]]
			}
		}

		//nested linked record case
		nestedObjectMeta := targetRecord.Meta.FindField(keyPart).LinkMeta
		if targetRecord.Data[keyPart] != nil {
			keyValue, _ := nestedObjectMeta.Key.ValueAsString(rawKeyValue)
			nestedRecord, _ := getRecordCallback(transaction, nestedObjectMeta.Name, keyValue, nil, nil, 1, false)
			return getSimpleValue(nestedRecord, keyParts[1:], transaction, getRecordCallback)
		} else {
			return nil
		}
	}
}

//get key value traversing down if needed
func getGenericValue(targetRecord *Record, getterConfig map[string]interface{}, transaction transactions.DbTransaction, getRecordCallback func(transaction transactions.DbTransaction, objectClass, key string, ip []string, ep []string, depth int, omitOuters bool) (*Record, error)) interface{} {
	genericFieldName := getterConfig["field"].(string)

	genericFieldValue := getSimpleValue(targetRecord, strings.Split(genericFieldName, "."), transaction, getRecordCallback, ).(*types.GenericInnerLink).AsMap()
	for _, objectCase := range getterConfig["cases"].([]interface{}) {
		castObjectCase := objectCase.(map[string]interface{})
		if genericFieldValue[types.GenericInnerLinkObjectKey] == castObjectCase["object"] {
			nestedObjectMeta := targetRecord.Meta.FindField(genericFieldName).LinkMetaList.GetByName(castObjectCase["object"].(string))
			nestedObjectPk, _ := nestedObjectMeta.Key.ValueAsString(genericFieldValue[nestedObjectMeta.Key.Name])
			nestedRecord, _ := getRecordCallback(transaction, genericFieldValue[types.GenericInnerLinkObjectKey].(string), nestedObjectPk, nil, nil, 1, false)
			return getSimpleValue(nestedRecord, strings.Split(castObjectCase["value"].(string), "."), transaction, getRecordCallback)
		}
	}
	return nil
}
