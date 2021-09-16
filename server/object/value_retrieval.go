package object

import (
	"custodian/server/object/description"
	"strings"
)

//TODO: Current interface looks a bit ugly because of "getRecordCallback" argument, it should be replaced somehow
//Also it should handle errors of getting values by wrong attributes
func (record *Record) GetValue(getterConfig interface{}) interface{} {
	switch getterValue := getterConfig.(type) {
	case map[string]interface{}:
		return getGenericValue(record, getterValue)
	case string:
		return getSimpleValue(record, strings.Split(getterValue, "."))
	}
	return ""
}

//get key value traversing down if needed
func getSimpleValue(targetRecord *Record, keyParts []string) interface{} {
	if targetRecord != nil {
		if len(keyParts) == 1 {
			return targetRecord.Data[keyParts[0]]
		} else {
			keyPart := keyParts[0]
			nestedObjectField := targetRecord.Meta.FindField(keyPart)

			rawKeyValue := targetRecord.GetData()[keyPart]
			//case of retrieving value or PK of generic field
			if nestedObjectField.Type == description.FieldTypeGeneric && len(keyParts) == 2 {
				if genericFieldValue, ok := rawKeyValue.(map[string]interface{}); ok {
					return genericFieldValue[keyParts[1]]
				}
			}

			//nested linked record case
			nestedObjectMeta := targetRecord.Meta.FindField(keyPart).LinkMeta
			if targetRecord.Data[keyPart] != nil {
				var keyValue string
				switch rawKeyValue.(type) {
				case DLink:
					keyValue, _ = nestedObjectMeta.Key.ValueAsString(rawKeyValue.(DLink).Id)
				default:
					keyValue, _ = nestedObjectMeta.Key.ValueAsString(rawKeyValue)
				}

				nestedRecord, _ := targetRecord.processor.Get(nestedObjectMeta.Name, keyValue, nil, nil, 1, false)
				return getSimpleValue(nestedRecord, keyParts[1:])
			} else {
				return nil
			}
		}
	}
	return nil
}

//get key value traversing down if needed
func getGenericValue(targetRecord *Record, getterConfig map[string]interface{}) interface{} {
	genericFieldName := getterConfig["field"].(string)
	genericFieldValue := getSimpleValue(targetRecord, strings.Split(genericFieldName, "."))
	switch v := genericFieldValue.(type) {
	case *GenericInnerLink:
		genericFieldValue = v.AsMap()
	case map[string]interface{}:
		genericFieldValue = v
	}

	if genericFieldValue != nil {
		val := genericFieldValue.(map[string]interface{})
		for _, objectCase := range getterConfig["cases"].([]interface{}) {
			castObjectCase := objectCase.(map[string]interface{})
			if val[GenericInnerLinkObjectKey] == castObjectCase["object"] {
				nestedObjectMeta := targetRecord.Meta.FindField(genericFieldName).LinkMetaList.GetByName(castObjectCase["object"].(string))
				nestedObjectPk, _ := nestedObjectMeta.Key.ValueAsString(val[nestedObjectMeta.Key.Name])
				nestedRecord, _ := targetRecord.processor.Get(val[GenericInnerLinkObjectKey].(string), nestedObjectPk, nil, nil, 1, false)
				return getSimpleValue(nestedRecord, strings.Split(castObjectCase["value"].(string), "."))
			}
		}
	}
	return nil
}
