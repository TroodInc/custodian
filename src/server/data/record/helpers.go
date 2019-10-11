package record

import "server/transactions"

func PopulateRecordValues(attributes []string, record *Record, dbTransaction transactions.DbTransaction, getRecordCallback func(objectClass, key string, ip []string, ep []string, depth int, omitOuters bool) (*Record, error)) map[string]interface{} {
	values := map[string]interface{}{}
	for _, attribute := range attributes {
		values[attribute] = record.GetValue(attribute, dbTransaction, getRecordCallback)
	}
	return values
}
