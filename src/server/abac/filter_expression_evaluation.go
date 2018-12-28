package abac

import (
	"server/data/record"
	"server/transactions"
	"strings"
	"fmt"
)

func evaluateFilterExpression(filterExpression *FilterExpression, record *record.Record, dbTransaction transactions.DbTransaction, getRecordCallback func(transaction transactions.DbTransaction, objectClass, key string, depth int, omitOuters bool) (*record.Record, error)) (bool, error) {
	if filterExpression.Operator == andOperator {
		result := true
		for _, childFilterExpression := range filterExpression.Value.([]*FilterExpression) {
			childResult, err := evaluateFilterExpression(childFilterExpression, record, dbTransaction, getRecordCallback)
			if err != nil {
				return false, err
			}
			result = result && childResult
		}
		return result, nil
	} else if filterExpression.Operator == orOperator {
		result := true
		for _, childFilterExpression := range filterExpression.Value.([]*FilterExpression) {
			childResult, err := evaluateFilterExpression(childFilterExpression, record, dbTransaction, getRecordCallback)
			if err != nil {
				return false, err
			}
			result = result || childResult
		}
		return result, nil
	} else if filterExpression.Operator == inOperator {
		value := record.GetValue(filterExpression.Operand, dbTransaction, getRecordCallback)
		possibleValues := strings.Split(filterExpression.Value.(string), ",")
		for _, possibleValue := range possibleValues {
			if valueToString(possibleValue) == valueToString(value) {
				return true, nil
			}
		}
		return false, nil
	} else if filterExpression.Operator == eqOperator {
		value := record.GetValue(filterExpression.Operand, dbTransaction, getRecordCallback)
		return valueToString(filterExpression.Value) == valueToString(value), nil
	} else if filterExpression.Operator == notOperator {
		childResult, err := evaluateFilterExpression(filterExpression.Value.(*FilterExpression), record, dbTransaction, getRecordCallback)
		if err != nil {
			return false, err
		}
		return !childResult, nil
	} else if filterExpression.Operator == ltOperator {
		recordValue := record.GetValue(filterExpression.Operand, dbTransaction, getRecordCallback)
		castRecordValue, err := valueToFloat(recordValue)
		if err != nil {
			return false, NewFilterValidationError(fmt.Sprintln("Failed to cast record value: ", filterExpression.Operand, err.(*FilterValidationError).msg))
		}

		ruleValue, err := valueToFloat(filterExpression.Value)
		castRuleValue, err := valueToFloat(ruleValue)
		if err != nil {
			return false, NewFilterValidationError(fmt.Sprintln("Failed to cast rule value: ", filterExpression.Value, err.(*FilterValidationError).msg))
		}

		return castRecordValue < castRuleValue, nil
	} else if filterExpression.Operator == gtOperator {
		recordValue := record.GetValue(filterExpression.Operand, dbTransaction, getRecordCallback)
		castRecordValue, err := valueToFloat(recordValue)
		if err != nil {
			return false, NewFilterValidationError(fmt.Sprintln("Failed to cast record value: ", filterExpression.Operand, err.(*FilterValidationError).msg))
		}

		ruleValue, err := valueToFloat(filterExpression.Value)
		castRuleValue, err := valueToFloat(ruleValue)
		if err != nil {
			return false, NewFilterValidationError(fmt.Sprintln("Failed to cast rule value: ", filterExpression.Value, err.(*FilterValidationError).msg))
		}

		return castRecordValue > castRuleValue, nil
	}
	panic(fmt.Sprintln("Unknown type of filter specified: ", filterExpression.Operator))
}

func valueToString(value interface{}) string {
	switch castValue := value.(type) {
	case float64:
		return fmt.Sprintf("%f", castValue)
	case uint64:
		return fmt.Sprintf("%f", float64(castValue))
	default:
		return value.(string)
	}
}

func valueToFloat(value interface{}) (float64, error) {
	switch castValue := value.(type) {
	case float64:
		return castValue, nil
	case uint64:
		return float64(castValue), nil
	default:
		return 0, NewFilterValidationError("Attempted to cast non-numeric value to float64")
	}
}
