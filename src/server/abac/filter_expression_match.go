package abac

import (
	"strings"
	"fmt"
)

func matchFilterExpression(filterExpression *FilterExpression, recordValues map[string]interface{}) (bool, error) {
	if filterExpression.Operator == andOperator {
		result := true
		for _, childFilterExpression := range filterExpression.Value.([]*FilterExpression) {
			childResult, err := matchFilterExpression(childFilterExpression, recordValues)
			if err != nil {
				return false, err
			}
			result = result && childResult
		}
		return result, nil
	} else if filterExpression.Operator == orOperator {
		result := false
		for _, childFilterExpression := range filterExpression.Value.([]*FilterExpression) {
			childResult, err := matchFilterExpression(childFilterExpression, recordValues)
			if err != nil {
				return false, err
			}
			result = result || childResult
		}
		return result, nil
	} else if filterExpression.Operator == inOperator {
		possibleValues := strings.Split(filterExpression.Value.(string), ",")
		for _, possibleValue := range possibleValues {
			if valueToString(possibleValue) == valueToString(recordValues[filterExpression.Operand]) {
				return true, nil
			}
		}
		return false, nil
	} else if filterExpression.Operator == eqOperator {
		return valueToString(filterExpression.Value) == valueToString(recordValues[filterExpression.Operand]), nil
	} else if filterExpression.Operator == notOperator {
		childResult, err := matchFilterExpression(filterExpression.Value.(*FilterExpression), recordValues)
		if err != nil {
			return false, err
		}
		return !childResult, nil
	} else if filterExpression.Operator == ltOperator {
		castRecordValue, err := valueToFloat(recordValues[filterExpression.Operand])
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
		castRecordValue, err := valueToFloat(recordValues[filterExpression.Operand])
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

func getReferencedAttributes(filterExpression *FilterExpression) []string {
	attributes := make([]string, 0)
	if filterExpression.Operator == andOperator || filterExpression.Operator == orOperator {
		for _, childFilterExpression := range filterExpression.Value.([]*FilterExpression) {
			attributes = append(attributes, getReferencedAttributes(childFilterExpression)...)
		}
		return attributes
	} else if filterExpression.Operator == inOperator || filterExpression.Operator == eqOperator || filterExpression.Operator == ltOperator || filterExpression.Operator == gtOperator {
		return []string{filterExpression.Operand}
	} else if filterExpression.Operator == notOperator {
		return getReferencedAttributes(filterExpression.Value.(*FilterExpression))
	}
	panic(fmt.Sprintln("Unknown type of filter specified: ", filterExpression.Operator))
}

func valueToString(value interface{}) string {
	if value == nil {
		return ""
	}
	switch castValue := value.(type) {
	case float64:
		return fmt.Sprintf("%f", castValue)
	case int:
		return fmt.Sprintf("%f", float64(castValue))
	case string:
		return castValue
	default:
		return value.(string)
	}
}

func valueToFloat(value interface{}) (float64, error) {
	switch castValue := value.(type) {
	case float64:
		return castValue, nil
	case int:
		return float64(castValue), nil
	default:
		return 0, NewFilterValidationError("Attempted to cast non-numeric value to float64")
	}
}
