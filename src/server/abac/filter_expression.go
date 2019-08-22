package abac

import (
	"strings"
	"fmt"
)

type FilterExpression struct {
	Operator string
	Operand  string
	Value    interface{}
}

func (fe *FilterExpression) String() string {
	if fe.Operator == andOperator || fe.Operator == orOperator {
		filterValues := make([]string, 0)
		for _, filterExpression := range fe.Value.([]*FilterExpression) {
			filterValues = append(filterValues, filterExpression.String())
		}
		return fmt.Sprint(fe.Operator, "(", strings.Join(filterValues, ","), ")")
	} else if fe.Operator == inOperator {
		return fmt.Sprint(fe.Operator, "(", fe.Operand, ",(", fe.Value, "))")
	} else if fe.Operator == notOperator {
		if fe.Operand != "" {
			return fmt.Sprint(fe.Operator, "(eq(", fe.Operand, ",", fe.Value, ")")
		} else {
			return fmt.Sprint(fe.Operator, "(", fe.Value, ")")
		}
	} else {
		return fmt.Sprint(fe.Operator, "(", fe.Operand, ",", fe.Value, ")")
	}
}

//matches the filter expression against the given record values
func (fe *FilterExpression) Match(recordValues map[string]interface{}) (bool, error) {
	return matchFilterExpression(fe, recordValues)
}

//returns all record`s attributes referenced in filter
func (fe *FilterExpression) ReferencedAttributes() []string {
	return getReferencedAttributes(fe)
}
