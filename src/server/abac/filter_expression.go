package abac

import (
	"strings"
	"fmt"
	"server/data/record"
	"server/transactions"
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
		return fmt.Sprint(fe.Operator, "(", fe.Value, ")")
	} else {
		return fmt.Sprint(fe.Operator, "(", fe.Operand, ",", fe.Value, ")")
	}
}

func (fe *FilterExpression) Evaluate(record *record.Record, dbTransaction transactions.DbTransaction, getRecordCallback func(transaction transactions.DbTransaction, objectClass, key string, depth int, omitOuters bool) (*record.Record, error)) (bool, error) {
	return evaluateFilterExpression(fe, record, dbTransaction, getRecordCallback)
}
