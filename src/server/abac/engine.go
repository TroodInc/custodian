package abac

import (
	"fmt"
	"strings"
	"github.com/fatih/structs"
	"reflect"
)

type Resolver interface{}

type TroodABACResolver struct {
	datasource map[string]interface{}
}

const andOperator = "and"
const orOperator = "or"
const inOperator = "in"
const eqOperator = "eq"
const notOperator = "not"
const ltOperator = "lt"
const gtOperator = "gt"

var operations map[string]func(interface{}, interface{}) (bool, interface{})
var aggregation map[string]func([]interface{}, interface{}) (bool, *FilterExpression)

func GetTroodABACResolver(datasource map[string]interface{}) TroodABACResolver {
	operations = map[string]func(interface{}, interface{}) (bool, interface{}){
		eqOperator:  operatorExact,
		notOperator: operatorNot,
		ltOperator:  operatorLt,
		gtOperator:  operatorGt,
	}

	aggregation = map[string]func([]interface{}, interface{}) (bool, *FilterExpression){
		inOperator:  operatorIn,
		andOperator: operatorAnd,
		orOperator:  operatorOr,
	}

	return TroodABACResolver{
		datasource,
	}
}

func (this *TroodABACResolver) EvaluateRule(rule map[string]interface{}) (bool, *FilterExpression) {
	condition := rule["rule"].(map[string]interface{})
	result, filters := this.evaluateCondition(condition)

	// todo: handle defaults and other edge cases
	if len(filters) > 0 {
		return result, &FilterExpression{Operator: andOperator, Operand: "", Value: filters}
	} else {
		return result, nil
	}
}

func (this *TroodABACResolver) evaluateCondition(condition map[string]interface{}) (bool, []*FilterExpression) {
	var filters []*FilterExpression

	totalResult := true
	var operator string

	for operand, value := range condition {
		switch value.(type) {
		case map[string]interface{}:
			for operator, value = range value.(map[string]interface{}) {
				break
			}
		case []interface{}:
			operator = operand
		default:
			operator = eqOperator
		}

		operand, value, is_filter := this.reveal(operand, value)

		fmt.Println("ABAC:  evaluating ", operand, operator, value)

		var flt *FilterExpression
		result := true
		if is_filter {
			flt = makeFilter(operand.(string), value)
		} else {
			if operator_func, ok := operations[operator]; ok {
				result, _ = operator_func(operand, value)
			}

			if operator_func, ok := aggregation[operator]; ok {
				if operator == inOperator {
					result, flt = operator_func(value.([]interface{}), operand)
				} else {
					result, flt = operator_func(value.([]interface{}), this)
				}
			}
		}
		totalResult = totalResult && result

		if flt != nil {
			filters = append(filters, flt)
		}
	}
	return totalResult, filters
}

func makeFilter(operand string, value interface{}) *FilterExpression {
	operator := eqOperator

	switch value.(type) {
	case map[string]interface{}:
		for operator, value = range value.(map[string]interface{}) {
			break
		}
	}

	return &FilterExpression{Operator: operator, Operand: operand, Value: value}
}

func (this *TroodABACResolver) reveal(operand interface{}, value interface{}) (interface{}, interface{}, bool) {

	var is_filter = false

	splited := strings.SplitN(operand.(string), ".", 2)

	if splited[0] == "obj" {
		operand = splited[1]
		is_filter = true
	} else if splited[0] == "sbj" || splited[0] == "ctx" {
		operand = GetAttributeByPath(this.datasource[splited[0]], splited[1])
	}

	if v, ok := value.(string); ok {
		splited := strings.SplitN(v, ".", 2)
		if splited[0] == "sbj" || splited[0] == "ctx" {
			value = GetAttributeByPath(this.datasource[splited[0]], splited[1])
		}
	}

	return operand, value, is_filter
}

func operatorExact(operand interface{}, value interface{}) (bool, interface{}) {
	if value == "*" {
		return true, nil
	}

	return cleanupType(operand) == cleanupType(value), nil
}

func operatorNot(operand interface{}, value interface{}) (bool, interface{}) {
	return operand != value, nil
}

func operatorIn(value []interface{}, operand interface{}) (bool, *FilterExpression) {
	for _, v := range value {
		if v == operand {
			return true, nil
		}
	}

	return false, nil
}

func operatorLt(value interface{}, operand interface{}) (bool, interface{}) {
	f_value := value.(float64)
	f_operand := operand.(float64)

	return f_operand < f_value, nil
}

func operatorGt(value interface{}, operand interface{}) (bool, interface{}) {
	f_value := value.(float64)
	f_operand := operand.(float64)

	return f_operand > f_value, nil
}

func operatorAnd(value []interface{}, resolver interface{}) (bool, *FilterExpression) {
	var filters []*FilterExpression

	for _, condition := range value {
		r := resolver.(*TroodABACResolver)
		res, flt := r.evaluateCondition(condition.(map[string]interface{}))
		filters = append(filters, flt...)

		if !res {
			return false, nil
		}
	}
	return true, &FilterExpression{Operator: andOperator, Operand: "", Value: filters}

}

func operatorOr(value []interface{}, resolver interface{}) (bool, *FilterExpression) {
	var filters []*FilterExpression
	var result = false
	for _, condition := range value {
		r := resolver.(*TroodABACResolver)
		res, flt := r.evaluateCondition(condition.(map[string]interface{}))
		filters = append(filters, flt...)

		if res {
			result = true
		}
	}

	return result, &FilterExpression{Operator: orOperator, Operand: "", Value: filters}
}

func cleanupType(value interface{}) interface{} {
	v := reflect.ValueOf(value)

	floatType := reflect.TypeOf(float64(0))
	if v.Type().ConvertibleTo(floatType) {
		return v.Convert(floatType).Float()
	}

	return value
}

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
