package abac

import (
	"fmt"
	"strings"
	"github.com/fatih/structs"
	"reflect"
)

type Resolver interface {

}

type TroodABACResolver struct {
	datasource map[string]interface{}
}

var operations map[string]func(interface{}, interface{})(bool, interface{})
var aggregation map[string]func([]interface{}, interface{})(bool, interface{})

func GetTroodABACResolver(datasource map[string]interface{}) TroodABACResolver {
	operations = map[string]func(interface{}, interface{})(bool, interface{}) {
		"eq": operatorExact,
		"not": operatorNot,
		"lt": operatorLt,
		"gt": operatorGt,
	}

	aggregation = map[string]func([]interface{}, interface{})(bool, interface{}) {
		"in": operatorIn,
		"and": operatorAnd,
		"or": operatorOr,
	}

	return TroodABACResolver{
		datasource,
	}
}


func (this *TroodABACResolver) EvaluateRule(rule map[string]interface{}) (bool, []string) {
	condition := rule["rule"].(map[string]interface{})
	result, filters := this.evaluateCondition(condition)

	// todo: handle defaults and other edge cases

	return result, filters
}

func (this *TroodABACResolver) evaluateCondition(condition map[string]interface{}) (bool, []string ) {
	var filters []string

	result := true
	operator := "eq"

	for operand, value := range condition {
		switch value.(type) {
		case map[string]interface{}:
			for operator, value = range value.(map[string]interface{}) { break }
		case []interface{}:
			operator = operand
		}

		operand, value, is_filter := this.reveal(operand, value)

		fmt.Println("ABAC:  evaluating ", operand, operator, value)

		var flt interface{} = nil
		if is_filter {
			flt = makeFilter(operand.(string), value)
		} else {
			if operator_func, ok := operations[operator]; ok {
				result, _ = operator_func(value, operand)
			}

			if operator_func, ok := aggregation[operator]; ok {
				if operator == "in" {
					result, flt = operator_func(value.([]interface{}), operand)
				} else {
					result, flt = operator_func(value.([]interface{}), this)
				}
			}
		}

		if flt != nil {
			filters = append(filters, flt.(string))
		}
	}
	return result, filters
}

func makeFilter(operand string, value interface{}) (string){
	operator := "eq"

	switch value.(type) {
	case map[string]interface{}:
		for operator, value = range value.(map[string]interface{}) { break }
	}

	return fmt.Sprint(operator, "(", operand, ",", value, ")")
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
	return cleanupType(operand) == cleanupType(value), nil
}

func operatorNot(operand interface{}, value interface{}) (bool, interface{}) {
	return operand != value, nil
}

func operatorIn(value []interface{}, operand interface{}) (bool, interface{}) {
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

func operatorGt(value interface{}, operand interface{}) (bool, interface{}){
	f_value := value.(float64)
	f_operand := operand.(float64)

	return f_operand > f_value, nil
}

func operatorAnd(value []interface{}, resolver interface{}) (bool, interface{}) {
	var filters []string

	for _, condition := range value {
		r := resolver.(*TroodABACResolver)
		res, flt := r.evaluateCondition(condition.(map[string]interface{}))
		filters = append(filters, flt...)

		if !res {
			return false, nil
		}
	}
	return true, filters
}

func operatorOr(value []interface{}, resolver interface{}) (bool, interface{}) {
	var filters []string
	var result = false
	for _, condition := range value {
		r := resolver.(*TroodABACResolver)
		res, flt := r.evaluateCondition(condition.(map[string]interface{}))
		filters = append(filters, flt...)

		if res {
			result = true
		}
	}

	return result, "or(" + strings.Join(filters, ",") + ")"
}

func cleanupType(value interface{}) interface{}{
	v := reflect.ValueOf(value)

	floatType := reflect.TypeOf(float64(0))
	if v.Type().ConvertibleTo(floatType) {
		return v.Convert(floatType).Float()
	}

	return value
}

func GetAttributeByPath(obj interface{}, path string) interface{} {
	attributes := strings.Split(path,".")
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