package abac

import (
	"server/data/record"
	"server/object/description"
	"strings"
)

type TroodABAC struct {
	RulesTree         map[string]interface{}
	DataSource        map[string]interface{}
	DefaultResolution string
}

type ABACRule struct {
	Mask []string
	Filter *FilterExpression
	Result string
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

func GetTroodABAC(datasource map[string]interface{}, rules map[string]interface{}, defaultResolution string) TroodABAC {
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

	return TroodABAC{
		rules,
		datasource,
		defaultResolution,
	}
}

func (abac *TroodABAC) FindRules(resource string, action string) []interface{}  {
	var rules []interface{}

	action_base := strings.SplitN(action, "_", 2)

	paths := []string{
		resource + "." + action, resource + "." + action_base[0] + "_*", resource + ".*",
	}

	for _, path := range paths {
		if val, _ := GetAttributeByPath(abac.RulesTree, path); val != nil {
			rules = append(rules, val.([]interface{})...)
		}
	}

	return rules
}

func (abac *TroodABAC) EvaluateRule(rule map[string]interface{}) (bool, *ABACRule) {
	condition := rule["rule"].(map[string]interface{})
	passed, filters := abac.evaluateCondition(condition)

	var result ABACRule

	if rule["mask"] != nil {
		for _, val := range rule["mask"].([]interface{}) {
			result.Mask = append(result.Mask, val.(string))
		}
	}

	if filters != nil {
		result.Filter = &FilterExpression{Operator: andOperator, Operand: "", Value: filters}
	}

	result.Result = rule["result"].(string)

	return passed, &result
}

func (abac *TroodABAC) evaluateCondition(condition map[string]interface{}) (bool, []*FilterExpression) {
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

		operand, value, is_filter := abac.reveal(operand, value)

		var flt *FilterExpression
		result := true
		if is_filter {
			flt = makeFilter(operator, operand.(string), value)
		} else {
			if operator_func, ok := operations[operator]; ok {
				result, _ = operator_func(operand, value)
			}

			if operator_func, ok := aggregation[operator]; ok {
				if operator == inOperator {
					result, flt = operator_func(value.([]interface{}), operand)
				} else {
					result, flt = operator_func(value.([]interface{}), abac)
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

func makeFilter(operator string, operand string, value interface{}) *FilterExpression {
	switch value.(type) {
	case map[string]interface{}:
		for operator, value = range value.(map[string]interface{}) {
			break
		}
	}

	return &FilterExpression{Operator: operator, Operand: operand, Value: value}
}

func (abac *TroodABAC) reveal(operand interface{}, value interface{}) (interface{}, interface{}, bool) {

	var is_filter = false

	splited := strings.SplitN(operand.(string), ".", 2)

	if splited[0] == "obj" {
		operand = splited[1]
		is_filter = true
	} else if splited[0] == "sbj" || splited[0] == "ctx" {
		operand, _ = GetAttributeByPath(abac.DataSource[splited[0]], splited[1])
	}

	if v, ok := value.(string); ok {
		splited := strings.SplitN(v, ".", 2)
		if splited[0] == "sbj" || splited[0] == "ctx" {
			value, _ = GetAttributeByPath(abac.DataSource[splited[0]], splited[1])
		}
	}

	return operand, value, is_filter
}

func (abac *TroodABAC) Check(resource string, action string) (bool, *ABACRule) {
	rules := abac.FindRules(resource, action)

	if rules != nil {
		for _, rule := range rules {
			passed, rule := abac.EvaluateRule(rule.(map[string]interface{}))
			if  passed {
				return passed, rule
			}
		}
	}

	return false, nil
}

func (abac *TroodABAC) CheckRecord(obj *record.Record, action string) (bool, *ABACRule)  {
	passed, rule := abac.Check(obj.Meta.Name, action)

	if rule != nil && rule.Filter != nil {
		if ok, _ := rule.Filter.Match(obj.GetData()); !ok {
			return abac.DefaultResolution == "allow", rule
		}
	}

	return passed, rule
}

func (abac *TroodABAC) MaskRecord(obj *record.Record, action string) (bool, interface{}) {

	ok, rule := abac.CheckRecord(obj, action)

	if ok && rule != nil && rule.Result == "allow" {
		for field := range rule.Mask {
			SetAttributeByPath(obj.Data, rule.Mask[field], map[string]string{"access": "denied"})
		}

		for key, val := range obj.Data {
			if !str_in(rule.Mask, key) {
				field := obj.Meta.FindField(key)

				switch field.Type {
				case description.FieldTypeObject:
					// Then Apply masks for sub-object
					if item, ok := val.(*record.Record); ok {
						_, obj.Data[key] = abac.MaskRecord(item, action);
					}

				case description.FieldTypeArray:
					val := val.([]interface{})
					var sub_set []*record.Record
					// Skip records with no access and apply mask on remained
					for i := range val {
						if item, ok := val[i].(*record.Record); ok {
							if ok, sub := abac.MaskRecord(item, action); ok {
								sub_set = append(sub_set, sub.(*record.Record))
							}
						}
					}

					obj.Data[key] = sub_set

				}
			}
		}

		return true, obj
	} else {
		return false, map[string]string{"access": "denied"}
	}

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
		if cleanupType(v) == cleanupType(operand) {
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
		r := resolver.(*TroodABAC)
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
		r := resolver.(*TroodABAC)
		res, flt := r.evaluateCondition(condition.(map[string]interface{}))
		filters = append(filters, flt...)

		if res {
			result = true
		}
	}

	return result, &FilterExpression{Operator: orOperator, Operand: "", Value: filters}
}