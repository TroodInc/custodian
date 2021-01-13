package abac

import (
	"custodian/server/data/record"
	"custodian/server/object/description"
	"fmt"
	"strings"
)

// TroodABAC engine structure
type TroodABAC struct {
	RulesTree         map[string]interface{}
	DataSource        map[string]interface{}
	DefaultResolution string
}

// RuleABAC structure
type RuleABAC struct {
	Mask   []string
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

// GetTroodABAC return TroodABAC engine
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

// FindRules find rules for resource and action
func (abac *TroodABAC) FindRules(resource string, action string) []interface{} {
	var rules []interface{}

	actionBase := strings.SplitN(action, "_", 2)

	paths := []string{
		resource + "." + action, resource + "." + actionBase[0] + "_*", resource + ".*",
		"*." + action, "*." + actionBase[0] + "_*", "*.*",
	}

	for _, path := range paths {
		if val, _ := GetAttributeByPath(abac.RulesTree, path); val != nil {
			rules = append(rules, val.([]interface{})...)
		}
	}

	return rules
}

// EvaluateRule execute ABAC rule
func (abac *TroodABAC) EvaluateRule(rule map[string]interface{}) (bool, *RuleABAC) {
	condition := rule["rule"].(map[string]interface{})
	passed, filters := abac.evaluateCondition(condition)

	var result RuleABAC

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

		operand, value, isFilter := abac.reveal(operand, value)

		result, flt := evaluateExpression(isFilter, operator, operand, value, abac)
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

	var isFilter = false

	splited := strings.SplitN(operand.(string), ".", 2)

	if splited[0] == "obj" {
		operand = splited[1]
		isFilter = true
	} else if splited[0] == "sbj" || splited[0] == "ctx" {
		operand, _ = GetAttributeByPath(abac.DataSource[splited[0]], splited[1])
	}

	if v, ok := value.(string); ok {
		splited := strings.SplitN(v, ".", 2)
		if splited[0] == "sbj" || splited[0] == "ctx" {
			value, _ = GetAttributeByPath(abac.DataSource[splited[0]], splited[1])
		}
	}

	return operand, value, isFilter
}

// Check : check resource and action
func (abac *TroodABAC) Check(resource string, action string) (bool, *RuleABAC) {
	rules := abac.FindRules(resource, action)

	if rules != nil {
		for _, rule := range rules {
			passed, rule := abac.EvaluateRule(rule.(map[string]interface{}))
			if passed {
				return rule.Result == "allow", rule
			}
		}
	}

	return abac.DefaultResolution == "allow", nil
}

// CheckRecord : check record and action
func (abac *TroodABAC) CheckRecord(obj *record.Record, action string) (bool, *RuleABAC) {
	passed, rule := abac.Check(obj.Meta.Name, action)

	if rule != nil && rule.Filter != nil {
		if ok, _ := rule.Filter.Match(obj.GetData()); !ok {
			return abac.DefaultResolution == "allow", rule
		}
	}

	return passed, rule
}

// MaskRecord : maskarad object
func (abac *TroodABAC) MaskRecord(obj *record.Record, action string) (bool, interface{}) {

	ok, rule := abac.CheckRecord(obj, action)

	if ok {
		if rule != nil {
			for field := range rule.Mask {
				SetAttributeByPath(obj.Data, rule.Mask[field], map[string]string{"access": "denied"})
			}

			for key, val := range obj.Data {
				if !strIn(rule.Mask, key) {
					field := obj.Meta.FindField(key)

					switch field.Type {
					case description.FieldTypeObject:
						// Then Apply masks for sub-object
						if item, ok := val.(*record.Record); ok {
							_, obj.Data[key] = abac.MaskRecord(item, action)
						}

					case description.FieldTypeArray:
						val := val.([]interface{})
						var subSet []*record.Record
						// Skip records with no access and apply mask on remained
						for i := range val {
							if item, ok := val[i].(*record.Record); ok {
								if ok, sub := abac.MaskRecord(item, action); ok {
									subSet = append(subSet, sub.(*record.Record))
								}
							}
						}

						obj.Data[key] = subSet

					}
				}
			}
		}

		return true, obj
	}
	return false, map[string]string{"access": "denied"}
}

func operatorExact(operand interface{}, value interface{}) (bool, interface{}) {
	if value == "*" {
		return true, nil
	}

	return cleanupType(operand) == cleanupType(value), nil
}

func evaluateExpression(isFilter bool, operator string, operand interface{}, value interface{}, abac *TroodABAC) (bool, *FilterExpression) {
	var flt *FilterExpression
	result := true
	if isFilter {
		flt = makeFilter(operator, operand.(string), value)
	} else {
		if operatorFunc, ok := operations[operator]; ok {
			result, _ = operatorFunc(operand, value)
		}

		if operatorFunc, ok := aggregation[operator]; ok {
			if operator == inOperator {
				result, flt = operatorFunc(value.([]interface{}), operand)
			} else {
				result, flt = operatorFunc(value.([]interface{}), abac)
			}
		}
	}
	return result, flt
}

func operatorNot(operand interface{}, value interface{}) (bool, interface{}) {
	if fmt.Sprintf("%T", value) == "string" {
		return operand != value, nil
	} else {
		var operator string
		for operator, value = range value.(map[string]interface{}) {
			break
		}
		result, flt := evaluateExpression(false, operator, operand, value, &TroodABAC{})
		return !result, flt
	}
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
	fValue := value.(float64)
	fOperand := operand.(float64)

	return fOperand < fValue, nil
}

func operatorGt(value interface{}, operand interface{}) (bool, interface{}) {
	fValue := value.(float64)
	fOperand := operand.(float64)

	return fOperand > fValue, nil
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
