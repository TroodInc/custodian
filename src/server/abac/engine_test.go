package abac

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"encoding/json"
)

type User struct {
	Id 		int 	`json:"id"`
	Login 	string	`json:"login"`
	Status 	string	`json:"status"`
	Role 	string	`json:"role"`
}

func json_to_condition(json_condition string) map[string]interface{} {
	var condition map[string]interface{}

	json.Unmarshal([]byte(json_condition), &condition)
	return condition
}

var _ = Describe("Abac Engine", func() {
	resolver := GetTroodABACResolver(
		map[string]interface{} {
			"sbj": User{10, "admin@demo.com", "active", "admin"},
			"ctx": nil,
		},
	)

	Describe("Operators", func() {
		It("Must check if values exact equal", func() {
			Context("for string values", func() {
				result_false, _  := operatorExact("first", "second")
				Expect(result_false).To(BeFalse())

				result_true, _  := operatorExact("same", "same")
				Expect(result_true).To(BeTrue())
			})

			Context("for numeric values", func() {
				int_float_check, _  := operatorExact(2, 3.5)
				Expect(int_float_check).To(BeFalse())

				int_int_check, _  := operatorExact(2, 2)
				Expect(int_int_check).To(BeTrue())

				float_float_check, _  := operatorExact(float64(1), float64(1))
				Expect(float_float_check).To(BeTrue())
			})
		})

		It("Must check if value in array", func() {
			Context("for strings", func() {
				var list []interface{}
				list = append(list, "a", "b", "c")

				result_false, _  := operatorIn(list, "z")
				Expect(result_false).To(BeFalse())

				result_true, _  := operatorIn(list, "a")
				Expect(result_true).To(BeTrue())
			})
		})

	})

	Describe("Reveal values from attr paths", func(){

		It("must reveal from Object", func() {
			operand, value, is_filter := resolver.reveal("sbj.role", "admin")

			Expect(operand).To(BeIdenticalTo("admin"))
			Expect(value).To(BeIdenticalTo("admin"))
			Expect(is_filter).To(BeFalse())
		})

		It("must reveal from map", func(){

		})
	})

	Describe("Rules", func() {

		It("must evaluate sbj EQ condition", func() {
			condition := json_to_condition(`{"sbj.role": "admin"}`)

			result, _ := resolver.evaluateCondition(condition)

			Expect(result).To(BeTrue())
		})

		It("must evaluate sbj IN condition", func(){
			condition := json_to_condition(`{"sbj.role": {"in": ["manager", "admin"]}}`)

			result, _ := resolver.evaluateCondition(condition)

			Expect(result).To(BeTrue())
		})

		It("must evaluate sbj NOT condition", func(){
			condition := json_to_condition(`{"sbj.role": {"not": "manager"}}`)

			result, _ := resolver.evaluateCondition(condition)

			Expect(result).To(BeTrue())
		})

		It("must evaluate OR sbj condition", func(){
			condition := json_to_condition(`{"or": [
				{"sbj.role": {"not": "manager"}},
				{"sbj.id": 5}
			]}`)

			result, _ := resolver.evaluateCondition(condition)
			Expect(result).To(BeTrue())
		})

		It("must evaluate AND sbj condition", func(){
			condition := json_to_condition(`{"and": [
				{"sbj.role": {"not": "manager"}},
				{"sbj.id": 10}	
			]}`)

			result, _ := resolver.evaluateCondition(condition)
			Expect(result).To(BeTrue())
		})
	})
})