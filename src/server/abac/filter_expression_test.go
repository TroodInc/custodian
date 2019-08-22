package abac

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("FilterExpression", func() {

	It("Can stringify EQ expression", func() {
		filterExpression := FilterExpression{
			Operator: eqOperator, Operand: "owner.role", Value: "ADMIN",
		}
		filterValue := filterExpression.String()
		Expect(filterValue).To(Equal("eq(owner.role,ADMIN)"))
	})

	It("Can stringify IN expression", func() {
		filterExpression := FilterExpression{
			Operator: inOperator, Operand: "owner.company", Value: "topline,velitto",
		}
		filterValue := filterExpression.String()
		Expect(filterValue).To(Equal("in(owner.company,(topline,velitto))"))
	})

	It("Can stringify AND expression", func() {
		filterExpression := FilterExpression{
			Operator: andOperator,
			Operand:  "",
			Value: []*FilterExpression{
				{
					Operator: eqOperator, Operand: "owner.role", Value: "ADMIN",
				},
				{
					Operator: inOperator, Operand: "owner.company", Value: "topline,velitto",
				},
			},
		}
		filterValue := filterExpression.String()
		Expect(filterValue).To(Equal("and(eq(owner.role,ADMIN),in(owner.company,(topline,velitto)))"))
	})

	It("Can stringify OR expression", func() {
		filterExpression := FilterExpression{
			Operator: orOperator,
			Operand:  "",
			Value: []*FilterExpression{
				{
					Operator: eqOperator, Operand: "owner.role", Value: "ADMIN",
				},
				{
					Operator: inOperator, Operand: "owner.company", Value: "topline,velitto",
				},
			},
		}
		filterValue := filterExpression.String()
		Expect(filterValue).To(Equal("or(eq(owner.role,ADMIN),in(owner.company,(topline,velitto)))"))
	})

	It("Can stringify full NOT expression", func() {
		filterExpression := FilterExpression{
			Operator: notOperator, Value: &FilterExpression{
				Operator: "eq", Operand: "owner.is_superuser", Value: 0,
			},
		}
		filterValue := filterExpression.String()
		Expect(filterValue).To(Equal("not(eq(owner.is_superuser,0))"))
	})

	It("Can stringify shorthand NOT expression", func() {
		filterExpression := FilterExpression{
			Operator: notOperator, Operand: "owner.is_superuser", Value: 0,
		}

		filterValue := filterExpression.String()
		Expect(filterValue).To(Equal("not(eq(owner.is_superuser,0)"))
	})

	It("Can stringify complex expression", func() {
		filterExpression := FilterExpression{
			Operator: andOperator,
			Operand:  "",
			Value: []*FilterExpression{
				{
					Operator: eqOperator, Operand: "owner.role", Value: "ADMIN",
				},
				{
					Operator: inOperator, Operand: "owner.company", Value: "topline,velitto",
				},
				{
					Operator: orOperator, Value: []*FilterExpression{
					{
						Operator: eqOperator, Operand: "owner.active", Value: 1,
					},

					{
						Operator: notOperator, Value: &FilterExpression{
						Operator: "eq", Operand: "owner.is_superuser", Value: 0,
					},
					},
				},
				},
			},
		}
		filterValue := filterExpression.String()
		Expect(filterValue).To(Equal("and(eq(owner.role,ADMIN),in(owner.company,(topline,velitto)),or(eq(owner.active,1),not(eq(owner.is_superuser,0))))"))
	})
})
