package abac

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("FilterExpression match", func() {
	Context("With the given filter expression", func() {
		filterExpression := FilterExpression{
			Operator: andOperator,
			Operand:  "",
			Value: []*FilterExpression{
				{
					Operator: eqOperator, Operand: "owner.role", Value: "ADMIN",
				},
				{
					Operator: eqOperator, Operand: "owner.is_staff", Value: true,
				},
				{
					Operator: inOperator, Operand: "owner.company", Value: "topline,velitto",
				},
				{
					Operator: gtOperator, Operand: "owner.access_level", Value: 50,
				},
				{
					Operator: ltOperator, Operand: "owner.max_booking_level", Value: 10,
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

		It("Should successfully validate correct data", func() {
			recordData := map[string]interface{}{
				"owner.role":              "ADMIN",
				"owner.is_staff":		   true,
				"owner.company":           "topline",
				"owner.active":            1,
				"owner.access_level":      65,
				"owner.max_booking_level": 5,
				"owner.is_superuser":      0,
			}
			ok, err := filterExpression.Match(recordData)
			Expect(err).To(BeNil())
			Expect(ok).To(BeTrue())
		})

		It("Should unsuccessfully validate data with not matching 'gt' expression", func() {
			recordData := map[string]interface{}{
				"owner.role":              "ADMIN",
				"owner.company":           "topline",
				"owner.access_level":      40,
				"owner.max_booking_level": 5,
				"owner.active":            1,
				"owner.is_superuser":      0,
			}
			ok, err := filterExpression.Match(recordData)
			Expect(err).To(BeNil())
			Expect(ok).To(BeFalse())
		})

		It("Should unsuccessfully validate data with not matching 'lt' expression", func() {
			recordData := map[string]interface{}{
				"owner.role":              "ADMIN",
				"owner.company":           "topline",
				"owner.active":            1,
				"owner.access_level":      65,
				"owner.max_booking_level": 15,
				"owner.is_superuser":      0,
			}
			ok, err := filterExpression.Match(recordData)
			Expect(err).To(BeNil())
			Expect(ok).To(BeFalse())
		})

		It("Should unsuccessfully validate data with not matching 'in' expression", func() {
			recordData := map[string]interface{}{
				"owner.role":              "ADMIN",
				"owner.company":           "bottomline",
				"owner.access_level":      65,
				"owner.max_booking_level": 5,
				"owner.active":            1,
				"owner.is_superuser":      0,
			}
			ok, err := filterExpression.Match(recordData)
			Expect(err).To(BeNil())
			Expect(ok).To(BeFalse())
		})

		It("Should unsuccessfully validate data with not matching 'eq' expression", func() {
			recordData := map[string]interface{}{
				"owner.role":              "MANAGER",
				"owner.is_staff":		   false,
				"owner.company":           "topline",
				"owner.access_level":      65,
				"owner.max_booking_level": 5,
				"owner.active":            1,
				"owner.is_superuser":      0,
			}
			ok, err := filterExpression.Match(recordData)
			Expect(err).To(BeNil())
			Expect(ok).To(BeFalse())
		})

		It("Should unsuccessfully validate data with not matching 'or' expression", func() {
			recordData := map[string]interface{}{
				"owner.role":              "ADMIN",
				"owner.company":           "topline",
				"owner.access_level":      65,
				"owner.max_booking_level": 5,
				"owner.active":            0,
				"owner.is_superuser":      0,
			}
			ok, err := filterExpression.Match(recordData)
			Expect(err).To(BeNil())
			Expect(ok).To(BeFalse())
		})

		It("Should unsuccessfully validate data with not matching 'or' expression", func() {
			recordData := map[string]interface{}{
				"owner.role":              "ADMIN",
				"owner.company":           "topline",
				"owner.access_level":      65,
				"owner.max_booking_level": 5,
				"owner.active":            0,
				"owner.is_superuser":      0,
			}
			ok, err := filterExpression.Match(recordData)
			Expect(err).To(BeNil())
			Expect(ok).To(BeFalse())
		})
	})
})
