package abac

import (
	"custodian/server/auth"
	"custodian/server/object"
	"custodian/server/object/description"

	"custodian/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type User struct {
	ID      int                    `json:"id"`
	Login   string                 `json:"login"`
	Status  string                 `json:"status"`
	Role    string                 `json:"role"`
	Profile map[string]interface{} `json:"profile"`
}

var _ = Describe("Abac Engine", func() {
	resolver := GetTroodABAC(
		map[string]interface{}{
			"sbj": User{
				10,
				"admin@demo.com",
				"active",
				"admin",
				map[string]interface{}{
					"id":   1,
					"name": "John",
				}},
			"ctx": nil,
		},
		nil,
		"allow",
	)

	Describe("Operators", func() {
		It("Must check if values exact equal", func() {
			Context("for string values", func() {
				resultFalse, _ := operatorExact("first", "second")
				Expect(resultFalse).To(BeFalse())

				resultTrue, _ := operatorExact("same", "same")
				Expect(resultTrue).To(BeTrue())
			})

			Context("for numeric values", func() {
				intFloatCheck, _ := operatorExact(2, 3.5)
				Expect(intFloatCheck).To(BeFalse())

				intIntCheck, _ := operatorExact(2, 2)
				Expect(intIntCheck).To(BeTrue())

				floatFloatCheck, _ := operatorExact(float64(1), float64(1))
				Expect(floatFloatCheck).To(BeTrue())
			})
		})

		It("Must check if value in array", func() {
			Context("for strings", func() {
				var list []interface{}
				list = append(list, "a", "b", "c")

				resultFalse, _ := operatorIn(list, "z")
				Expect(resultFalse).To(BeFalse())

				resultTrue, _ := operatorIn(list, "a")
				Expect(resultTrue).To(BeTrue())
			})
		})

	})

	Describe("Reveal values from attr paths", func() {

		It("must reveal from Object", func() {
			operand, value, isFilter := resolver.reveal("sbj.role", "admin")

			Expect(operand).To(BeIdenticalTo("admin"))
			Expect(value).To(BeIdenticalTo("admin"))
			Expect(isFilter).To(BeFalse())
		})

		It("must reveal from map", func() {
			operand, value, isFilter := resolver.reveal("obj.owner", "sbj.profile.id")

			Expect(operand).To(BeIdenticalTo("owner"))
			Expect(value).To(BeIdenticalTo(1))
			Expect(isFilter).To(BeTrue())
		})

		It("must reveal non-existing properties as nil", func() {
			operand, value, isFilter := resolver.reveal("sbj.some.nonexisting.prop", "sbj.another")

			Expect(operand).To(BeNil())
			Expect(value).To(BeNil())
			Expect(isFilter).To(BeFalse())
		})
	})

	Describe("Rules", func() {

		It("must evaluate sbj EQ condition", func() {
			condition := JsonToObject(`{"sbj.role": "admin"}`)

			result, _ := resolver.evaluateCondition(condition)

			Expect(result).To(BeTrue())
		})

		It("must evaluate sbj IN condition", func() {
			condition := JsonToObject(`{"sbj.role": {"in": ["manager", "admin"]}}`)

			result, _ := resolver.evaluateCondition(condition)

			Expect(result).To(BeTrue())
		})

		It("must evaluate sbj NOT condition", func() {
			condition := JsonToObject(`{"sbj.role": {"not": "manager"}}`)

			result, _ := resolver.evaluateCondition(condition)

			Expect(result).To(BeTrue())
		})

		It("must evaluate OR sbj condition", func() {
			condition := JsonToObject(`{"or": [
				{"sbj.role": {"not": "manager"}},
				{"sbj.id": 5}
			]}`)

			result, _ := resolver.evaluateCondition(condition)
			Expect(result).To(BeTrue())
		})

		It("must evaluate AND sbj condition", func() {
			condition := JsonToObject(`{"and": [
				{"sbj.role": {"not": "manager"}},
				{"sbj.id": 10}	
			]}`)

			result, _ := resolver.evaluateCondition(condition)
			Expect(result).To(BeTrue())
		})

		It("must evaluate and or condition", func() {
			condition := JsonToObject(`{
                "or": [
					{"obj.executor.account": "sbj.id"},
					{"obj.responsible.account": "sbj.id"}
                ],
				"sbj.role": "admin"}`)
			result, _ := resolver.evaluateCondition(condition)
			Expect(result).To(BeTrue())

		})

		It("must evaluate wildcard value", func() {
			condition := JsonToObject(`{"sbj.role": "*"}`)
			result, _ := resolver.evaluateCondition(condition)
			Expect(result).To(BeTrue())
		})

		It("must evaluate condition with null/non-existing arguments", func() {
			condition := JsonToObject(`{"sbj.none.existing.field": null}`)
			result, _ := resolver.evaluateCondition(condition)
			Expect(result).To(BeTrue())
		})
	})

	Describe("Building filter expression", func() {
		It("Can parse rule and build correct filter expression", func() {
			condition := JsonToObject(`{
                "or": [
					{"obj.executor.account": "sbj.id"},
					{"obj.responsible.account": "sbj.id"}
                ],
				"sbj.role": "admin"}`)
			_, filterExpressions := resolver.evaluateCondition(condition)
			Expect(filterExpressions).To(HaveLen(1))

			value := filterExpressions[0].Value.([]*FilterExpression)

			Expect(value).To(HaveLen(2))
			Expect(value[0].Operand).To(Equal("executor.account"))
			Expect(value[0].Value.(int)).To(Equal(10))
			Expect(value[1].Operand).To(Equal("responsible.account"))
			Expect(value[1].Value.(int)).To(Equal(10))
		})

		It("Returns nil if there are no suitable rules to build filter expression", func() {
			condition := JsonToObject(`{"sbj.role": "admin"}`)
			_, rule := resolver.EvaluateRule(map[string]interface{}{"rule": condition, "result": "allow"})
			Expect(rule.Filter).To(BeNil())
		})
	})

	Describe("Abac hierachical objects test", func() {
		appConfig := utils.GetConfig()
		db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)

		//transaction managers
		dbTransactionManager := object.NewPgDbTransactionManager(db)

		metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager, object.NewCache(), db)
		metaStore := object.NewStore(metaDescriptionSyncer, dbTransactionManager)
		dataProcessor, _ := object.NewProcessor(metaStore, dbTransactionManager)

		abacTree := JsonToObject(`{
			"t_client": {
				"data_GET": [
					{ "result": "allow", "rule": { "sbj.role": "admin" }, "mask": [] },
					{ "result": "allow", "rule": { "sbj.role": "partner", "obj.owner": "sbj.id" }, "mask": [] },
					{ "result": "allow", "rule": { "sbj.role": "manager", "obj.manager": "sbj.id"}, "mask": ["total"] }
				]
			},
			"t_payment": {
				"data_GET": [
					{"result": "allow", "rule": { "sbj.role": { "in": ["admin", "partner"] } }, "mask": [] },
					{"result": "allow", "rule": { "sbj.role": "manager", "obj.responsible": "sbj.id" }, "mask": [] }
				]
			},
			"t_employee": {
				"data_GET": [
					{"result": "allow", "rule": { "sbj.role": "admin" }, "mask": [] },
					{"result": "deny", "rule": { "sbj.role": { "not": "admin"} }, "mask": [] }
				]
			}
		}`)

		AfterEach(func() {
			metaStore.Flush()
		})

		It("Must filter Custodian Nodes", func() {
			metaEmployee, err := metaStore.NewMeta(&description.MetaDescription{
				Name: "t_employee",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name: "id", Type: description.FieldTypeNumber, Optional: true,
						Def: map[string]interface{}{"func": "nextval"},
					},
					{Name: "total", Type: description.FieldTypeNumber, Optional: true},
				},
			})
			Expect(err).To(BeNil())
			err = metaStore.Create(metaEmployee)
			Expect(err).To(BeNil())

			metaClient, err := metaStore.NewMeta(&description.MetaDescription{
				Name: "t_client",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name: "id", Type: description.FieldTypeNumber, Optional: true,
						Def: map[string]interface{}{"func": "nextval"},
					},
					{Name: "name", Type: description.FieldTypeString, Optional: true},
					{Name: "total", Type: description.FieldTypeNumber, Optional: true},
					{Name: "owner", Type: description.FieldTypeNumber, Optional: true},
					{Name: "manager", Type: description.FieldTypeNumber, Optional: true},
					{
						Name: "employee", Type: description.FieldTypeObject, LinkType: description.LinkTypeInner,
						LinkMeta: "t_employee", Optional: false,
					},
				},
			})
			Expect(err).To(BeNil())
			err = metaStore.Create(metaClient)
			Expect(err).To(BeNil())

			metaPayment, err := metaStore.NewMeta(&description.MetaDescription{
				Name: "t_payment",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name: "id", Type: description.FieldTypeNumber, Optional: true,
						Def: map[string]interface{}{"func": "nextval"},
					},
					{
						Name: "client", Type: description.FieldTypeObject, LinkType: description.LinkTypeInner,
						LinkMeta: "t_client", Optional: false,
					},
					{Name: "responsible", Type: description.FieldTypeNumber, Optional: false},
					{Name: "total", Type: description.FieldTypeNumber, Optional: true},
				},
			})
			Expect(err).To(BeNil())
			err = metaStore.Create(metaPayment)
			Expect(err).To(BeNil())

			mdClientNew := description.MetaDescription{
				Name: "t_client",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name: "id", Type: description.FieldTypeNumber, Optional: true,
						Def: map[string]interface{}{"func": "nextval"},
					},
					{Name: "name", Type: description.FieldTypeString, Optional: true},
					{Name: "total", Type: description.FieldTypeNumber, Optional: true},
					{Name: "owner", Type: description.FieldTypeNumber, Optional: true},
					{Name: "manager", Type: description.FieldTypeNumber, Optional: true},
					{
						Name: "employee", Type: description.FieldTypeObject, LinkType: description.LinkTypeInner,
						LinkMeta: "t_employee", Optional: false,
					}, {
						Name: "payments", Type: description.FieldTypeArray, LinkType: description.LinkTypeOuter,
						LinkMeta: "t_payment", OuterLinkField: "client", Optional: true,
					},
				},
			}
			(&description.NormalizationService{}).Normalize(&mdClientNew)
			metaClientNew, err := metaStore.NewMeta(&mdClientNew)
			Expect(err).To(BeNil())

			_, err = metaStore.Update(metaClient.Name, metaClientNew, true, true)
			Expect(err).To(BeNil())

			recordEmployee, err := dataProcessor.CreateRecord(metaEmployee.Name, map[string]interface{}{}, auth.User{})

			recordClientOne, err := dataProcessor.CreateRecord(
				metaClient.Name,
				map[string]interface{}{
					"name": "client_1", "total": 9000, "owner": 1, "manager": 1, "employee": recordEmployee.Data["id"],
				}, auth.User{},
			)

			_, err = dataProcessor.CreateRecord(
				metaClient.Name,
				map[string]interface{}{
					"name": "client_1", "total": 100500, "owner": 2, "employee": recordEmployee.Data["id"],
				}, auth.User{},
			)

			_, err = dataProcessor.CreateRecord(
				metaPayment.Name,
				map[string]interface{}{"client": recordClientOne.Data["id"], "responsible": 1, "total": 1488}, auth.User{},
			)

			_, err = dataProcessor.CreateRecord(
				metaPayment.Name,
				map[string]interface{}{"client": recordClientOne.Data["id"], "responsible": 2, "total": 7777}, auth.User{},
			)

			abac := GetTroodABAC(
				map[string]interface{}{
					"sbj": User{
						1,
						"manager@demo.com",
						"active",
						"manager",
						map[string]interface{}{
							"id":   1,
							"name": "John",
						}},
					"ctx": nil,
				},
				abacTree,
				"deny",
			)

			client, _ := dataProcessor.Get(
				"t_client", "1",
				nil, nil, 2, false,
			)

			ok, filtered := abac.MaskRecord(client, "data_GET")

			Expect(ok).To(BeTrue())

			Expect(filtered.(*object.Record).Data["employee"]).To(Equal(map[string]string{"access": "denied"}))
			Expect(filtered.(*object.Record).Data["total"]).To(Equal(map[string]string{"access": "denied"}))
			Expect(filtered.(*object.Record).Data["payments"]).To(HaveLen(1))
		})
	})

	Describe("Allow/Deny resolutions", func() {
		It("Must resolve to True if only obj rules are set in policy", func() {
			condition := JsonToObject(`{"obj.color": "red"}`)

			result, filters := resolver.evaluateCondition(condition)

			Expect(result).To(BeTrue())
			Expect(filters).NotTo(BeNil())
		})
	})
})
