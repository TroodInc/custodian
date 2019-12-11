package abac

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Utils", func() {
	var testObj = map[string]interface{}{
		"a": 1,
		"b": 2,
		"c": map[string]interface{}{
			"x": 10,
			"y": 20,
			"z": 30,
		},
	}

	It("Must update map attributes", func() {
		obj := SetAttributeByPath(testObj, "c.z", "update_test")
		Expect(obj["c"].(map[string]interface{})["z"]).To(Equal("update_test"))
	})

	It("Must return field value from path", func() {
		Context("first level", func() {
			value, ok := GetAttributeByPath(testObj, "b")
			Expect(ok).To(BeTrue())
			Expect(value).To(Equal(2))
		})

		Context("Other depth", func() {
			value, ok := GetAttributeByPath(testObj, "c.x")
			Expect(ok).To(BeTrue())
			Expect(value).To(Equal(10))
		})
	})

	It("Must return False when cant get value from path", func() {
		Context("first level", func() {
			value, ok := GetAttributeByPath(testObj, "some_field")
			Expect(ok).To(BeFalse())
			Expect(value).To(BeNil())
		})

		Context("Other depth", func() {
			value, ok := GetAttributeByPath(testObj, "z.y")
			Expect(ok).To(BeFalse())
			Expect(value).To(BeNil())
		})
	})

	It("Must remove map attributes", func() {
		Context("Setting nil", func() {
			noB := RemoveMapAttributeByPath(testObj, "b", true)
			Expect(noB["b"]).To(BeNil())

			noCZ := RemoveMapAttributeByPath(testObj, "c.z", true)
			Expect(noCZ["c"].(map[string]interface{})["z"]).To(BeNil())
		})

		Context("Full remove", func() {
			noB := RemoveMapAttributeByPath(testObj, "b", false)
			Expect(noB).NotTo(HaveKey("b"))

			noCZ := RemoveMapAttributeByPath(testObj, "c.z", false)
			Expect(noCZ["c"]).NotTo(HaveKey("z"))
		})

	})
})
