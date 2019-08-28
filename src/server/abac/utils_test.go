package abac

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)


var _ = Describe("Utils", func() {
	var test_obj = map[string]interface{}{
		"a": 1,
		"b": 2,
		"c": map[string]interface{}{
			"x": 10,
			"y": 20,
			"z": 30,
		},
	}

	It("Must update map attributes", func() {
		obj := SetAttributeByPath(test_obj, "c.z", "update_test")
		Expect(obj["c"].(map[string]interface{})["z"]).To(Equal("update_test"))
	})

	It("Must return field value from path", func() {
		Context("first level", func() {
			value, ok := GetAttributeByPath(test_obj, "b")
			Expect(ok).To(BeTrue())
			Expect(value).To(Equal(2))
		})

		Context("Other depth", func() {
			value, ok := GetAttributeByPath(test_obj, "c.x")
			Expect(ok).To(BeTrue())
			Expect(value).To(Equal(10))
		})
	})

	It("Must return False when cant get value from path", func() {
		Context("first level", func() {
			value, ok := GetAttributeByPath(test_obj, "some_field")
			Expect(ok).To(BeFalse())
			Expect(value).To(BeNil())
		})

		Context("Other depth", func() {
			value, ok := GetAttributeByPath(test_obj, "z.y")
			Expect(ok).To(BeFalse())
			Expect(value).To(BeNil())
		})
	})

	It("Must remove map attributes", func() {
		Context("Setting nil", func() {
			no_b := RemoveMapAttributeByPath(test_obj, "b", true)
			Expect(no_b["b"]).To(BeNil())

			no_c_z := RemoveMapAttributeByPath(test_obj, "c.z", true)
			Expect(no_c_z["c"].(map[string]interface{})["z"]).To(BeNil())
		})

		Context("Full remove", func() {
			no_b := RemoveMapAttributeByPath(test_obj, "b", false)
			Expect(no_b).NotTo(HaveKey("b"))

			no_c_z := RemoveMapAttributeByPath(test_obj, "c.z", false)
			Expect(no_c_z["c"]).NotTo(HaveKey("z"))
		})

	})
})