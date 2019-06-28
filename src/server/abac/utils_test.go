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