package abac

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)


var _ = Describe("Utils", func() {
	It("Must remove map attributes", func() {
		test_obj := map[string]interface{}{
			"a": 1,
			"b": 2,
			"c": map[string]interface{}{
				"x": 10,
				"y": 20,
				"z": 30,
			},
		}

		no_b := RemoveAttributeByPath(test_obj, "b")
		Expect(no_b).NotTo(HaveKey("b"))

		no_a := RemoveAttributeByPath(test_obj, "c.z")
		no_c_z := no_a.(map[string]interface{})

		Expect(no_c_z["c"]).NotTo(HaveKey("z"))
	})
})