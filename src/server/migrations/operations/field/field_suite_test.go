package field_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestField(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "NewField Suite")
}
