package constructor_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestConstructor(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Constructor Suite")
}
