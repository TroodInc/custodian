package abac_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestAbac(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Abac Suite")
}
