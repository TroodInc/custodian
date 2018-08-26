package meta

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestMeta(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "MetaDescription Suite")
}
