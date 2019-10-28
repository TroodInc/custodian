package abac_test

import (
	"github.com/onsi/ginkgo/reporters"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestAbac(t *testing.T) {
	RegisterFailHandler(Fail)
	if ci := os.Getenv("CI"); ci != "" {
		teamcityReporter := reporters.NewTeamCityReporter(os.Stdout)
		RunSpecsWithCustomReporters(t, "Abac Suite", []Reporter{teamcityReporter})
	} else {
		RunSpecs(t, "Abac Suite")
	}
}
