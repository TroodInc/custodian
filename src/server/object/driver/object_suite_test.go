package driver

import (
	"github.com/onsi/ginkgo/reporters"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestData(t *testing.T) {
	RegisterFailHandler(Fail)
	if ci := os.Getenv("CI"); ci != "" {
		teamcityReporter := reporters.NewTeamCityReporter(os.Stdout)
		RunSpecsWithCustomReporters(t, "Drivers Suite", []Reporter{teamcityReporter})
	} else {
		RunSpecs(t, "Drivers Suite")
	}
}

