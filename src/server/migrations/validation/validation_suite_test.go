package validation_test

import (
	"github.com/onsi/ginkgo/reporters"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestValidation(t *testing.T) {
	RegisterFailHandler(Fail)
	if ci := os.Getenv("CI"); ci != "" {
		teamcityReporter := reporters.NewTeamCityReporter(os.Stdout)
		RunSpecsWithCustomReporters(t, "Validation Manager Suite", []Reporter{teamcityReporter})
	} else {
		RunSpecs(t, "Validation Suite")
	}
}
