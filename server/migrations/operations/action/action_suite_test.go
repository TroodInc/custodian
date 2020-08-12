package action_test

import (
	"github.com/onsi/ginkgo/reporters"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestAction(t *testing.T) {
	RegisterFailHandler(Fail)
	if ci := os.Getenv("CI"); ci != "" {
		teamcityReporter := reporters.NewTeamCityReporter(os.Stdout)
		RunSpecsWithCustomReporters(t, "Action Suite", []Reporter{teamcityReporter})
	} else {
		RunSpecs(t, "Action Suite")
	}
}
