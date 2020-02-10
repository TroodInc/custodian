package object

import (
	"github.com/onsi/ginkgo/reporters"
	"os"
	"server/object/description"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestData(t *testing.T) {
	RegisterFailHandler(Fail)
	if ci := os.Getenv("CI"); ci != "" {
		teamcityReporter := reporters.NewTeamCityReporter(os.Stdout)
		RunSpecsWithCustomReporters(t, "Objects Suite", []Reporter{teamcityReporter})
	} else {
		RunSpecs(t, "Objects Suite")
	}
}

func GetBaseMetaData(name string) *description.MetaDescription {
	return &description.MetaDescription{
		Name: name,
		Key:  "id",
		Cas:  false,
		Fields: []description.Field{
			{
				Name: "id",
				Type: description.FieldTypeNumber,
				Def: map[string]interface{}{
					"func": "nextval",
				},
			},
		},
	}
}