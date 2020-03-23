package driver

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/xo/dburl"
	"server/object"
	"server/object/meta"
	"utils"
)

var _ = Describe("Test Json driver", func() {
	var driver *PostgresDriver
	appConfig := utils.GetConfig()
	db, _ := dburl.Open(appConfig.DbConnectionUrl)

	BeforeEach(func() {
		driver = NewPostgresDriver(appConfig.DbConnectionUrl)
	})

	AfterEach(func() {
		db.Exec("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
	})

	It("Must get meta tables list", func() {
		metaName := utils.RandomString(8)
		db.Exec(
			fmt.Sprintf(
				"CREATE SEQUENCE o_%[1]s_id_seq; CREATE TABLE o_%[1]s (id int PRIMARY KEY DEFAULT nextval('o_%[1]s_id_seq'));",
				metaName,
			),
		)

		metaTablesList := driver.getMetaList()
		Expect(metaTablesList).To(ContainElement(metaName))
	})

	It("Must create meta table", func() {
		metaObj := object.GetBaseMetaData(utils.RandomString(8))
		err := driver.Create(metaObj)

		Expect(err).To(BeNil())

		metaTablesList := driver.getMetaList()
		Expect(metaTablesList).To(ContainElement(metaObj.Name))
	})

	It("Must get meta by name", func() {
		metaObj := object.GetBaseMetaData(utils.RandomString(8))
		driver.Create(metaObj)

		metaToTest := driver.Get(metaObj.Name)
		Expect(metaToTest).NotTo(BeNil())

		Expect(metaToTest).To(Equal(map[string]interface{}{
			"name": metaObj.Name,
			"key": "id",
			"fields": map[string]interface{}{
				"type": meta.FieldTypeNumber,
				"unique": true,
				"optional": false,
				"default": map[string]interface{}{"func": "nextval"},
			},
		}))
	})
	//
	//It("Must remove meta table", func() {
	//
	//})
})