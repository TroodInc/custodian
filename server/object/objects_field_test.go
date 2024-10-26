package object

import (
	"custodian/server/object/description"

	"custodian/utils"
	"encoding/json"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Objects field", func() {
	appConfig := utils.GetConfig()
	db, _ := NewDbConnection(appConfig.DbConnectionUrl)

	dbTransactionManager := NewPgDbTransactionManager(db)

	metaDescriptionSyncer := NewPgMetaDescriptionSyncer(dbTransactionManager, NewCache(), db)
	metaStore := NewStore(metaDescriptionSyncer, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("can unmarshal meta with 'objects' field", func() {
		data := map[string]interface{}{
			"name": "a",
			"key":  "id",
			"fields": []interface{}{
				map[string]string{
					"name":     "b_list",
					"type":     "objects",
					"linkType": "inner",
				},
			},
			"cas": false,
		}
		//marshal data into string
		var metaDescription description.MetaDescription
		buffer, err := json.Marshal(data)
		Expect(err).To(BeNil())

		//unmarshal string into metaDescription
		err = json.Unmarshal(buffer, &metaDescription)
		Expect(err).To(BeNil())
		Expect(metaDescription.Fields).To(HaveLen(1))
		Expect(metaDescription.Fields[0].Name).To(Equal("b_list"))
		Expect(metaDescription.Fields[0].Type).To(Equal(description.FieldTypeObjects))
		Expect(metaDescription.Fields[0].LinkType).To(Equal(description.LinkTypeInner))
	})

	It("can build meta with 'objects' field and filled 'throughLink'", func() {
		aMetaDescription := GetBaseMetaData(utils.RandomString(8) + "_a")
		bMetaDescription := GetBaseMetaData(utils.RandomString(8) + "_b")

		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		bMetaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		aMetaDescription.Fields = append(aMetaDescription.Fields, description.Field{
			Name:     "b",
			Type:     description.FieldTypeObjects,
			LinkMeta: bMetaDescription.Name,
			LinkType: description.LinkTypeInner,
		})

		//check field's properties
		updatedAMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		Expect(updatedAMetaObj.Fields).To(HaveLen(2))
		Expect(updatedAMetaObj.Fields[1].LinkMeta.Name).To(Equal(bMetaDescription.Name))
		Expect(updatedAMetaObj.Fields[1].Type).To(Equal(description.FieldTypeObjects))
		Expect(updatedAMetaObj.Fields[1].LinkType).To(Equal(description.LinkTypeInner))

		//create meta and check through meta was created
		_, err = metaStore.Update(updatedAMetaObj.Name, updatedAMetaObj, true, true)
		Expect(err).To(BeNil())

		throughMeta, _, err := metaStore.Get(updatedAMetaObj.Fields[1].LinkThrough.Name, false)
		Expect(err).To(BeNil())

		Expect(throughMeta.Name).To(Equal(fmt.Sprintf("%s__%s", aMetaDescription.Name, bMetaDescription.Name)))
		Expect(throughMeta.Fields).To(HaveLen(3))

		Expect(throughMeta.Fields[1].Name).To(Equal(aMetaDescription.Name))
		Expect(throughMeta.Fields[1].Type).To(Equal(description.FieldTypeObject))

		Expect(throughMeta.Fields[2].Name).To(Equal(bMetaDescription.Name))
		Expect(throughMeta.Fields[2].Type).To(Equal(description.FieldTypeObject))
	})
})
