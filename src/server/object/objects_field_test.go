package object

import (
	"encoding/json"
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/object/driver"
	"server/object/meta"
	"utils"
)

var _ = Describe("Objects field", func() {
	appConfig := utils.GetConfig()

	driver := driver.NewJsonDriver(appConfig.DbConnectionUrl, "./")
	metaStore  := NewStore(driver)

	AfterEach(func() {
		metaStore.Flush()
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
		var metaDescription meta.Meta
		buffer, err := json.Marshal(data)
		Expect(err).To(BeNil())

		//unmarshal string into metaDescription
		err = json.Unmarshal(buffer, &metaDescription)
		Expect(err).To(BeNil())
		Expect(metaDescription.Fields).To(HaveLen(1))
		Expect(metaDescription.Fields).To(HaveKey("b_list"))
		Expect(metaDescription.Fields["b_list"].Type).To(Equal(meta.FieldTypeObjects))
		Expect(metaDescription.Fields["b_list"].LinkType).To(Equal(meta.LinkTypeInner))
	})

	It("can build meta with 'objects' field and filled 'throughLink'", func() {
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		bMetaDescription := GetBaseMetaData(utils.RandomString(8))

		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(aMetaObj)

		bMetaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(bMetaObj)

		aMetaDescription.AddField(&meta.Field{
			Name:     "b",
			Type:     meta.FieldTypeObjects,
			LinkMeta: bMetaDescription,
			LinkType: meta.LinkTypeInner,
		})

		//check field's properties
		updatedAMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		Expect(updatedAMetaObj.Fields).To(HaveLen(2))
		Expect(updatedAMetaObj.Fields["b"].LinkMeta.Name).To(Equal(bMetaDescription.Name))
		Expect(updatedAMetaObj.Fields["b"].Type).To(Equal(meta.FieldTypeObjects))
		Expect(updatedAMetaObj.Fields["b"].LinkType).To(Equal(meta.LinkTypeInner))

		//create meta and check through meta was created
		metaStore.Update(updatedAMetaObj)

		throughMeta := metaStore.Get(updatedAMetaObj.Fields["b"].LinkThrough.Name)
		Expect(err).To(BeNil())

		Expect(throughMeta.Name).To(Equal(fmt.Sprintf("%s__%s", aMetaDescription.Name, bMetaDescription.Name)))
		Expect(throughMeta.Fields).To(HaveLen(3))

		// TODO: Figure out to fix after Meta.Fields became Map
		Expect(throughMeta.Fields["b"].Name).To(Equal(aMetaDescription.Name))
		Expect(throughMeta.Fields["b"].Type).To(Equal(meta.FieldTypeObject))

		Expect(throughMeta.Fields["?"].Name).To(Equal(bMetaDescription.Name))
		Expect(throughMeta.Fields["?"].Type).To(Equal(meta.FieldTypeObject))
	})
})
