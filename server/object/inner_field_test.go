package object

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"custodian/server/pg"
	"custodian/utils"
	"custodian/server/object/meta"
	"custodian/server/transactions/file_transaction"
	"custodian/server/transactions"
	"custodian/server/object/description"
)

var _ = Describe("Inner generic field", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg.NewPgDbTransactionManager(dataManager)
	metaDescriptionSyncer := pg.NewPgMetaDescriptionSyncer(dbTransactionManager)

	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("automatically creates reverse outer link field", func() {
		aMetaObj, err := metaStore.NewMeta(GetBaseMetaData(utils.RandomString(8)))
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		bMetaDescription := GetBaseMetaData(utils.RandomString(8))
		bMetaDescription.Fields = append(bMetaDescription.Fields, description.Field{
			Name:     "a",
			Type:     description.FieldTypeObject,
			LinkType: description.LinkTypeInner,
			LinkMeta: aMetaObj.Name,
		})
		bMetaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		aMetaObj, _, err = metaStore.Get(aMetaObj.Name, true)
		Expect(err).To(BeNil())

		reverseField := aMetaObj.FindField(bMetaObj.Name + "_set")
		Expect(reverseField).NotTo(BeNil())
		Expect(reverseField.Type).To(Equal(description.FieldTypeArray))
		Expect(reverseField.LinkType).To(Equal(description.LinkTypeOuter))
		Expect(reverseField.LinkMeta.Name).To(Equal(bMetaObj.Name))
	})
})
