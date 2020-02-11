package object

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"utils"
)

var _ = Describe("Inner generic field", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	metaDescriptionSyncer := transactions.NewFileMetaDescriptionSyncer("./")
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

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
		bMetaDescription.Fields = append(bMetaDescription.Fields, &Field{
			Name:     "a",
			Type:     FieldTypeObject,
			LinkType: LinkTypeInner,
			LinkMeta: aMetaObj,
		})
		bMetaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		aMetaObj, _, err = metaStore.Get(aMetaObj.Name, true)
		Expect(err).To(BeNil())

		reverseField := aMetaObj.FindField(bMetaObj.Name + "_set")
		Expect(reverseField).NotTo(BeNil())
		Expect(reverseField.Type).To(Equal(FieldTypeArray))
		Expect(reverseField.LinkType).To(Equal(LinkTypeOuter))
		Expect(reverseField.LinkMeta.Name).To(Equal(bMetaObj.Name))
	})
})
