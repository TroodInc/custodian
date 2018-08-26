package object

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"utils"
	"server/pg"
	"server/object/meta"
	"server/transactions/file_transaction"

	"server/object/description"
	pg_transactions "server/pg/transactions"
	"server/transactions"
)

var _ = Describe("File Meta driver", func() {
	fileMetaDriver := meta.NewFileMetaDriver("./")
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := file_transaction.NewFileMetaDescriptionTransactionManager(fileMetaDriver.Remove, fileMetaDriver.Create)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	BeforeEach(func() {
		var err error

		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		err = metaStore.Flush(globalTransaction)
		Expect(err).To(BeNil())
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	AfterEach(func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		err = metaStore.Flush(globalTransaction)
		Expect(err).To(BeNil())

		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("can restore objects on rollback", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		defer func() { globalTransactionManager.CommitTransaction(globalTransaction) }()

		Context("having an object", func() {
			metaDescription := description.MetaDescription{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: false,
					},
				},
			}
			fileMetaDriver.Create(globalTransaction.MetaDescriptionTransaction, metaDescription)

			Context("and this object is removed within transaction", func() {
				metaDescriptionList, _, _ := metaStore.List()
				fileMetaTransaction, err := fileMetaTransactionManager.BeginTransaction(*metaDescriptionList)
				Expect(err).To(BeNil())

				fileMetaDriver.Remove(metaDescription.Name)
				_, ok, _ := fileMetaDriver.Get(metaDescription.Name)
				Expect(ok).To(BeFalse())
				Context("object should be restored after rollback", func() {
					fileMetaTransactionManager.RollbackTransaction(fileMetaTransaction)
					_, ok, _ := fileMetaDriver.Get(metaDescription.Name)
					Expect(ok).To(BeTrue())
					//	clean up
					fileMetaDriver.Remove(metaDescription.Name)
				})
			})
		})
	})

	It("removes objects created during transaction on rollback", func() {
		Context("having an object", func() {
			metaDescription := description.MetaDescription{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: false,
					},
				},
			}
			metaDescriptionList, _, _ := metaStore.List()
			metaTransaction, err := fileMetaTransactionManager.BeginTransaction(*metaDescriptionList)
			Expect(err).To(BeNil())
			err = fileMetaDriver.Create(metaTransaction, metaDescription)
			Expect(err).To(BeNil())

			Context("and another object is created within new transaction", func() {
				metaDescriptionList, _, _ := metaStore.List()
				metaTransaction, err := fileMetaTransactionManager.BeginTransaction(*metaDescriptionList)
				Expect(err).To(BeNil())

				bMetaDescription := description.MetaDescription{
					Name: "b",
					Key:  "id",
					Cas:  false,
					Fields: []description.Field{
						{
							Name:     "id",
							Type:     description.FieldTypeNumber,
							Optional: false,
						},
					},
				}
				fileMetaDriver.Create(metaTransaction, bMetaDescription)

				Context("B object should be removed after rollback", func() {
					err = fileMetaTransactionManager.RollbackTransaction(metaTransaction)
					Expect(err).To(BeNil())
					_, ok, _ := fileMetaDriver.Get(bMetaDescription.Name)
					Expect(ok).To(BeFalse())
				})
				fileMetaDriver.Remove(metaDescription.Name)
			})
		})
	})
})
