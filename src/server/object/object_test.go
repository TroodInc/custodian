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

var _ = Describe("File MetaDescription driver", func() {
	fileMetaDriver := meta.NewFileMetaDescriptionSyncer("./")
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := file_transaction.NewFileMetaDescriptionTransactionManager(fileMetaDriver.Remove, fileMetaDriver.Create)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer, globalTransactionManager)

	BeforeEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("can restore objects on rollback", func() {

		Context("having an object", func() {
			metaDescription := description.GetBasicMetaDescription("random")
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			fileMetaDriver.Create(globalTransaction.MetaDescriptionTransaction, *metaDescription)

			Context("and this object is removed within transaction", func() {
				metaDescriptionList, _, _ := metaStore.List()
				fileMetaTransaction, err := fileMetaTransactionManager.BeginTransaction(metaDescriptionList)
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

			globalTransactionManager.CommitTransaction(globalTransaction)
		})
	})

	It("removes objects created during transaction on rollback", func() {
		Context("having an object", func() {
			metaDescription := description.GetBasicMetaDescription("random")
			metaDescriptionList, _, _ := metaStore.List()
			metaTransaction, err := fileMetaTransactionManager.BeginTransaction(metaDescriptionList)
			Expect(err).To(BeNil())
			err = fileMetaDriver.Create(metaTransaction, *metaDescription)
			Expect(err).To(BeNil())

			Context("and another object is created within new transaction", func() {
				metaDescriptionList, _, _ := metaStore.List()
				metaTransaction, err := fileMetaTransactionManager.BeginTransaction(metaDescriptionList)
				Expect(err).To(BeNil())

				bMetaDescription := description.GetBasicMetaDescription("random")
				fileMetaDriver.Create(metaTransaction, *bMetaDescription)

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
