package object

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"custodian/server/object/meta"
	"custodian/server/pg"
	"custodian/server/transactions/file_transaction"
	"custodian/utils"

	pg_transactions "custodian/server/pg/transactions"
	"custodian/server/transactions"
	"custodian/server/pg_meta"
)

var _ = Describe("File MetaDescription driver", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	metaDescriptionSyncer := pg_meta.NewPgMetaDescriptionSyncer(dbTransactionManager)
	//transaction managers
	fileMetaTransactionManager := file_transaction.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer.Remove, metaDescriptionSyncer.Create)

	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("can restore objects on rollback", func() {

		Context("having an object", func() {
			metaDescription := GetBaseMetaData(utils.RandomString(8))
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			metaDescriptionSyncer.Create(globalTransaction.MetaDescriptionTransaction, *metaDescription)

			Context("and this object is removed within transaction", func() {
				metaDescriptionList, _, _ := metaStore.List()
				fileMetaTransaction, err := fileMetaTransactionManager.BeginTransaction(metaDescriptionList)
				Expect(err).To(BeNil())

				metaDescriptionSyncer.Remove(metaDescription.Name)
				_, ok, _ := metaDescriptionSyncer.Get(metaDescription.Name)
				Expect(ok).To(BeFalse())
				Context("object should be restored after rollback", func() {
					fileMetaTransactionManager.RollbackTransaction(fileMetaTransaction)
					_, ok, _ := metaDescriptionSyncer.Get(metaDescription.Name)
					Expect(ok).To(BeTrue())
					//	clean up
					metaDescriptionSyncer.Remove(metaDescription.Name)
				})
			})

			globalTransactionManager.CommitTransaction(globalTransaction)
		})
	})

	It("removes objects created during transaction on rollback", func() {
		Context("having an object", func() {
			metaDescription := GetBaseMetaData(utils.RandomString(8))
			metaDescriptionList, _, _ := metaStore.List()
			metaTransaction, err := fileMetaTransactionManager.BeginTransaction(metaDescriptionList)
			Expect(err).To(BeNil())
			err = metaDescriptionSyncer.Create(metaTransaction, *metaDescription)
			Expect(err).To(BeNil())

			Context("and another object is created within new transaction", func() {
				metaDescriptionList, _, _ := metaStore.List()
				metaTransaction, err := fileMetaTransactionManager.BeginTransaction(metaDescriptionList)
				Expect(err).To(BeNil())

				bMetaDescription := GetBaseMetaData(utils.RandomString(8))
				metaDescriptionSyncer.Create(metaTransaction, *bMetaDescription)

				Context("B object should be removed after rollback", func() {
					err = fileMetaTransactionManager.RollbackTransaction(metaTransaction)
					Expect(err).To(BeNil())
					_, ok, _ := metaDescriptionSyncer.Get(bMetaDescription.Name)
					Expect(ok).To(BeFalse())
				})
				metaDescriptionSyncer.Remove(metaDescription.Name)
			})
		})
	})
})
