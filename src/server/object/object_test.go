package object

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"utils"
)

var _ = Describe("File MetaDescription driver", func() {
	fileMetaDriver := transactions.NewFileMetaDescriptionSyncer("./")
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(fileMetaDriver)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := NewStore(fileMetaDriver, syncer, globalTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("can restore objects on rollback", func() {

		Context("having an object", func() {
			metaDescription := GetBaseMetaData(utils.RandomString(8))
			globalTransaction, err := globalTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			fileMetaDriver.Create(globalTransaction.MetaDescriptionTransaction, metaDescription.Name, metaDescription.ForExport())

			Context("and this object is removed within transaction", func() {
				fileMetaTransaction, err := fileMetaTransactionManager.BeginTransaction()
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
			metaDescription := GetBaseMetaData(utils.RandomString(8))
			metaTransaction, err := fileMetaTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())
			err = fileMetaDriver.Create(metaTransaction, metaDescription.Name, metaDescription.ForExport())
			Expect(err).To(BeNil())

			Context("and another object is created within new transaction", func() {
				metaTransaction, err := fileMetaTransactionManager.BeginTransaction()
				Expect(err).To(BeNil())

				bMetaDescription := GetBaseMetaData(utils.RandomString(8))
				fileMetaDriver.Create(metaTransaction, bMetaDescription.Name, bMetaDescription.ForExport())

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
