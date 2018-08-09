package meta

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/meta"
)

var _ = Describe("File Meta driver", func() {
	fileMetaDriver := object.NewFileMetaDriver("./")

	flushMeta := func() {
		metaList, _, _ := fileMetaDriver.List()
		for _, meta := range *metaList {
			fileMetaDriver.Remove(meta.Name)
		}
	}
	BeforeEach(func() {
		flushMeta()
	})
	AfterEach(func() {
		flushMeta()
	})

	It("can restore objects on rollback", func() {
		Context("having an object", func() {
			metaDescription := object.MetaDescription{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: false,
					},
				},
			}
			fileMetaDriver.BeginTransaction()
			fileMetaDriver.Create(metaDescription)
			fileMetaDriver.CommitTransaction()

			Context("and this object is removed within transaction", func() {
				fileMetaDriver.BeginTransaction()
				fileMetaDriver.Remove(metaDescription.Name)
				_, ok, _ := fileMetaDriver.Get(metaDescription.Name)
				Expect(ok).To(BeFalse())
				Context("object should be restored after rollback", func() {
					fileMetaDriver.RollbackTransaction()
					_, ok, _ := fileMetaDriver.Get(metaDescription.Name)
					Expect(ok).To(BeTrue())
				})
			})
		})
	})

	It("removes objects which were created during transaction on rollback", func() {
		Context("having an object", func() {
			metaDescription := object.MetaDescription{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: false,
					},
				},
			}
			fileMetaDriver.BeginTransaction()
			fileMetaDriver.Create(metaDescription)
			fileMetaDriver.CommitTransaction()

			Context("and another object is created within new transaction", func() {
				fileMetaDriver.BeginTransaction()

				metaDescription := object.MetaDescription{
					Name: "b",
					Key:  "id",
					Cas:  false,
					Fields: []object.Field{
						{
							Name:     "id",
							Type:     object.FieldTypeNumber,
							Optional: false,
						},
					},
				}
				fileMetaDriver.BeginTransaction()
				fileMetaDriver.Create(metaDescription)

				Context("new object should be removed after rollback", func() {
					fileMetaDriver.RollbackTransaction()
					_, ok, _ := fileMetaDriver.Get(metaDescription.Name)
					Expect(ok).To(BeFalse())
				})
			})
		})
	})
})
