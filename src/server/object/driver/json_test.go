package driver

import (
	."github.com/onsi/ginkgo"
	."github.com/onsi/gomega"
	"io/ioutil"
	"os"
	"path/filepath"
	"server/object"
	"utils"
)

var _ = Describe("Test Json driver", func() {
	var driver *JsonDriver
	var tmpMetaDir string
	appConfig := utils.GetConfig()

	BeforeEach(func() {
		tmpMetaDir, _ = ioutil.TempDir("", "meta_")
		driver = NewJsonDriver(appConfig.DbConnectionUrl, tmpMetaDir)
	})

	AfterEach(func() {
		os.RemoveAll(tmpMetaDir)
	})

	It("Must make filepath from name", func() {
		name := utils.RandomString(8)
		path := driver.getMetaFileName(name)

		Expect(path).To(Equal(tmpMetaDir + "/" + name + ".json"))
	})

	It("Must get meta files list", func() {
		tmpFile, _ := ioutil.TempFile(tmpMetaDir, "*.json")
		metaFilesList := driver.getMetaList()

		Expect(metaFilesList).To(HaveLen(1))
		Expect(metaFilesList[0]).To(Equal(tmpFile.Name()))
	})

	It("Must create meta file", func() {
		testMeta := object.GetBaseMetaData(utils.RandomString(8))
		err := driver.Create(testMeta)

		metaFile, err := ioutil.ReadFile(tmpMetaDir + "/" + testMeta.Name + ".json")

		Expect(err).To(BeNil())
		Expect(metaFile).NotTo(HaveLen(0))
	})

	It("Must remove meta file", func() {
		testMeta := object.GetBaseMetaData(utils.RandomString(8))
		driver.Create(testMeta)

		ok, err := driver.Remove(testMeta.Name)
		filse, _ := filepath.Glob(tmpMetaDir + "/*.json")

		Expect(ok).To(BeTrue())
		Expect(err).To(BeNil())
		Expect(filse).To(BeEmpty())
	})
})