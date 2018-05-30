package server_test


import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server"
	"net/http"
	"fmt"
	"encoding/json"
	"bytes"
	"net/http/httptest"
	"server/pg"
	"server/meta"
)

var _ = Describe("Server", func() {
	var httpServer *http.Server
	var urlPrefix = "/custodian"
	var recorder *httptest.ResponseRecorder
	databaseConnectionOptions := "host=localhost dbname=custodian sslmode=disable"
	syncer, _ := pg.NewSyncer(databaseConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

	BeforeEach(func() {
		httpServer = server.New("localhost", "8081", urlPrefix, databaseConnectionOptions).Setup()
		recorder = httptest.NewRecorder()
		metaStore.Flush()
	})

	It("can create the object", func() {
		Context("having valid object description", func() {
			metaData := map[string]interface{}{
				"name": "person",
				"key":  "id",
				"cas":  true,
				"fields": []map[string]interface{}{
					{
						"name":     "id",
						"type":     "number",
						"optional": true,
						"default": map[string]interface{}{
							"func": "nextval",
						},
					}, {
						"name":     "name",
						"type":     "string",
						"optional": false,
					}, {
						"name":     "gender",
						"type":     "string",
						"optional": true,
					}, {
						"name":     "cas",
						"type":     "number",
						"optional": false,
					},
				},
			}
			Context("and valid HTTP request object", func() {
				encodedMetaData, _ := json.Marshal(metaData)
				var request, _ = http.NewRequest("PUT", fmt.Sprintf("%s/meta", urlPrefix), bytes.NewBuffer(encodedMetaData))
				request.Header.Set("Content-Type", "application/json")

				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()
				Expect(responseBody).To(Equal("{\"status\":\"OK\"}"))
			})
		})
	})
})
