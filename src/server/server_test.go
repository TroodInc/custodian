package server_test
//
//import (
//	. "github.com/onsi/ginkgo"
//	. "github.com/onsi/gomega"
//	"server"
//	"net/http"
//	"fmt"
//	"encoding/json"
//	"bytes"
//	"net/http/httptest"
//)
//
//var _ = Describe("Server", func() {
//	var httpServer *http.Server
//	var urlPrefix = "/custodian"
//	var recorder *httptest.ResponseRecorder
//	databaseConnectionOptions := "host=localhost dbname=custodian sslmode=disable"
//
//	BeforeEach(func() {
//		httpServer = server.New("localhost", "8081", urlPrefix, databaseConnectionOptions).Setup()
//		recorder = httptest.NewRecorder()
//	})
//
//	Describe("Create object", func() {
//		metaData := map[string]interface{}{
//			"name": "person",
//			"key":  "id",
//			"cas":  true,
//			"fields": []map[string]interface{}{
//				{
//					"name":     "id",
//					"type":     "number",
//					"optional": true,
//					"default": map[string]interface{}{
//						"func": "nextval",
//					},
//				}, {
//					"name":     "name",
//					"type":     "string",
//					"optional": false,
//				}, {
//					"name":     "gender",
//					"type":     "string",
//					"optional": true,
//				}, {
//					"name":     "cas",
//					"type":     "number",
//					"optional": false,
//				},
//			},
//		}
//		encodedMetaData, _ := json.Marshal(metaData)
//		var request, _ = http.NewRequest("PUT", fmt.Sprintf("%s/meta", urlPrefix), bytes.NewBuffer(encodedMetaData))
//		request.Header.Set("Content-Type", "application/json")
//
//		Context("when data is OK", func() {
//
//			It("returns a empty body", func() {
//				httpServer.Handler.ServeHTTP(recorder, request)
//				Expect(recorder.Body.String()).To(Equal("[]"))
//			})
//		})
//
//	})
//})
