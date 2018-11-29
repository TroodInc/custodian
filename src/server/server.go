package server

import (
	"encoding/json"
	"logger"
	"server/data"
	"server/data/errors"
	"server/object/meta"
	"server/pg"
	"server/auth"
	"github.com/julienschmidt/httprouter"
	"io"
	"mime"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"time"
	"context"
	"github.com/getsentry/raven-go"
	"server/transactions"
	"server/transactions/file_transaction"
	pg_transactions "server/pg/transactions"
	"server/object/description"
	. "server/errors"
	. "server/streams"
	_ "net/http/pprof"
)

type CustodianApp struct {
	router        *httprouter.Router
	authenticator auth.Authenticator
}

func GetApp(cs *CustodianServer) *CustodianApp {
	var authenticator auth.Authenticator
	if cs.auth_url != "" {
		authenticator = &auth.TroodAuthenticator{
			cs.auth_url,
		}
	} else {
		authenticator = &auth.EmptyAuthenticator{}
	}

	return &CustodianApp{
		httprouter.New(),
		authenticator,
	}
}

func (app *CustodianApp) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	if user, err := app.authenticator.Authenticate(req); err == nil {

		ctx := context.WithValue(req.Context(), "auth_user", user)

		app.router.ServeHTTP(w, req.WithContext(ctx))
	} else {
		returnError(w, err)
	}
}

func softParseQuery(m url.Values, query string) (err error) {
	for query != "" {
		key := query
		if i := strings.IndexAny(key, "&;"); i >= 0 {
			key, query = key[:i], key[i+1:]
		} else {
			query = ""
		}
		if key == "" {
			continue
		}
		value := ""
		if i := strings.Index(key, "="); i >= 0 {
			key, value = key[:i], key[i+1:]
		}
		key, err1 := url.QueryUnescape(key)
		if err1 != nil {
			if err == nil {
				err = err1
			}
			continue
		}
		m[key] = append(m[key], value)
	}
	return err
}

//Custodian server description
type CustodianServer struct {
	addr, port, root string
	s                *http.Server
	db               string
	auth_url         string
}

func New(host, port, urlPrefix, databaseConnectionOptions string) *CustodianServer {
	return &CustodianServer{addr: host, port: port, root: urlPrefix, db: databaseConnectionOptions}
}

func (cs *CustodianServer) SetAddr(a string) {
	cs.addr = a
}

func (cs *CustodianServer) SetPort(p string) {
	cs.port = p
}

func (cs *CustodianServer) SetRoot(r string) {
	cs.root = r
}

func (cs *CustodianServer) SetDb(d string) {
	cs.db = d
}

func (cs *CustodianServer) SetAuth(s string) {
	cs.auth_url = s
}

func (cs *CustodianServer) Setup(enableProfiler bool) *http.Server {

	app := GetApp(cs)

	//Meta routes
	syncer, err := pg.NewSyncer(cs.db)
	dataManager, _ := syncer.NewDataManager()
	fileMetaDriver := meta.NewFileMetaDriver("./")
	metaStore := meta.NewStore(fileMetaDriver, syncer)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager)

	//transaction managers
	fileMetaTransactionManager := file_transaction.NewFileMetaDescriptionTransactionManager(fileMetaDriver.Remove, fileMetaDriver.Create)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	if err != nil {
		logger.Error("Failed to create syncer: %s", err.Error())
		panic(err)
	}

	//object operations
	app.router.GET(cs.root+"/meta", CreateJsonAction(func(r io.ReadCloser, js *JsonSink, _ httprouter.Params, q url.Values) {
		if metaList, _, err := metaStore.List(); err == nil {
			js.push(map[string]interface{}{"status": "OK", "data": metaList})
		} else {
			js.pushError(err)
		}
	}))

	app.router.GET(cs.root+"/meta/:name", CreateJsonAction(func(_ io.ReadCloser, js *JsonSink, p httprouter.Params, q url.Values) {
		//there is no need to retrieve list of objects when not modifying them
		if globalTransaction, err := globalTransactionManager.BeginTransaction(make([]*description.MetaDescription, 0)); err != nil {
			js.push(map[string]interface{}{"status": "FAIL", "error": err.Error()})
		} else {
			if metaObj, _, e := metaStore.Get(globalTransaction, p.ByName("name"), true); e == nil {
				globalTransactionManager.CommitTransaction(globalTransaction)
				js.push(map[string]interface{}{"status": "OK", "data": metaObj.DescriptionForExport()})
			} else {
				globalTransactionManager.RollbackTransaction(globalTransaction)
				js.push(map[string]interface{}{"status": "FAIL", "error": e.Error()})
			}
		}
	}))

	app.router.PUT(cs.root+"/meta", CreateJsonAction(func(r io.ReadCloser, js *JsonSink, _ httprouter.Params, q url.Values) {
		metaDescriptionList, _, _ := metaStore.List()
		if globalTransaction, err := globalTransactionManager.BeginTransaction(*metaDescriptionList); err != nil {
			js.push(map[string]interface{}{"status": "FAIL", "error": err.Error()})
		} else {
			metaObj, err := metaStore.UnmarshalIncomingJSON(r)
			if err != nil {
				js.pushError(err)
				globalTransactionManager.RollbackTransaction(globalTransaction)
				return
			}
			if e := metaStore.Create(globalTransaction, metaObj); e == nil {
				globalTransactionManager.CommitTransaction(globalTransaction)
				js.push(map[string]interface{}{"status": "OK", "data": metaObj.DescriptionForExport()})
			} else {
				globalTransactionManager.RollbackTransaction(globalTransaction)
				js.pushError(e)
			}
		}
	}))
	app.router.DELETE(cs.root+"/meta/:name", CreateJsonAction(func(_ io.ReadCloser, js *JsonSink, p httprouter.Params, q url.Values) {
		metaDescriptionList, _, _ := metaStore.List()
		if globalTransaction, err := globalTransactionManager.BeginTransaction(*metaDescriptionList); err != nil {
			js.push(map[string]interface{}{"status": "FAIL", "error": err.Error()})
		} else {
			if ok, e := metaStore.Remove(globalTransaction, p.ByName("name"), false); ok {
				globalTransactionManager.CommitTransaction(globalTransaction)
				js.pushEmpty()
			} else {
				if e != nil {
					globalTransactionManager.RollbackTransaction(globalTransaction)
					js.pushError(e)
				} else {
					globalTransactionManager.RollbackTransaction(globalTransaction)
					js.pushError(&ServerError{Status: http.StatusNotFound, Code: ErrNotFound})
				}
			}
		}
	}))
	app.router.POST(cs.root+"/meta/:name", CreateJsonAction(func(r io.ReadCloser, js *JsonSink, p httprouter.Params, q url.Values) {
		metaDescriptionList, _, _ := metaStore.List()
		if globalTransaction, err := globalTransactionManager.BeginTransaction(*metaDescriptionList); err != nil {
			js.push(map[string]interface{}{"status": "FAIL", "error": err.Error()})
		} else {
			metaObj, err := metaStore.UnmarshalIncomingJSON(r)
			if err != nil {
				js.pushError(err)
				globalTransactionManager.RollbackTransaction(globalTransaction)
				return
			}
			if _, err := metaStore.Update(globalTransaction, p.ByName("name"), metaObj, true); err == nil {
				globalTransactionManager.CommitTransaction(globalTransaction)
				js.push(map[string]interface{}{"status": "OK", "data": metaObj.DescriptionForExport()})
			} else {
				globalTransactionManager.RollbackTransaction(globalTransaction)
				js.pushError(err)
			}
		}
	}))

	//RecordSetOperations operations
	app.router.PUT(cs.root+"/data/single/:name", CreateDualJsonAction(func(src *JsonSource, sink *JsonSink, p httprouter.Params, r *http.Request) {
		user := r.Context().Value("auth_user").(auth.User)
		objectName := p.ByName("name")
		if dbTransaction, err := dbTransactionManager.BeginTransaction(); err != nil {
			sink.pushError(err)
		} else {
			if record, err := dataProcessor.CreateRecord(dbTransaction, objectName, src.Value, user); err != nil {
				dbTransactionManager.RollbackTransaction(dbTransaction)
				sink.pushError(err)
			} else {
				var depth = 1
				if i, e := strconv.Atoi(r.URL.Query().Get("depth")); e == nil {
					depth = i
				}
				objectMeta, _ := dataProcessor.GetMeta(dbTransaction, objectName)
				pkValue, _ := objectMeta.Key.ValueAsString(record.Data[objectMeta.Key.Name])
				if record, err := dataProcessor.Get(dbTransaction, objectName, pkValue, depth, false);
					err != nil {
					dbTransactionManager.RollbackTransaction(dbTransaction)
					sink.pushError(err)
				} else {
					dbTransactionManager.CommitTransaction(dbTransaction)
					sink.pushGeneric(record.Data)
				}
			}
		}
	}, false))

	app.router.PUT(cs.root+"/data/bulk/:name", CreateDualJsonStreamAction(func(stream *JsonStream, sink *JsonSinkStream, p httprouter.Params, request *http.Request) {
		defer sink.Complete(nil)
		user := request.Context().Value("auth_user").(auth.User)

		if dbTransaction, err := dbTransactionManager.BeginTransaction(); err != nil {
			sink.PushError(err)
		} else {

			e := dataProcessor.BulkCreateRecords(dbTransaction, p.ByName("name"), func() (map[string]interface{}, error) {
				if obj, eof, e := stream.Next(); e != nil {
					return nil, e
				} else if eof {
					return nil, nil
				} else {
					return obj, nil
				}
			}, func(obj map[string]interface{}) error { return sink.PourOff(obj) }, user)
			if e != nil {
				dbTransactionManager.RollbackTransaction(dbTransaction)
				sink.PushError(e)
			} else {
				dbTransactionManager.CommitTransaction(dbTransaction)
			}
		}
	}))

	app.router.GET(cs.root+"/data/single/:name/:key", CreateJsonAction(func(r io.ReadCloser, sink *JsonSink, p httprouter.Params, q url.Values) {
		if dbTransaction, err := dbTransactionManager.BeginTransaction(); err != nil {
			sink.pushError(err)
		} else {
			var depth = 2
			if i, e := strconv.Atoi(q.Get("depth")); e == nil {
				depth = i
			}

			var omitOuters = false
			if len(q.Get("omit_outers")) > 0 {
				omitOuters = true
			}

			if o, e := dataProcessor.Get(dbTransaction, p.ByName("name"), p.ByName("key"), depth, omitOuters); e != nil {
				dbTransactionManager.RollbackTransaction(dbTransaction)
				sink.pushError(e)
			} else {
				if o == nil {
					dbTransactionManager.RollbackTransaction(dbTransaction)
					sink.pushError(&ServerError{http.StatusNotFound, ErrNotFound, "record not found"})
				} else {
					dbTransactionManager.CommitTransaction(dbTransaction)
					sink.pushGeneric(o.Data)
				}
			}
		}
	}))

	app.router.GET(cs.root+"/data/bulk/:name", CreateJsonStreamAction(func(sink *JsonSinkStream, p httprouter.Params, q *url.URL) {
		var count int
		if dbTransaction, err := dbTransactionManager.BeginTransaction(); err != nil {
			sink.PushError(err)
		} else {
			defer sink.Complete(&count)
			pq := make(url.Values)
			if e := softParseQuery(pq, q.RawQuery); e != nil {
				sink.PushError(e)
				dbTransactionManager.RollbackTransaction(dbTransaction)
			} else {
				var depth = 2
				if i, e := strconv.Atoi(url.QueryEscape(pq.Get("depth"))); e == nil {
					depth = i
				}

				var omitOuters = false
				if len(pq.Get("omit_outers")) > 0 {
					omitOuters = true
				}

				count, e = dataProcessor.GetBulk(dbTransaction, p.ByName("name"), pq.Get("q"), depth, omitOuters, func(obj map[string]interface{}) error { return sink.PourOff(obj) })
				if e != nil {
					sink.PushError(e)
					dbTransactionManager.RollbackTransaction(dbTransaction)
				} else {
					dbTransactionManager.CommitTransaction(dbTransaction)
				}
			}
		}
	}))

	app.router.DELETE(cs.root+"/data/single/:name/:key", CreateDualJsonAction(func(src *JsonSource, sink *JsonSink, p httprouter.Params, r *http.Request) {

		user := r.Context().Value("auth_user").(auth.User)
		if dbTransaction, err := dbTransactionManager.BeginTransaction(); err != nil {
			sink.pushError(err)
		} else {
			if removedData, e := dataProcessor.RemoveRecord(dbTransaction, p.ByName("name"), p.ByName("key"), user); e != nil {
				dbTransactionManager.RollbackTransaction(dbTransaction)
				sink.pushError(e)
			} else {
				dbTransactionManager.CommitTransaction(dbTransaction)
				sink.pushGeneric(removedData)
			}
		}
	}, true))

	app.router.DELETE(cs.root+"/data/bulk/:name", CreateDualJsonStreamAction(func(stream *JsonStream, sink *JsonSinkStream, p httprouter.Params, request *http.Request) {
		if dbTransaction, err := dbTransactionManager.BeginTransaction(); err != nil {
			sink.PushError(err)
		} else {
			defer sink.Complete(nil)
			user := request.Context().Value("auth_user").(auth.User)
			e := dataProcessor.BulkDeleteRecords(dbTransaction, p.ByName("name"), func() (map[string]interface{}, error) {
				if obj, eof, e := stream.Next(); e != nil {
					return nil, e
				} else if eof {
					return nil, nil
				} else {
					return obj, nil
				}
			}, user)
			if e != nil {
				dbTransactionManager.RollbackTransaction(dbTransaction)
				sink.PushError(e)
			} else {
				dbTransactionManager.CommitTransaction(dbTransaction)
			}
		}
	}))

	app.router.POST(cs.root+"/data/single/:name/:key", CreateDualJsonAction(func(src *JsonSource, sink *JsonSink, p httprouter.Params, r *http.Request) {
		if dbTransaction, err := dbTransactionManager.BeginTransaction(); err != nil {
			sink.pushError(err)
		} else {
			user := r.Context().Value("auth_user").(auth.User)
			objectName := p.ByName("name")
			recordPkValue := p.ByName("key")
			//TODO: building record data respecting "depth" argument should be implemented inside dataProcessor
			//also "FillRecordValues" also should be moved from Node struct
			if recordData, e := dataProcessor.UpdateRecord(dbTransaction, objectName, recordPkValue, src.Value, user); e != nil {
				if dt, ok := e.(*errors.DataError); ok && dt.Code == errors.ErrCasFailed {
					dbTransactionManager.RollbackTransaction(dbTransaction)
					sink.pushError(&ServerError{http.StatusPreconditionFailed, dt.Code, dt.Msg})
				} else {
					dbTransactionManager.RollbackTransaction(dbTransaction)
					sink.pushError(e)
				}
			} else {
				if recordData != nil {
					var depth = 1
					if i, e := strconv.Atoi(r.URL.Query().Get("depth")); e == nil {
						depth = i
					}
					if recordData, err := dataProcessor.Get(dbTransaction, objectName, recordPkValue, depth, false);
						err != nil {
						dbTransactionManager.RollbackTransaction(dbTransaction)
						sink.pushError(err)
					} else {
						dbTransactionManager.CommitTransaction(dbTransaction)
						sink.pushGeneric(recordData.Data)
					}

				} else {
					dbTransactionManager.RollbackTransaction(dbTransaction)
					sink.pushError(&ServerError{http.StatusNotFound, ErrNotFound, "record not found"})
				}
			}
		}
	}, false))

	app.router.POST(cs.root+"/data/bulk/:name", CreateDualJsonStreamAction(func(stream *JsonStream, sink *JsonSinkStream, p httprouter.Params, request *http.Request) {
		if dbTransaction, err := dbTransactionManager.BeginTransaction(); err != nil {
			sink.PushError(err)
		} else {
			defer sink.Complete(nil)
			user := request.Context().Value("auth_user").(auth.User)
			e := dataProcessor.BulkUpdateRecords(dbTransaction, p.ByName("name"), func() (map[string]interface{}, error) {
				if obj, eof, e := stream.Next(); e != nil {
					return nil, e
				} else if eof {
					return nil, nil
				} else {
					return obj, nil
				}
			}, func(obj map[string]interface{}) error { return sink.PourOff(obj) }, user)
			if e != nil {
				if dt, ok := e.(*errors.DataError); ok && dt.Code == errors.ErrCasFailed {
					dbTransactionManager.RollbackTransaction(dbTransaction)
					sink.PushError(&ServerError{http.StatusPreconditionFailed, dt.Code, dt.Msg})
				} else {
					dbTransactionManager.RollbackTransaction(dbTransaction)
					sink.PushError(e)
				}
			} else {
				dbTransactionManager.CommitTransaction(dbTransaction)
			}
		}
	}))

	if enableProfiler {
		app.router.Handler(http.MethodGet, "/debug/pprof/:item", http.DefaultServeMux)
	}

	app.router.PanicHandler = func(w http.ResponseWriter, r *http.Request, err interface{}) {
		user := r.Context().Value("auth_user").(auth.User)
		raven.SetUserContext(&raven.User{ID: strconv.Itoa(user.Id), Username: user.Login})
		raven.SetHttpContext(raven.NewHttp(r))
		if err, ok := err.(error); ok {
			raven.CaptureErrorAndWait(err, nil)
			raven.ClearContext()
		}

	}

	cs.s = &http.Server{
		Addr:           cs.addr + ":" + cs.port,
		Handler:        app,
		ReadTimeout:    60 * time.Second,
		WriteTimeout:   60 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	return cs.s
}

//Creates an action to process an HTTP request in JSON format.
//It takes an function to process request, which accepts JsonSource, JsonSink and PathSegments.
func CreateDualJsonAction(f func(*JsonSource, *JsonSink, httprouter.Params, *http.Request), allowEmptyBody bool) func(http.ResponseWriter, *http.Request, httprouter.Params) {
	return func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		src, e := (*httpRequest)(r).asJsonSource()
		if e != nil && !allowEmptyBody {
			returnError(w, e)
			return
		}
		sink, _ := asJsonSink(w)
		f(src, sink, p, r)
	}
}

func CreateDualJsonStreamAction(callbackFunction func(*JsonStream, *JsonSinkStream, httprouter.Params, *http.Request)) func(http.ResponseWriter, *http.Request, httprouter.Params) {
	return func(w http.ResponseWriter, request *http.Request, p httprouter.Params) {
		stream, e := (*httpRequest)(request).asJsonStream()
		if e != nil {
			returnError(w, e)
			return
		}
		sink, _ := AsJsonSinkStream(w)
		callbackFunction(stream, sink, p, request)
	}
}

func CreateJsonAction(f func(io.ReadCloser, *JsonSink, httprouter.Params, url.Values)) func(http.ResponseWriter, *http.Request, httprouter.Params) {
	return func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		sink, _ := asJsonSink(w)
		f(r.Body, sink, p, r.URL.Query())
	}
}

func CreateJsonStreamAction(f func(*JsonSinkStream, httprouter.Params, *url.URL)) func(http.ResponseWriter, *http.Request, httprouter.Params) {
	return func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		sink, _ := AsJsonSinkStream(w)
		f(sink, p, r.URL)
	}
}

//Returns an error to HTTP response in JSON format.
//If the error object accepted is of ServerError type so HTTP status and code are taken from the error object.
//If the error corresponds to JsonError interface so HTTP status set to http.StatusBadRequest and code taken from the error object.
//Otherwise they sets to http.StatusInternalServerError and ErrInternalServerError respectively.
func returnError(w http.ResponseWriter, e error) {
	w.Header().Set("Content-Type", "application/json")
	responseData := map[string]interface{}{"status": "FAIL"}
	switch e := e.(type) {
	case *ServerError:
		responseData["error"] = e.Serialize()
		w.WriteHeader(e.Status)
	case *auth.AuthError:
		w.WriteHeader(http.StatusForbidden)
		responseData["error"] = e.Serialize()
	case JsonError:
		w.WriteHeader(http.StatusBadRequest)
		responseData["error"] = e.Serialize()
	default:
		w.WriteHeader(http.StatusInternalServerError)
		err := ServerError{Status: http.StatusInternalServerError, Code: ErrInternalServerError, Msg: e.Error()}
		responseData["error"] = err.Serialize()
	}
	//encoded
	encodedData, _ := json.Marshal(responseData)
	w.Write(encodedData)
}

//The source of JSON object. It contains a value of type map[string]interface{}.
type JsonSource struct {
	Value map[string]interface{}
}

type JsonStream struct {
	stream *json.Decoder
}

func (js *JsonStream) Next() (map[string]interface{}, bool, error) {
	if js.stream.More() {
		var v interface{}
		err := js.stream.Decode(&v)
		if err != nil {
			return nil, false, &ServerError{http.StatusBadRequest, ErrBadRequest, "bad JSON"}
		}

		jobj, ok := v.(map[string]interface{})
		if !ok {
			return nil, false, &ServerError{http.StatusBadRequest, ErrBadRequest, "expected JSON object"}
		}
		return jobj, false, err
	} else {
		_, err := js.stream.Token()
		return nil, true, err
	}

}

type httpRequest http.Request

//Converts an HTTP request to the JsonSource if the request is valid and contains a valid JSON object in its body.
func (r *httpRequest) asJsonSource() (*JsonSource, error) {
	var smime = r.Header.Get(textproto.CanonicalMIMEHeaderKey("Content-Type"))
	if smime == "" {
		return nil, &ServerError{http.StatusUnsupportedMediaType, ErrUnsupportedMediaType, "content type not found"}
	}
	mm, _, e := mime.ParseMediaType(smime)
	if e != nil {
		return nil, &ServerError{http.StatusBadRequest, ErrBadRequest, e.Error()}
	}
	if mm != "application/json" {
		return nil, &ServerError{http.StatusUnsupportedMediaType, ErrUnsupportedMediaType, "mime type is not of 'application/json'"}
	}
	var body = r.Body
	if body == nil {
		return nil, &ServerError{http.StatusBadRequest, ErrBadRequest, "no body"}
	}
	var v interface{}
	if e := json.NewDecoder(body).Decode(&v); e != nil {
		return nil, &ServerError{http.StatusBadRequest, ErrBadRequest, "bad JSON"}
	}
	jobj, ok := v.(map[string]interface{})
	if !ok {
		return nil, &ServerError{http.StatusBadRequest, ErrBadRequest, "expected JSON object"}
	}
	return &JsonSource{jobj}, nil
}

func (r *httpRequest) asJsonStream() (*JsonStream, error) {
	var smime = r.Header.Get(textproto.CanonicalMIMEHeaderKey("Content-Type"))
	if smime == "" {
		return nil, &ServerError{http.StatusUnsupportedMediaType, ErrUnsupportedMediaType, "content type not found"}
	}
	mm, _, e := mime.ParseMediaType(smime)
	if e != nil {
		return nil, &ServerError{http.StatusBadRequest, ErrBadRequest, e.Error()}
	}
	if mm != "application/json" {
		return nil, &ServerError{http.StatusUnsupportedMediaType, ErrUnsupportedMediaType, "mime type is not of 'application/json'"}
	}
	var body = r.Body
	if body == nil {
		return nil, &ServerError{http.StatusBadRequest, ErrBadRequest, "no body"}
	}

	var js = json.NewDecoder(body)
	if _, err := js.Token(); err != nil {
		return nil, &ServerError{http.StatusBadRequest, ErrBadRequest, "bad JSON"}
	}
	return &JsonStream{js}, nil
}

//The JSON object sink into the HTTP response.
type JsonSink struct {
	rw http.ResponseWriter
}

//Converts http.ResponseWriter into JsonSink.
func asJsonSink(w http.ResponseWriter) (*JsonSink, error) {
	return &JsonSink{w}, nil
}

//Push an error into JsonSink.
func (js *JsonSink) pushError(e error) {
	returnError(js.rw, e)
}

//Push an JSON object into JsonSink
func (js *JsonSink) pushGeneric(obj map[string]interface{}) {
	responseData := map[string]interface{}{"status": "OK"}
	if obj != nil {
		responseData["data"] = obj
	}
	if encodedData, err := json.Marshal(responseData); err != nil {
		returnError(js.rw, err)
	} else {
		js.rw.Header().Set("Content-Type", "application/json")
		js.rw.WriteHeader(http.StatusOK)
		js.rw.Write(encodedData)
	}
}

func (js *JsonSink) push(i interface{}) {
	if j, e := json.Marshal(i); e != nil {
		returnError(js.rw, e)
	} else {
		js.rw.Header().Set("Content-Type", "application/json")
		js.rw.WriteHeader(http.StatusOK)
		js.rw.Write(j)
	}
}

//Push an emptiness into JsonSink.
func (js *JsonSink) pushEmpty() {
	js.rw.WriteHeader(http.StatusNoContent)
}
