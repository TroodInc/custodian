package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/getsentry/raven-go"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"logger"
	"mime"
	"net/http"
	_ "net/http/pprof"
	"net/textproto"
	"net/url"
	"os"
	"server/abac"
	"server/auth"
	"server/data"
	"server/data/errors"
	"server/data/record"
	. "server/errors"
	"server/migrations"
	"server/migrations/constructor"
	migrations_description "server/migrations/description"
	"server/object/description"
	"server/object/meta"
	"server/pg"
	"server/pg/migrations/managers"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"server/transactions/file_transaction"
	"strconv"
	"strings"
	"time"
	"utils"
)

type CustodianApp struct {
	router        *httprouter.Router
	authenticator auth.Authenticator
}

func GetApp(cs *CustodianServer) *CustodianApp {
	return &CustodianApp{
		router:        httprouter.New(),
		authenticator: cs.authenticator,
	}
}

func (app *CustodianApp) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	if user, abac_data, err := app.authenticator.Authenticate(req); err == nil {
		ctx := context.WithValue(req.Context(), "auth_user", *user)

		handler, opts, _ := app.router.Lookup(req.Method, req.URL.Path)

		if handler != nil {
			var res= strings.Split(opts.ByName("name"), "?")[0]
			splited := strings.Split(req.URL.Path, "/")
			action := ""
			if res != "" {
				if splited[2] == "meta" {
					action = "meta_"
				} else if splited[2] == "data" {
					action = "data_"
				}
			} else {
				if splited[2] == "meta" {
					res = "meta"
				} else {
					res = "*"
				}
			}

			var abac_tree = map[string]interface{}{"_default_resolution": "allow"}
			if tree, ok := abac_data[os.Getenv("SERVICE_DOMAIN")]; ok {
				abac_tree = tree.(map[string]interface{})
			}

			abac_default_resolution := "allow"
			if abac_tree != nil {
				if domain_default_resolution, ok := abac_tree["_default_resolution"]; ok {
					abac_default_resolution = domain_default_resolution.(string)
				} else if abac_global_resolution, ok := abac_data["_default_resolution"]; ok {
					abac_default_resolution = abac_global_resolution.(string)
				}
			}

			abac_resolver := abac.GetTroodABAC(
				map[string]interface{}{
					"sbj": *user,
				},
				abac_tree,
				abac_default_resolution,
			)
			ctx = context.WithValue(ctx, "resource", res)
			ctx = context.WithValue(ctx, "action", action + req.Method)
			ctx = context.WithValue(ctx, "abac", abac_resolver)

		}

		app.router.ServeHTTP(w, req.WithContext(ctx))

	} else {
		returnError(w, err)
	}
}

//Custodian server description
type CustodianServer struct {
	addr, port, root string
	s                *http.Server
	db               string
	auth_url         string
	authenticator    auth.Authenticator
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

func (cs *CustodianServer) SetAuthenticator(authenticator auth.Authenticator) {
	cs.authenticator = authenticator
}

//TODO: "enableProfiler" option should be configured like other options
func (cs *CustodianServer) Setup(config *utils.AppConfig) *http.Server {
	if cs.authenticator == nil {
		cs.authenticator = auth.GetAuthenticator()
	}

	app := GetApp(cs)

	//MetaDescription routes
	syncer, err := pg.NewSyncer(config.DbConnectionUrl)
	dataManager, _ := syncer.NewDataManager()
	metaDescriptionSyncer := meta.NewFileMetaDescriptionSyncer("./")

	//transaction managers
	fileMetaTransactionManager := file_transaction.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer.Remove, metaDescriptionSyncer.Create)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	migrationDBDescriptionSyncer := pg.NewDbMetaDescriptionSyncer(dbTransactionManager)
	migrationStore := meta.NewStore(migrationDBDescriptionSyncer, syncer, globalTransactionManager)
	migrationManager := managers.NewMigrationManager(
		metaStore, migrationStore, dataManager, globalTransactionManager,
	)

	getDataProcessor := func () *data.Processor {
		dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
		processor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)
		return processor
	}

	if err != nil {
		logger.Error("Failed to create syncer: %s", err.Error())
		panic(err)
	}

	app.router.ServeFiles("/static/*filepath", http.Dir("/home/static"))
	app.router.GET(cs.root+"/swagger", func(w http.ResponseWriter, req *http.Request, p httprouter.Params){
		http.ServeFile(w, req, "/home/static/swagger_ui.html")
	})

	//object operations
	app.router.GET(cs.root+"/meta", CreateJsonAction(func(src *JsonSource, js *JsonSink, _ httprouter.Params, q url.Values, request *http.Request) {
		if metaList, _, err := metaStore.List(); err == nil {
			var result []interface{}
			for _, val := range(metaList) {
				result = append(result, *val)
			}
			js.pushList(result, len(result))
		} else {
			js.pushError(err)
		}
	}))

	app.router.GET(cs.root+"/meta/:name", CreateJsonAction(func(_ *JsonSource, js *JsonSink, p httprouter.Params, q url.Values, request *http.Request) {
		//set transaction to the context
		if metaObj, _, e := metaStore.Get(p.ByName("name"), true); e == nil {
			js.pushObj(metaObj.ForExport())
		} else {
			js.pushError(e)
		}
	}))

	app.router.POST(cs.root+"/meta", CreateJsonAction(func(r *JsonSource, js *JsonSink, _ httprouter.Params, q url.Values, request *http.Request) {
		metaDescriptionList, _, _ := metaStore.List()
		if globalTransaction, err := globalTransactionManager.BeginTransaction(metaDescriptionList); err != nil {
			js.pushError(err)
		} else {
			//set transaction to the context
			*request = *request.WithContext(context.WithValue(request.Context(), "db_transaction", globalTransaction))

			metaObj, err := metaStore.UnmarshalIncomingJSON(bytes.NewReader(r.body))
			if err != nil {
				js.pushError(err)
				globalTransactionManager.RollbackTransaction(globalTransaction)
				return
			}
			if e := metaStore.Create(metaObj); e == nil {
				globalTransactionManager.CommitTransaction(globalTransaction)
				js.pushObj(metaObj.ForExport())
			} else {
				globalTransactionManager.RollbackTransaction(globalTransaction)
				js.pushError(e)
			}
		}
	}))

	app.router.DELETE(cs.root+"/meta/:name", CreateJsonAction(func(_ *JsonSource, js *JsonSink, p httprouter.Params, q url.Values, request *http.Request) {
		if ok, e := metaStore.Remove(p.ByName("name"), false); ok {
			js.pushObj(nil)
		} else {
			if e != nil {

				js.pushError(e)
			} else {

				js.pushError(&ServerError{Status: http.StatusNotFound, Code: ErrNotFound})
			}
		}
	}))

	app.router.PATCH(cs.root+"/meta/:name", CreateJsonAction(func(r *JsonSource, js *JsonSink, p httprouter.Params, q url.Values, request *http.Request) {
		metaObj, err := metaStore.UnmarshalIncomingJSON(bytes.NewReader(r.body))
		if err != nil {
			js.pushError(err)
			return
		}
		if _, err := metaStore.Update(p.ByName("name"), metaObj, true); err == nil {
			js.pushObj(metaObj.ForExport())
		} else {
			js.pushError(err)
		}

	}))

	//RecordSetOperations operations
	app.router.POST(cs.root+"/data/:name", CreateJsonAction(func(src *JsonSource, sink *JsonSink, p httprouter.Params, q url.Values, r *http.Request) {
		dataProcessor := getDataProcessor()
		user := r.Context().Value("auth_user").(auth.User)
		objectName := p.ByName("name")

		abac_resolver := r.Context().Value("abac").(abac.TroodABAC)
		_, rule := abac_resolver.Check(objectName, "data_POST")

		if src.single != nil {
			if rule != nil {
				restricted := abac.CheckMask(src.single, rule.Mask)

				if len(restricted) > 0 {
					sink.pushError(
						abac.NewError(
							fmt.Sprintf("Creating object with fields [%s] restricted by ABAC rule", strings.Join(restricted, ",")),
						),
					)
					return
				}
			}

			if record, err := dataProcessor.CreateRecord(objectName, src.single, user); err != nil {
				sink.pushError(err)
			} else {
				var depth = 1
				if i, e := strconv.Atoi(r.URL.Query().Get("depth")); e == nil {
					depth = i
				}

				pkValue, _ := record.Meta.Key.ValueAsString(record.Data[record.Meta.Key.Name])
				if record, err := dataProcessor.Get(objectName, pkValue, r.URL.Query()["only"], r.URL.Query()["exclude"], depth, false);
					err != nil {
					sink.pushError(err)
				} else {
					sink.pushObj(record.GetData())
				}
			}

		} else if src.list != nil {

			var i = 0
			records, e := dataProcessor.BulkCreateRecords(p.ByName("name"), func() (map[string]interface{}, error) {
				if i < len(src.list) {
					i += 1
					return src.list[i-1], nil
				} else {
					return nil, nil
				}

			}, user)

			if e != nil {
				sink.pushError(e)
			} else {
				result := make([]interface{}, 0)
				for _, obj := range records {
					result = append(result, obj.GetData())
				}
				sink.pushList(result, len(result))
			}
		}

	}))

	app.router.GET(cs.root+"/data/:name/:key", CreateJsonAction(func(r *JsonSource, sink *JsonSink, p httprouter.Params, q url.Values, request *http.Request) {
		dataProcessor := getDataProcessor()

		var depth = 2
		if i, e := strconv.Atoi(q.Get("depth")); e == nil {
			depth = i
		}

		var omitOuters = false
		if len(q.Get("omit_outers")) > 0 {
			omitOuters = true
		}

		if o, e := dataProcessor.Get(p.ByName("name"), p.ByName("key"), q["only"], q["exclude"], depth, omitOuters); e != nil {
			sink.pushError(e)
		} else {
			if o == nil {
				sink.pushError(&ServerError{http.StatusNotFound, ErrNotFound, "record not found", nil})
			} else {
				abac_resolver := request.Context().Value("abac").(abac.TroodABAC)
				pass, result := abac_resolver.MaskRecord(o, "data_GET")
				if !pass {
					sink.pushError(abac.NewError("Permission denied"))
					return
				}

				sink.pushObj(result.(*record.Record).GetData())
			}
		}

	}))

	app.router.GET(cs.root+"/data/:name", CreateJsonAction(func(_ *JsonSource, sink *JsonSink, p httprouter.Params, q url.Values, request *http.Request) {
		dataProcessor := getDataProcessor()
		abac_resolver := request.Context().Value("abac").(abac.TroodABAC)
		var depth = 2
		if i, e := strconv.Atoi(url.QueryEscape(q.Get("depth"))); e == nil {
			depth = i
		}

		var omitOuters = false
		if len(q.Get("omit_outers")) > 0 {
			omitOuters = true
		}

		_, rule := abac_resolver.Check(p.ByName("name"), "data_GET") // ??
		user_filters := q.Get("q")

		var filters []string
		if rule != nil && rule.Filter != nil {
			if rule.Result == "deny" {
				filters = append(filters, rule.Filter.Invert().String())
			} else {
				filters = append(filters, rule.Filter.String())
			}
		}

		if user_filters != "" {
			filters = append(filters, user_filters)
		}

		result := make([]interface{}, 0)
		count, records, e := dataProcessor.GetBulk(
			p.ByName("name"), strings.Join(filters, ","), q["only"], q["exclude"], depth, omitOuters,
		)

		if e != nil {
			sink.pushError(e)
		} else {
			for _, obj := range records {
				pass, rec := abac_resolver.MaskRecord(obj, "data_GET")
				if pass {
					result = append(result, rec.(*record.Record).GetData())
				}
			}

			sink.pushList(result, count)
		}
	}))

	app.router.DELETE(cs.root+"/data/:name/:key", CreateJsonAction(func(src *JsonSource, sink *JsonSink, p httprouter.Params, q url.Values, r *http.Request) {
		dataProcessor := getDataProcessor()
		user := r.Context().Value("auth_user").(auth.User)

		objectName := p.ByName("name")
		recordPkValue := p.ByName("key")
		//set transaction to the context


		//process access check
		recordToUpdate, err := dataProcessor.Get(objectName, recordPkValue, r.URL.Query()["only"], r.URL.Query()["exclude"], 1, true)
		if err != nil || recordToUpdate == nil {
			sink.pushError(&ServerError{http.StatusNotFound, ErrNotFound, "record not found", nil})
		} else {
			abac_resolver := r.Context().Value("abac").(abac.TroodABAC)
			pass, _ := abac_resolver.CheckRecord(recordToUpdate, "data_DELETE")
			if !pass {
				sink.pushError(abac.NewError("Permission denied"))

				return
			}
			//end access check

			if removedData, e := dataProcessor.RemoveRecord(objectName, recordPkValue, user); e != nil {

				sink.pushError(e)
			} else {

				sink.pushObj(removedData)
			}
		}
	}))

	app.router.DELETE(cs.root+"/data/:name", CreateJsonAction(func(src *JsonSource, sink *JsonSink, p httprouter.Params,  q url.Values, request *http.Request) {
		dataProcessor := getDataProcessor()

		user := request.Context().Value("auth_user").(auth.User)
		var i = 0
		e := dataProcessor.BulkDeleteRecords(p.ByName("name"), func() (map[string]interface{}, error) {
			if i < len(src.list) {
				i += 1
				return src.list[i-1], nil
			} else {
				return nil, nil
			}
		}, user)
		if e != nil {

			defer sink.pushError(e)
		} else {
			defer sink.pushObj(nil)

		}

	}))

	app.router.PATCH(cs.root+"/data/:name/:key", CreateJsonAction(func(src *JsonSource, sink *JsonSink, p httprouter.Params, u url.Values, r *http.Request) {
		dataProcessor := getDataProcessor()
		user := r.Context().Value("auth_user").(auth.User)
		objectName := p.ByName("name")
		recordPkValue := p.ByName("key")

		//process access check
		recordToUpdate, err := dataProcessor.Get(objectName, recordPkValue, r.URL.Query()["only"], r.URL.Query()["exclude"], 1, true)
		if err != nil {
			sink.pushError(&ServerError{http.StatusNotFound, ErrNotFound, "record not found", nil})
		} else {
			abac_resolver := r.Context().Value("abac").(abac.TroodABAC)
			pass, rule := abac_resolver.CheckRecord(recordToUpdate, "data_PATCH")
			if !pass {
				sink.pushError(abac.NewError("Permission denied"))
				return
			}

			if rule != nil {
				restricted := abac.CheckMask(src.single, rule.Mask)

				if len(restricted) > 0 {
					sink.pushError(
						abac.NewError(
							fmt.Sprintf("Updating fields [%s] restricted by ABAC rule", strings.Join(restricted, ",")),
						),
					)
					return
				}
			}
		}

		//end access check

		//TODO: building record data respecting "depth" argument should be implemented inside dataProcessor
		//also "FillRecordValues" also should be moved from Node struct

		if updatedRecord, e := dataProcessor.UpdateRecord(objectName, recordPkValue, src.single, user); e != nil {
			if dt, ok := e.(*errors.DataError); ok && dt.Code == errors.ErrCasFailed {
				sink.pushError(&ServerError{http.StatusPreconditionFailed, dt.Code, dt.Msg, nil})
			} else {
				sink.pushError(e)
			}
		} else {
			if updatedRecord != nil {
				var depth = 1
				if i, e := strconv.Atoi(r.URL.Query().Get("depth")); e == nil {
					depth = i
				}
				if recordData, err := dataProcessor.Get(objectName, recordPkValue, r.URL.Query()["only"], r.URL.Query()["exclude"], depth, false);
					err != nil {
					sink.pushError(err)
				} else {
					sink.pushObj(recordData.GetData())
				}
			} else {
				sink.pushError(&ServerError{http.StatusNotFound, ErrNotFound, "record not found", nil})
			}
		}

	}))

	app.router.PATCH(cs.root+"/data/:name", CreateJsonAction(func(src *JsonSource, sink *JsonSink, p httprouter.Params,  q url.Values, request *http.Request) {
		dataProcessor := getDataProcessor()
		if dbTransaction, err := dbTransactionManager.BeginTransaction(); err != nil {
			sink.pushError(err)
		} else {
			//set transaction to the context
			*request = *request.WithContext(context.WithValue(request.Context(), "db_transaction", dbTransaction))

			user := request.Context().Value("auth_user").(auth.User)

			var i = 0
			var result []interface{}
			e := dataProcessor.BulkUpdateRecords(dbTransaction, p.ByName("name"), func() (map[string]interface{}, error) {
				if i < len(src.list) {
					i += 1
					return src.list[i-1], nil
				} else {
					return nil, nil
				}
			}, func(obj map[string]interface{}) error { result = append(result, obj); return nil  }, user)
			if e != nil {
				if dt, ok := e.(*errors.DataError); ok && dt.Code == errors.ErrCasFailed {
					dbTransactionManager.RollbackTransaction(dbTransaction)
					sink.pushError(&ServerError{http.StatusPreconditionFailed, dt.Code, dt.Msg, nil})
				} else {
					dbTransactionManager.RollbackTransaction(dbTransaction)
					sink.pushError(e)
				}
			} else {
				dbTransactionManager.CommitTransaction(dbTransaction)
				defer sink.pushList(result, len(result))
			}
		}
	}))

	app.router.POST(cs.root+"/migrations/construct", CreateJsonAction(func(r *JsonSource, js *JsonSink, p httprouter.Params, q url.Values, request *http.Request) {
		if globalTransaction, err := globalTransactionManager.BeginTransaction(make([]*description.MetaDescription, 0)); err != nil {
			globalTransactionManager.RollbackTransaction(globalTransaction)
			js.pushError(err)
			return
		} else {
			migrationMetaDescription, err := migrations_description.MigrationMetaDescriptionFromJson(bytes.NewReader(r.body))
			if err != nil {
				globalTransactionManager.RollbackTransaction(globalTransaction)
				js.pushError(err)
				return
			}

			migrationConstructor := constructor.NewMigrationConstructor(migrationManager)

			var currentMetaDescription *description.MetaDescription
			if len(migrationMetaDescription.PreviousName) != 0 {
				currentMetaDescription, _, err = metaDescriptionSyncer.Get(migrationMetaDescription.PreviousName)
				if err != nil {
					js.pushError(err)
					return
				}
			}
			//migration constructor expects migrationMetaDescription to be nil if object is being deleted
			//in its turn, object is supposed to be deleted if migrationMetaDescription.name is an empty string
			if migrationMetaDescription.Name == "" {
				migrationMetaDescription = nil
			}

			migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, migrationMetaDescription, globalTransaction.DbTransaction)
			if err != nil {
				js.pushError(err)
				return
			}

			err = globalTransactionManager.CommitTransaction(globalTransaction)
			if err != nil {
				js.pushError(err)
				return
			} else {
				js.pushObj(migrationDescription)
				return
			}
		}
	}))

	app.router.POST(cs.root+"/migrations/apply", CreateJsonAction(func(r *JsonSource, js *JsonSink, p httprouter.Params, q url.Values, request *http.Request) {
		migrationDescription, err := migrations_description.MigrationDescriptionFromJson(bytes.NewReader(r.body))
		if err != nil {
			js.pushError(err)
			return
		}

		fake := len(q.Get("fake")) > 0

		updatedMetaDescription, err := migrationManager.Apply(migrationDescription, !fake, fake)

		metaStore.Cache().Invalidate()

		if updatedMetaDescription != nil {
			js.pushObj(updatedMetaDescription.ForExport())
		} else {
			js.pushError(err)
		}
	}))

	app.router.GET(cs.root+"/migrations", CreateJsonAction(func(_ *JsonSource, sink *JsonSink, p httprouter.Params, q url.Values, request *http.Request) {
		migrationList, err := migrationManager.List(q.Get("q"))
		if err != nil {
			sink.pushError(err)
			return
		} else {
			result := make([]interface{}, 0)
			for _, obj := range migrationList {
				data := obj.GetData()
				var meta_state map[string]interface{}
				var operations map[string]interface{}
				json.Unmarshal([]byte(fmt.Sprintf("%v", data["meta_state"])), &meta_state)
				json.Unmarshal([]byte(fmt.Sprintf("%v", data["operations"])), &operations)
				data["meta_state"] = meta_state
				data["operations"] = operations

				result = append(result, data)
			}
			sink.pushList(result, len(result))
		}
	}))

	app.router.GET(cs.root+"/migrations/description/:migration_id", CreateJsonAction(func(r *JsonSource, sink *JsonSink, p httprouter.Params, q url.Values, request *http.Request) {
		migration, err := migrationManager.Get(p.ByName("migration_id"))
		if err != nil {
			sink.pushError(err)
			return
		} else if migration == nil {
			sink.pushError(&ServerError{http.StatusNotFound, ErrNotFound, "record not found", nil})
		} else {
			sink.pushObj(migration.GetData())
		}
	}))

	app.router.POST(cs.root+"/migrations/rollback", CreateJsonAction(func(requestData *JsonSource, sink *JsonSink, p httprouter.Params, q url.Values, request *http.Request) {

		fake := len(q.Get("fake")) > 0

		migrationId, ok := requestData.single["migrationId"]
		if !ok {
			sink.pushError(NewValidationError(migrations.MigrationErrorInvalidDescription, "Migration`s ID should be specified with 'migrationId' attribute", nil))
			return
		}
		metaDescription, err := migrationManager.RollBackTo(migrationId.(string), !fake, fake)

		if err != nil {
			sink.pushError(err)
			return
		} else {
			sink.pushObj(metaDescription)
			return
		}

	}))

	if config.EnableProfiler {
		app.router.Handler(http.MethodGet, "/debug/pprof/:item", http.DefaultServeMux)
	}

	if !config.DisableSafePanicHandler {
		app.router.PanicHandler = func(w http.ResponseWriter, r *http.Request, err interface{}) {
			user := r.Context().Value("auth_user").(auth.User)
			raven.SetUserContext(&raven.User{ID: strconv.Itoa(user.Id), Username: user.Login})
			raven.SetHttpContext(raven.NewHttp(r))
			if err, ok := err.(error); ok {
				raven.CaptureErrorAndWait(err, nil)
				raven.ClearContext()

				//rollback set transactions
				if dbTransaction := r.Context().Value("db_transaction"); dbTransaction != nil {
					dbTransactionManager.RollbackTransaction(dbTransaction.(transactions.DbTransaction))
				}

				if globalTransaction := r.Context().Value("global_transaction"); globalTransaction != nil {
					globalTransactionManager.RollbackTransaction(globalTransaction.(*transactions.GlobalTransaction))
				}
				//

				returnError(w, err.(error))
			}
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

func CreateJsonAction(f func(*JsonSource, *JsonSink, httprouter.Params, url.Values, *http.Request)) func(http.ResponseWriter, *http.Request, httprouter.Params) {
	return func(w http.ResponseWriter, req *http.Request, p httprouter.Params) {
		sink, _ := asJsonSink(w)
		src, e := (*httpRequest)(req).asJsonSource()

		if e != nil {
			returnError(w, e)
			return
		}

		ctx := req.Context()

		var resolver_context = make(map[string]interface{})

		if src != nil {
			resolver_context["data"] = src.GetData()
		}

		var params = make(map[string]interface{})
		for _, param := range p {
			params[param.Key] = param.Value
		}
		resolver_context["params"] = params
		resolver_context["query"] = req.URL.Query()

		abac_resolver := ctx.Value("abac").(abac.TroodABAC)
		abac_resolver.DataSource["ctx"] = resolver_context

		passed, rule := abac_resolver.Check(
			ctx.Value("resource").(string), ctx.Value("action").(string),
		)

		if rule != nil {
			if !passed && rule.Result == "allow" {
				returnError(w, abac.NewError("Access restricted by ABAC access rule"))
				return
			}

			if rule.Result == "deny" {
				sink.Status = "RESTRICTED"
			}
		}

		query  := make(url.Values)
		err := parseQuery(query, req.URL.RawQuery)

		if err != nil {
			returnError(w, err)
			return
		}

		f(src, sink, p, query, req.WithContext(ctx))

	}
}

func parseQuery(m  url.Values, query string) (err error) {

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

//Returns an error to HTTP response in JSON format.
//If the error object accepted is of ServerError type so HTTP status and code are taken from the error object.
//If the error corresponds to JsonError interface so HTTP status set to http.StatusBadRequest and code taken from the error object.
//Otherwise they sets to http.StatusInternalServerError and ErrInternalServerError respectively.
func returnError(w http.ResponseWriter, e interface{}) {
	w.Header().Set("Content-Type", "application/json")
	responseData := map[string]interface{}{"status": "FAIL"}
	switch e := e.(type) {
	case *auth.AuthError:
		w.WriteHeader(http.StatusUnauthorized)
		responseData["error"] = e.Serialize()
	case *abac.AccessError:
		w.WriteHeader(http.StatusForbidden)
		responseData["error"] = e.Serialize()
	case *ServerError:
		w.WriteHeader(e.Status)
		responseData["error"] = e.Serialize()
	default:
		w.WriteHeader(http.StatusInternalServerError)
		responseData["error"] = e.(error).Error()
	}
	//encoded
	encodedData, _ := json.Marshal(responseData)
	w.Write(encodedData)
}

//The source of JSON object. It contains a value of type map[string]interface{}.
type JsonSource struct {
	body []byte
	single map[string]interface{}
	list []map[string]interface{}
}

type httpRequest http.Request

func (js *JsonSource) GetData() interface{} {
	if js.list != nil && len(js.list) > 0 {
		return js.list
	} else {
		return js.single
	}
}

//Converts an HTTP request to the JsonSource if the request is valid and contains a valid JSON object in its body.
func (r *httpRequest) asJsonSource() (*JsonSource, error) {
	if r.Body != nil {
		smime := r.Header.Get(textproto.CanonicalMIMEHeaderKey("Content-Type"));

		if mm, _, e := mime.ParseMediaType(smime); e == nil && mm == "application/json" {
			var result JsonSource
			result.body, _ = ioutil.ReadAll(r.Body)

			if len(result.body) > 0 {
				if e := json.Unmarshal(result.body, &result.single); e != nil {
					if e = json.Unmarshal(result.body, &result.list); e != nil {
						return nil, &ServerError{http.StatusBadRequest, ErrBadRequest, "bad JSON", e.Error()}
					}
				}
			}
			return &result, nil
		}
	}

	return nil, nil
}

//The JSON object sink into the HTTP response.
type JsonSink struct {
	rw     http.ResponseWriter
	Status string
}

//Converts http.ResponseWriter into JsonSink.
func asJsonSink(w http.ResponseWriter) (*JsonSink, error) {
	return &JsonSink{w, "OK"}, nil
}

//Push an error into JsonSink.
func (js *JsonSink) pushError(e error) {
	returnError(js.rw, e)
}

//Push an JSON object into JsonSink
func (js *JsonSink) pushObj(object interface{}) {
	responseData := map[string]interface{}{"status": js.Status}
	if object != nil {
		responseData["data"] = object
	}
	if encodedData, err := json.Marshal(responseData); err != nil {
		returnError(js.rw, err)
	} else {
		js.rw.Header().Set("Content-Type", "application/json")
		js.rw.WriteHeader(http.StatusOK)
		js.rw.Write(encodedData)
	}
}

func (js *JsonSink) pushList(objects []interface{}, total int) {
	responseData := map[string]interface{}{"status": js.Status}
	if objects == nil {
		objects = make([]interface{},0)
	}
	responseData["data"] = objects
	responseData["total_count"] = total

	if encodedData, err := json.Marshal(responseData); err != nil {
		returnError(js.rw, err)
	} else {
		js.rw.Header().Set("Content-Type", "application/json")
		js.rw.WriteHeader(http.StatusOK)
		js.rw.Write(encodedData)
	}
}
