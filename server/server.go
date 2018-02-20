package server

import (
	"encoding/json"
	"fmt"
	"github.com/Q-CIS-DEV/custodian/logger"
	"github.com/Q-CIS-DEV/custodian/server/data"
	"github.com/Q-CIS-DEV/custodian/server/meta"
	"github.com/Q-CIS-DEV/custodian/server/pg"
	"github.com/julienschmidt/httprouter"
	"io"
	"mime"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"time"
)

//Server errors description
const (
	ErrUnsupportedMediaType = "unsupported_media_type"
	ErrBadRequest           = "bad_request"
	ErrInternalServerError  = "internal_server_error"
	ErrNotFound             = "not_found"
)

//The interface of error convertable to JSON in format {"code":"some_code"; "msg":"message"}.
type JsonError interface {
	Json() []byte
}

type ServerError struct {
	status int
	code   string
	msg    string
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("Server error: status = %d, code = '%s', msg = '%s'", e.status, e.code, e.msg)
}

func marshalError(code, msg string) []byte {
	j, _ := json.Marshal(map[string]string{
		"code": code,
		"msg":  msg,
	})
	return j
}

func (e *ServerError) Json() []byte {
	return marshalError(e.code, e.msg)
}

func NewServerError(status int, code string, msg string, a ...interface{}) *ServerError {
	return &ServerError{status: status, code: code, msg: fmt.Sprintf(msg, a...)}
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
}

func New(a, p, r, db string) *CustodianServer {
	return &CustodianServer{addr: a, port: p, root: r}
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

func (cs *CustodianServer) Run() {

	router := httprouter.New()

	//Meta routes
	syncer, err := pg.NewSyncer(cs.db)
	if err != nil {
		logger.Error("Failed to create syncer: %s", err.Error())
		panic(err)
	}

	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

	router.GET(cs.root+"/meta", CreateJsonAction(func(r io.ReadCloser, js *JsonSink, _ httprouter.Params, q url.Values) {
		if metaList, _, err := metaStore.List(); err == nil {
			js.push(map[string]interface{}{"status": "OK", "data": metaList})
		} else {
			js.pushError(err)
		}
	}))
	router.GET(cs.root+"/meta/:name", CreateJsonAction(func(_ io.ReadCloser, js *JsonSink, p httprouter.Params, q url.Values) {
		if metaObj, ok, e := metaStore.Get(p.ByName("name")); e == nil {
			js.push(map[string]interface{}{"status": "OK", "data": metaObj})
		} else {
			if ok {
				js.pushError(e)
			} else {
				js.pushError(&ServerError{status: http.StatusNotFound, code: ErrNotFound})
			}
		}
	}))
	router.PUT(cs.root+"/meta", CreateJsonAction(func(r io.ReadCloser, js *JsonSink, _ httprouter.Params, q url.Values) {
		metaObj, err := metaStore.UnmarshalJSON(r)
		if err != nil {
			js.pushError(err)
			return
		}
		if e := metaStore.Create(metaObj); e == nil {
			js.push(map[string]string{"status": "OK"})
		} else {
			js.pushError(e)
		}
	}))
	router.DELETE(cs.root+"/meta/:name", CreateJsonAction(func(_ io.ReadCloser, js *JsonSink, p httprouter.Params, q url.Values) {
		if ok, e := metaStore.Remove(p.ByName("name")); e == nil {
			js.pushEmpty()
		} else {
			if ok {
				js.pushError(e)
			} else {
				js.pushError(&ServerError{status: http.StatusNotFound, code: ErrNotFound})
			}
		}
	}))
	router.POST(cs.root+"/meta/:name", CreateJsonAction(func(r io.ReadCloser, js *JsonSink, p httprouter.Params, q url.Values) {
		//TODO: meta object gets stored in MetaStore cache while unmarshalling, so it would be available even if it was not
		// actually stored in the Custodian
		metaObj, err := metaStore.UnmarshalJSON(r)
		if err != nil {
			js.pushError(err)
			return
		}
		if _, err := metaStore.Update(p.ByName("name"), metaObj); err == nil {
			js.pushEmpty()
		} else {
			js.pushError(err)
		}
	}))

	dm, _ := syncer.NewDataManager()

	proc, _ := data.NewProcessor(metaStore, dm)

	//Data routes
	router.PUT(cs.root+"/data/single/:name", CreateDualJsonAction(func(src *JsonSource, sink *JsonSink, p httprouter.Params, q url.Values) {
		if o, e := proc.Put(p.ByName("name"), src.Value); e != nil {
			sink.pushError(e)
		} else {
			sink.pushGeneric(o)
		}
	}))

	router.PUT(cs.root+"/data/bulk/:name", CreateDualJsonStreamAction(func(stream *JsonStream, sink *JsonSinkStream, p httprouter.Params, q url.Values) {
		defer sink.Complete()
		e := proc.PutBulk(p.ByName("name"), func() (map[string]interface{}, error) {
			if obj, eof, e := stream.Next(); e != nil {
				return nil, e
			} else if eof {
				return nil, nil
			} else {
				return obj, nil
			}
		}, func(obj map[string]interface{}) error { return sink.PourOff(obj) })
		if e != nil {
			sink.pushError(e)
		}
	}))

	router.GET(cs.root+"/data/single/:name/:key", CreateJsonAction(func(r io.ReadCloser, sink *JsonSink, p httprouter.Params, q url.Values) {
		var depth = 100
		if i, e := strconv.Atoi(q.Get("depth")); e == nil {
			depth = i
		}
		if o, e := proc.Get(p.ByName("name"), p.ByName("key"), depth); e != nil {
			sink.pushError(e)
		} else {
			if o == nil {
				sink.pushError(&ServerError{http.StatusNotFound, ErrNotFound, "object not found"})
			} else {
				sink.pushGeneric(o)
			}
		}
	}))

	router.GET(cs.root+"/data/bulk/:name", CreateJsonStreamAction(func(sink *JsonSinkStream, p httprouter.Params, q *url.URL) {
		defer sink.Complete()
		pq := make(url.Values)
		if e := softParseQuery(pq, q.RawQuery); e != nil {
			sink.pushError(e)
		} else {
			var depth = 100
			if i, e := strconv.Atoi(url.QueryEscape(pq.Get("depth"))); e == nil {
				depth = i
			}
			e := proc.GetBulk(p.ByName("name"), pq.Get("q"), depth, func(obj map[string]interface{}) error { return sink.PourOff(obj) })
			if e != nil {
				sink.pushError(e)
			}
		}
	}))

	router.DELETE(cs.root+"/data/single/:name/:key", CreateJsonAction(func(r io.ReadCloser, sink *JsonSink, p httprouter.Params, q url.Values) {
		if ok, e := proc.Delete(p.ByName("name"), p.ByName("key")); e != nil {
			sink.pushError(e)
		} else {
			if ok {
				sink.pushEmpty()
			} else {
				sink.pushError(&ServerError{http.StatusNotFound, ErrNotFound, "object not found"})
			}
		}
	}))

	router.DELETE(cs.root+"/data/bulk/:name", CreateDualJsonStreamAction(func(stream *JsonStream, sink *JsonSinkStream, p httprouter.Params, q url.Values) {
		defer sink.Complete()
		e := proc.DeleteBulk(p.ByName("name"), func() (map[string]interface{}, error) {
			if obj, eof, e := stream.Next(); e != nil {
				return nil, e
			} else if eof {
				return nil, nil
			} else {
				return obj, nil
			}
		})
		if e != nil {
			sink.pushError(e)
		}
	}))

	router.POST(cs.root+"/data/single/:name/:key", CreateDualJsonAction(func(src *JsonSource, sink *JsonSink, p httprouter.Params, q url.Values) {
		if o, e := proc.Update(p.ByName("name"), p.ByName("key"), src.Value); e != nil {
			if dt, ok := e.(*data.DataError); ok && dt.Code == data.ErrCasFailed {
				sink.pushError(&ServerError{http.StatusPreconditionFailed, dt.Code, dt.Msg})
			} else {
				sink.pushError(e)
			}
		} else {
			if o != nil {
				sink.pushGeneric(o)
			} else {
				sink.pushError(&ServerError{http.StatusNotFound, ErrNotFound, "object not found"})
			}
		}
	}))

	router.POST(cs.root+"/data/bulk/:name", CreateDualJsonStreamAction(func(stream *JsonStream, sink *JsonSinkStream, p httprouter.Params, q url.Values) {
		defer sink.Complete()
		e := proc.UpdateBulk(p.ByName("name"), func() (map[string]interface{}, error) {
			if obj, eof, e := stream.Next(); e != nil {
				return nil, e
			} else if eof {
				return nil, nil
			} else {
				return obj, nil
			}
		}, func(obj map[string]interface{}) error { return sink.PourOff(obj) })
		if e != nil {
			if dt, ok := e.(*data.DataError); ok && dt.Code == data.ErrCasFailed {
				sink.pushError(&ServerError{http.StatusPreconditionFailed, dt.Code, dt.Msg})
			} else {
				sink.pushError(e)
			}
		}
	}))

	cs.s = &http.Server{
		Addr:           cs.addr + ":" + cs.port,
		Handler:        router,
		ReadTimeout:    60 * time.Second,
		WriteTimeout:   60 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	cs.s.ListenAndServe()
}

//Creates an action to process an HTTP request in JSON format.
//It takes an function to process request, which accepts JsonSource, JsonSink and PathSegments.
func CreateDualJsonAction(f func(*JsonSource, *JsonSink, httprouter.Params, url.Values)) func(http.ResponseWriter, *http.Request, httprouter.Params) {
	return func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		src, e := (*httpRequest)(r).asJsonSource()
		if e != nil {
			returnError(w, e)
			return
		}
		sink, _ := asJsonSink(w)
		f(src, sink, p, r.URL.Query())
	}
}

func CreateDualJsonStreamAction(f func(*JsonStream, *JsonSinkStream, httprouter.Params, url.Values)) func(http.ResponseWriter, *http.Request, httprouter.Params) {
	return func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		stream, e := (*httpRequest)(r).asJsonStream()
		if e != nil {
			returnError(w, e)
			return
		}
		sink, _ := asJsonSinkStream(w)
		f(stream, sink, p, r.URL.Query())
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
		sink, _ := asJsonSinkStream(w)
		f(sink, p, r.URL)
	}
}

//Returns an error to HTTP response in JSON format.
//If the error object accepted is of ServerError type so HTTP status and code are taken from the error object.
//If the error corresponds to JsonError interface so HTTP status set to http.StatusBadRequest and code taken from the error object.
//Otherwise they sets to http.StatusInternalServerError and ErrInternalServerError respectively.
func returnError(w http.ResponseWriter, e error) {
	w.Header().Set("Content-Type", "application/json")
	switch e := e.(type) {
	case *ServerError:
		w.WriteHeader(e.status)
		w.Write(e.Json())
		return
	case JsonError:
		w.WriteHeader(http.StatusBadRequest)
		w.Write(e.Json())
		return
	default:
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(marshalError(ErrInternalServerError, e.Error()))
		return
	}
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
	if j, e := json.Marshal(obj); e != nil {
		returnError(js.rw, e)
	} else {
		js.rw.Header().Set("Content-Type", "application/json")
		js.rw.WriteHeader(http.StatusOK)
		js.rw.Write(j)
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

type JsonSinkStream struct {
	rw         http.ResponseWriter
	empty      bool
	status     string
	err        []byte
	httpStatus int
}

func asJsonSinkStream(w http.ResponseWriter) (*JsonSinkStream, error) {
	return &JsonSinkStream{rw: w, empty: true, status: "OK", err: nil, httpStatus: http.StatusOK}, nil
}

func (jss *JsonSinkStream) PourOff(obj map[string]interface{}) error {
	b, e := json.Marshal(obj)
	if e != nil {
		return e
	}
	if jss.empty {
		jss.empty = false
		jss.rw.Header().Set("Content-Type", "application/json")
		jss.rw.WriteHeader(jss.httpStatus)
		jss.rw.Write([]byte("{\"objs\":["))
		jss.rw.Write(b)
		return nil
	} else {
		jss.rw.Write([]byte{','})
		jss.rw.Write(b)
		return nil
	}
}

func (jss *JsonSinkStream) pushError(e error) {
	jss.status = "FAILED"
	switch e := e.(type) {
	case *ServerError:
		jss.httpStatus = e.status
		jss.err = e.Json()
		return
	case JsonError:
		jss.httpStatus = http.StatusBadRequest
		jss.err = e.Json()
		return
	default:
		jss.httpStatus = http.StatusInternalServerError
		jss.err = marshalError(ErrInternalServerError, e.Error())
		return
	}
}

func (jss *JsonSinkStream) Complete() {
	if jss.empty {
		jss.empty = false
		jss.rw.Header().Set("Content-Type", "application/json")
		jss.rw.WriteHeader(jss.httpStatus)
		jss.rw.Write([]byte("{\"objs\":["))
	}
	jss.rw.Write([]byte("],\"status\":\"" + jss.status + "\""))
	if jss.err != nil {
		jss.rw.Write([]byte(",\"error\":"))
		jss.rw.Write(jss.err)
	}
	jss.rw.Write([]byte("}"))

}
