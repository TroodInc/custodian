package server

import (
	"github.com/julienschmidt/httprouter"
	"net/http"
	"io"
	"net/url"
	. "server/streams"
	"runtime/debug"
)

func ProfilerMiddleware(enableProfiler bool, wrapped func(http.ResponseWriter, *http.Request, httprouter.Params)) func(http.ResponseWriter, *http.Request, httprouter.Params) {
	return func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		wrapped(w, r, p)
		if enableProfiler {
			debug.FreeOSMemory()
		}
	}
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
