package streams

import (
	"net/http"
	"encoding/json"
	. "server/errors"
	"strconv"
)

type JsonSinkStream struct {
	rw         http.ResponseWriter
	empty      bool
	status     string
	err        []byte
	httpStatus int
}

func AsJsonSinkStream(w http.ResponseWriter) (*JsonSinkStream, error) {
	return &JsonSinkStream{rw: w, empty: true, status: "OK", err: nil, httpStatus: http.StatusOK}, nil
}

func (jsonSinkStream *JsonSinkStream) PourOff(obj map[string]interface{}) error {
	//TODO: rewrite this method, response should not be wrote as byte sequence
	b, e := json.Marshal(obj)
	if e != nil {
		return e
	}
	if jsonSinkStream.empty {
		jsonSinkStream.empty = false
		jsonSinkStream.rw.Header().Set("Content-Type", "application/json")
		jsonSinkStream.rw.WriteHeader(jsonSinkStream.httpStatus)
		jsonSinkStream.rw.Write([]byte("{\"data\":["))
		jsonSinkStream.rw.Write(b)
		return nil
	} else {
		jsonSinkStream.rw.Write([]byte{','})
		jsonSinkStream.rw.Write(b)
		return nil
	}
}

func (jsonSinkStream *JsonSinkStream) PushError(e error) {
	jsonSinkStream.status = "FAILED"
	switch e := e.(type) {
	case *ServerError:
		jsonSinkStream.rw.WriteHeader(e.Status)
		jsonSinkStream.err = e.Json()
		jsonSinkStream.rw.Write(e.Json())
	case JsonError:
		jsonSinkStream.rw.WriteHeader(http.StatusBadRequest)
		jsonSinkStream.err = e.Json()
		jsonSinkStream.rw.Write(e.Json())
	default:
		err := ServerError{Status: http.StatusInternalServerError, Code: ErrInternalServerError, Msg: e.Error()}
		jsonSinkStream.err = err.Json()
		//header should be wrote before body
		jsonSinkStream.rw.WriteHeader(http.StatusInternalServerError)
		jsonSinkStream.rw.Write(err.Json())
	}

}

func (jsonSinkStream *JsonSinkStream) Complete(totalCount *int) {
	//TODO: rewrite this method
	if jsonSinkStream.empty {
		jsonSinkStream.empty = false
		jsonSinkStream.rw.Header().Set("Content-Type", "application/json")
		jsonSinkStream.rw.WriteHeader(jsonSinkStream.httpStatus)
		jsonSinkStream.rw.Write([]byte("{\"data\":["))
	}
	jsonSinkStream.rw.Write([]byte("],\"status\":\"" + jsonSinkStream.status + "\""))

	if totalCount != nil {
		jsonSinkStream.rw.Write([]byte(",\"total_count\":" + strconv.Itoa(*totalCount)))
	}

	if jsonSinkStream.err != nil {
		jsonSinkStream.rw.Write([]byte(",\"error\":"))
		jsonSinkStream.rw.Write(jsonSinkStream.err)
	}
	jsonSinkStream.rw.Write([]byte("}"))

}
