package noti

import (
	"bytes"
	"encoding/json"
	"logger"
	"io"
	"net/http"
	"net/url"
	"time"
)

var REST_MAX_REDELIVERY_BUF_SIZE int = 1024 * 1024 //0 - turn off redilivery
var REST_REDELIVERY_PAUSE_IN_SEC time.Duration = 10
var REST_MAX_REDILIVERY_ATTEMPTS byte = 3

const (
	ErrRESTNoURLFound = "rest_no_url_found"
	ErrRESTFailedURL  = "rest_failed_url"
)

var restClient = &http.Client{
	Timeout: time.Second * 10,
}

type restNotifier struct {
	url             string
	activeIfNotRoot bool
}

func NewRestNotifier(args []string, activeIfNotRoot bool) (Notifier, error) {
	if len(args) < 1 {
		return nil, NewNotiError(ErrRESTNoURLFound, "Build a rest notifier failed. No URL found in arguments")
	}

	if _, err := url.Parse(args[0]); err != nil {
		return nil, NewNotiError(ErrRESTFailedURL, "Build a rest notifier failed. Specified URL '%s' is bad: %s", args[0], err.Error())
	}
	return &restNotifier{url: args[0], activeIfNotRoot: activeIfNotRoot}, nil
}

type jsonBodyStream struct {
	w          io.WriteCloser
	entityName string
	empty      bool
	status     string
	err        []byte
}

func (jbs *jsonBodyStream) PourOff(o interface{}) error {
	b, e := json.Marshal(o)
	if e != nil {
		return e
	}

	if jbs.empty {
		jbs.empty = false
		//use append to reduse count of chunks
		_, e = jbs.w.Write(append([]byte("{\""+jbs.entityName+"\":["), b...))
	} else {
		_, e = jbs.w.Write(append([]byte(","), b...))
	}
	return e
}

func (jbs *jsonBodyStream) Failed(err error) {
	//currently error not use but in the future it's possible
	jbs.status = "FAILED"
}

func (jbs *jsonBodyStream) Complete() error {
	defer func() {
		jbs.w.Close()
	}()
	//use buffer to reduse count of chunks
	var b bytes.Buffer
	if jbs.empty {
		jbs.empty = false
		b.Write([]byte("{\"" + jbs.entityName + "\":["))
	}
	b.Write([]byte("],\"status\":\"" + jbs.status + "\""))
	if jbs.err != nil {
		b.Write([]byte(",\"error\":"))
		b.Write(jbs.err)
	}
	b.Write([]byte("}"))
	_, e := b.WriteTo(jbs.w)
	return e
}

func newJsonBodyStream(entityName string, w io.WriteCloser) *jsonBodyStream {
	return &jsonBodyStream{entityName: entityName, w: w, empty: true, status: "OK", err: nil}
}

type bufSilentWriter struct {
	cap   int
	buf   bytes.Buffer
	total int
}

func (bw *bufSilentWriter) isExceeded() bool {
	return bw.total > bw.cap
}

func (bw *bufSilentWriter) Write(data []byte) (int, error) {
	bw.total = bw.total + len(data)
	if bw.isExceeded() {
		return len(data), nil
	} else {
		return bw.buf.Write(data)
	}
}

func (bw *bufSilentWriter) Close() error {
	return nil
}

func newBufSilentWriter(cap int) *bufSilentWriter {
	return &bufSilentWriter{cap: cap}
}

type atLeastOneWriteCloser struct {
	writers []io.WriteCloser
}

func (t *atLeastOneWriteCloser) Write(p []byte) (int, error) {
	lastError := io.ErrClosedPipe
	survivors := t.writers[:0]
	for _, w := range t.writers {
		if _, err := w.Write(p); err != nil {
			logger.Error("Failed write to one of the writers: %s", err.Error())
			lastError = err
		} else {
			survivors = append(survivors, w)
		}
	}

	if len(survivors) == 0 {
		return 0, lastError
	}

	t.writers = survivors
	return len(p), nil
}

func (t *atLeastOneWriteCloser) Close() error {
	allFailed := true
	lastError := io.ErrClosedPipe
	for _, w := range t.writers {
		if err := w.Close(); err != nil {
			logger.Error("Failed to close one of the writers: %s", err.Error())
			lastError = err
		} else {
			allFailed = false
		}
	}

	if allFailed {
		return lastError
	}

	return nil
}

func newAtLeastOneWriteCloser(writers ...io.WriteCloser) io.WriteCloser {
	return &atLeastOneWriteCloser{writers}
}

func (rn *restNotifier) handle(event *Event, body *jsonBodyStream) error {
	if event.err != nil {
		body.Failed(event.err)
		return event.err
	}
	if rn.activeIfNotRoot || event.isRoot {
		return body.PourOff(event.obj)
	}
	return nil
}

//must be fast bacause of there is backprebackpre to object operations
func (rn *restNotifier) consume(in chan *Event, body *jsonBodyStream, complete chan bool) {
	defer func() {
		body.Complete()
		complete <- true
	}()
	handle := rn.handle
	var err error
	for event := range in {
		if err = handle(event, body); err != nil {
			logger.Error("The notification has stopped due to en error: %s", err.Error())
			handle = func(_ *Event, _ *jsonBodyStream) error { return nil }
		}
	}
}

func (rn *restNotifier) redelivery(body []byte, attempt byte) {
	timer := time.NewTimer(time.Second * REST_REDELIVERY_PAUSE_IN_SEC)
	logger.Info("Scheduled '%d' attempt of re-delivery notification for '%s' URL in '%d' seconds", attempt, rn.url, REST_REDELIVERY_PAUSE_IN_SEC)
	<-timer.C
	logger.Info("Setup '%d' attempt of rre-delivery notification for '%s' URL", attempt, rn.url)
	pr, pw := io.Pipe()
	go func() {
		pw.Write(body)
		pw.Close()
	}()
	tryAgain := func() {
		if attempt < REST_MAX_REDILIVERY_ATTEMPTS {
			go rn.redelivery(body, attempt+1)
		} else {
			logger.Error("Can't schedule re-delivery for '%s' URL. Achived max re-delivery attempts '%d'", rn.url, REST_MAX_REDILIVERY_ATTEMPTS)
		}
	}
	resp, err := restClient.Post(rn.url, "application/json", pr)
	if err != nil {
		logger.Error("Error sending notification: %s", err.Error())
		tryAgain()
		return
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		logger.Error("Received an invalid response code '%d' from the '%s' notified server", resp.StatusCode, rn.url)
		tryAgain()
	}
}

func (rn *restNotifier) start(in chan *Event) {
	pr, pw := io.Pipe()
	redelBuf := newBufSilentWriter(REST_MAX_REDELIVERY_BUF_SIZE)
	mw := newAtLeastOneWriteCloser(pw, redelBuf)
	body := newJsonBodyStream("events", mw)
	complete := make(chan bool, 1)
	go rn.consume(in, body, complete)
	resp, err := restClient.Post(rn.url, "application/json", pr)
	failed := false
	if err != nil {
		logger.Error("Error sending notification: %s", err.Error())
		failed = true
	} else {
		resp.Body.Close()
		if resp.StatusCode != 200 {
			logger.Error("Received an invalid response code '%d' from the '%s' notified server", resp.StatusCode, rn.url)
			failed = true
		}
	}

	//waiting for the end of cunsumption
	if <-complete && failed {
		if !redelBuf.isExceeded() {
			go rn.redelivery(redelBuf.buf.Bytes(), 1)
		} else if REST_MAX_REDELIVERY_BUF_SIZE > 0 {
			logger.Error("Can't schedule re-delivery for '%s' URL. The body size '%d' is exceeded specified max redilivery buffer size '%d'", rn.url, redelBuf.total, REST_MAX_REDELIVERY_BUF_SIZE)
		}
	}
}

func (rn *restNotifier) NewNotification() chan *Event {
	// Use buffer to reduce the effect of network latency on process
	in := make(chan *Event, 100)
	go rn.start(in)
	return in
}
