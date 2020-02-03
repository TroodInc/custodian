package noti

import (
	"encoding/json"
	"fmt"
)

var (
	REST = protocol_iota("REST")
	TEST = protocol_iota("TEST")
)

var NotifierFactories = map[Protocol]Factory{
	REST: NewRestNotifier,
	TEST: NewTestNotifier,
}

type NotiError struct {
	code string
	msg  string
}

func (e *NotiError) Error() string {
	return fmt.Sprintf("DDL error:  code='%s'  msg = '%s'", e.code, e.msg)
}

func (e *NotiError) Json() []byte {
	j, _ := json.Marshal(map[string]string{
		"code": "noti:" + e.code,
		"msg":  e.msg,
	})
	return j
}

func NewNotiError(code string, msg string, a ...interface{}) *NotiError {
	return &NotiError{code: code, msg: fmt.Sprintf(msg, a...)}
}

type Event struct {
	obj    map[string]interface{}
	isRoot bool
	err    error
}

func NewObjectEvent(notificationObject map[string]interface{}, isRoot bool) *Event {
	return &Event{obj: notificationObject, isRoot: isRoot}
}

func NewErrorEvent(err error) *Event {
	return &Event{err: err}
}

type Notifier interface {
	NewNotification() chan *Event
}

type Factory func(args []string, activeIfNotRoot bool) (Notifier, error)

func fan_out(in chan *Event, out chan *Event) {
	defer func() {
		close(out)
	}()
	for obj := range in {
		out <- obj
	}
}

func Broadcast(notifier Notifier) chan *Event {
	in := make(chan *Event, 100)
	out := make(chan *Event, 0)
	out = notifier.NewNotification()
	go fan_out(in, out)
	return in
}
