package noti

import (
	"encoding/json"
	"fmt"
)

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

func NewObjectEvent(obj map[string]interface{}, isRoot bool) *Event {
	return &Event{obj: obj, isRoot: isRoot}
}

func NewErrorEvent(err error) *Event {
	return &Event{err: err}
}

type Notifier interface {
	NewNotification() chan *Event
}

type Factory func(args []string, activeIfNotRoot bool) (Notifier, error)

func fan_out(in chan *Event, outs []chan *Event) {
	defer func() {
		for i, _ := range outs {
			close(outs[i])
		}
	}()
	for obj := range in {
		for i, _ := range outs {
			outs[i] <- obj
		}
	}
}

func Broadcast(notifiers []Notifier) chan *Event {
	in := make(chan *Event, 100)
	outs := make([]chan *Event, 0, len(notifiers))
	for i, _ := range notifiers {
		outs = append(outs, notifiers[i].NewNotification())
	}
	go fan_out(in, outs)
	return in
}
