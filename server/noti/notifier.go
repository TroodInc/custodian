package noti

import (
	"custodian/server/errors"
	"encoding/json"
	"fmt"
)
var protocols []string

type Protocol int

func (p Protocol) String() (string, bool) {
	if i := int(p); i <= 0 || i > len(protocols) {
		return "", false
	} else {
		return protocols[i-1], true
	}
}
func protocol_iota(s string) Protocol {
	protocols = append(protocols, s)
	return Protocol(len(protocols))
}

func asProtocol(name string) (Protocol, bool) {
	for i, _ := range protocols {
		if protocols[i] == name {
			return Protocol(i + 1), true
		}
	}
	return Protocol(0), false
}

var (
	REST = protocol_iota("REST")
	TEST = protocol_iota("TEST")
)

func (p *Protocol) MarshalJSON() ([]byte, error) {
	if s, ok := p.String(); ok {
		return json.Marshal(s)
	} else {
		return nil, errors.NewValidationError("ErrJsonMarshal", "Incorrect protocol: %v", p)
	}
}
func (p *Protocol) UnmarshalJSON(b []byte) error {
	var s string
	if e := json.Unmarshal(b, &s); e != nil {
		return e
	}
	if protocol, ok := asProtocol(s); ok {
		*p = protocol
		return nil
	} else {
		return errors.NewValidationError("ErrJsonUnmarshal", "Incorrect protocol: %s", s)
	}
}

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

func (e Event) Obj() map[string]interface{} {
	return e.obj
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
	out := notifier.NewNotification()
	go fan_out(in, out)
	return in
}
