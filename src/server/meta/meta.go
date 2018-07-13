package meta

import (
	"encoding/json"
	"server/noti"
)

type LinkType int

const (
	LinkTypeOuter LinkType = iota + 1 //Child refers to the parent
	LinkTypeInner                     //Parent refres to the Child
)

func (lt LinkType) String() (string, bool) {
	switch lt {
	case LinkTypeOuter:
		return "outer", true
	case LinkTypeInner:
		return "inner", true
	default:
		return "", false
	}
}

func AsLinkType(s string) (LinkType, bool) {
	switch s {
	case "outer":
		return LinkTypeOuter, true
	case "inner":
		return LinkTypeInner, true
	default:
		return 0, false
	}
}

func (lt *LinkType) UnmarshalJSON(b []byte) error {
	var s string
	if e := json.Unmarshal(b, &s); e != nil {
		return e
	}
	if l, ok := AsLinkType(s); ok {
		*lt = l
		return nil
	} else {
		return NewMetaError("", "json_unmarshal", ErrJsonUnmarshal, "Incorrect link type: %s", s)
	}
}

func (lt LinkType) MarshalJSON() ([]byte, error) {
	if s, ok := lt.String(); ok {
		return json.Marshal(s)
	} else {
		return nil, NewMetaError("", "json_marshal", ErrJsonMarshal, "Incorrect link type: %v", lt)
	}
}

type Def interface{}
type DefConstStr struct{ Value string }
type DefConstFloat struct{ Value float64 }
type DefConstInt struct{ Value int }
type DefConstBool struct{ Value bool }
type DefExpr struct {
	Func string
	Args []interface{}
}

type Method int

const (
	MethodRetrive Method = iota + 1
	MethodCreate
	MethodRemove
	MethodUpdate
)

func (m Method) String() (string, bool) {
	switch m {
	case MethodRetrive:
		return "retrive", true
	case MethodCreate:
		return "create", true
	case MethodRemove:
		return "remove", true
	case MethodUpdate:
		return "update", true
	default:
		return "", false
	}
}

func AsMethod(s string) (Method, bool) {
	switch s {
	case "retrive":
		return MethodRetrive, true
	case "create":
		return MethodCreate, true
	case "remove":
		return MethodRemove, true
	case "update":
		return MethodUpdate, true
	default:
		return 0, false
	}
}

func (mt *Method) UnmarshalJSON(b []byte) error {
	var s string
	if e := json.Unmarshal(b, &s); e != nil {
		return e
	}
	if m, ok := AsMethod(s); ok {
		*mt = m
		return nil
	} else {
		return NewMetaError("", "json_unmarshal", ErrJsonUnmarshal, "Incorrect method: %s", s)
	}
}

func (mt Method) MarshalJSON() ([]byte, error) {
	if s, ok := mt.String(); ok {
		return json.Marshal(s)
	} else {
		return nil, NewMetaError("", "json_marshal", ErrJsonMarshal, "Incorrect method: %v", mt)
	}
}

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
)

func (p *Protocol) MarshalJSON() ([]byte, error) {
	if s, ok := p.String(); ok {
		return json.Marshal(s)
	} else {
		return nil, NewMetaError("", "json_marshal", ErrJsonMarshal, "Incorrect protocol: %v", p)
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
		return NewMetaError("", "json_unmarshal", ErrJsonUnmarshal, "Incorrect protocol: %s", s)
	}
}

var notifierFactories = map[Protocol]noti.Factory{
	REST: noti.NewRestNotifier,
}

type actions struct {
	original  []action
	notifiers map[Method][]noti.Notifier
}

func newActions(array []action) (*actions, error) {
	notifiers := make(map[Method][]noti.Notifier)
	for i, _ := range array {
		factory, ok := notifierFactories[array[i].Protocol]
		if !ok {
			ps, _ := array[i].Protocol.String()
			return nil, NewMetaError("", "create_actions", ErrInternal, "Notifier factory not found for protocol: %s", ps)
		}

		notifier, err := factory(array[i].Args, array[i].ActiveIfNotRoot)
		if err != nil {
			return nil, err
		}
		m := array[i].Method
		notifiers[m] = append(notifiers[m], notifier)
	}
	return &actions{original: array, notifiers: notifiers}, nil
}

func (a *actions) StartNotification(method Method) chan *noti.Event {
	return noti.Broadcast(a.notifiers[method])
}

type action struct {
	Method          Method   `json:"method"`
	Protocol        Protocol `json:"protocol"`
	Args            []string `json:"args,omitempty"`
	ActiveIfNotRoot bool     `json:"activeIfNotRoot"`
}

//Object metadata description.
type Meta struct {
	*MetaDescription
	Key     *FieldDescription
	Fields  []FieldDescription
	Actions *actions
}

func (m *Meta) FindField(name string) *FieldDescription {
	for i, _ := range m.Fields {
		if m.Fields[i].Name == name {
			return &m.Fields[i]
		}
	}
	return nil
}

func (m Meta) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.MetaDescription)
}

//The shadow struct of the Meta struct.
type MetaDescription struct {
	Name    string   `json:"name"`
	Key     string   `json:"key"`
	Fields  []Field  `json:"fields"`
	Actions []action `json:"actions,omitempty"`
	Cas     bool     `json:"cas"`
}

func (f *FieldDescription) canBeLinkTo(m *Meta) bool {
	return (f.IsSimple() && f.Type == m.Key.Type) || (f.Type == FieldTypeObject && f.LinkMeta.Name == m.Name && f.LinkType == LinkTypeInner)
}

/*
   Meta driver interface.
*/
type MetaDriver interface {
	List() (*[]*MetaDescription, bool, error)
	Get(name string) (*MetaDescription, bool, error)
	Create(m MetaDescription) error
	Remove(name string) (bool, error)
	Update(name string, m MetaDescription) (bool, error)
	BeginTransaction() (error)
	CommitTransaction() (error)
	RollbackTransaction() (error)
}

type MetaDbSyncer interface {
	CreateObj(*Meta) error
	RemoveObj(string, bool) error
	UpdateObj(old, new *Meta) error
	UpdateObjTo(*Meta) error
	ValidateObj(*Meta) (bool, error)
	BeginTransaction() (error)
	CommitTransaction() (error)
	RollbackTransaction() (error)
}
