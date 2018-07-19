package meta

import (
	"encoding/json"
	"server/noti"
	"utils"
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
	MethodRetrieve Method = iota + 1
	MethodCreate
	MethodRemove
	MethodUpdate
)

func (m Method) AsString() (string) {
	switch m {
	case MethodRetrieve:
		return "retrieve"
	case MethodCreate:
		return "create"
	case MethodRemove:
		return "remove"
	case MethodUpdate:
		return "update"
	default:
		return ""
	}
}

func (m Method) Validate() (string, bool) {
	if methodAsString := m.AsString(); methodAsString == "" {
		return methodAsString, false
	} else {
		return methodAsString, true
	}
}

func AsMethod(s string) (Method, bool) {
	switch s {
	case "retrieve":
		return MethodRetrieve, true
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
	if s, ok := mt.Validate(); ok {
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
	isSimpleFieldWithSameTypeAsPk := f.IsSimple() && f.Type == m.Key.Type
	isInnerLinkToMeta := f.Type == FieldTypeObject && f.LinkMeta.Name == m.Name && f.LinkType == LinkTypeInner
	isGenericInnerLinkToMeta := f.Type == FieldTypeGeneric && f.LinkType == LinkTypeInner && utils.Contains(f.Field.LinkMetaList, m.Name)
	canBeLinkTo := isSimpleFieldWithSameTypeAsPk || isInnerLinkToMeta || isGenericInnerLinkToMeta
	return canBeLinkTo
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
