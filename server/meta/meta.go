package meta

import (
	"encoding/json"
	"fmt"
	"github.com/Q-CIS-DEV/custodian/logger"
	"io"
	"reflect"
	"strconv"
	"sync"
)

//Types description
type FieldType int

const (
	FieldTypeString FieldType = iota + 1
	FieldTypeNumber
	FieldTypeBool
	FieldTypeArray
	FieldTypeObject
)

func (fieldType FieldType) String() (string, bool) {
	switch fieldType {
	case FieldTypeString:
		return "string", true
	case FieldTypeNumber:
		return "number", true
	case FieldTypeBool:
		return "bool", true
	case FieldTypeArray:
		return "array", true
	case FieldTypeObject:
		return "object", true
	default:
		return "", false
	}
}

func (fieldType FieldType) AssertType(i interface{}) bool {
	switch fieldType {
	case FieldTypeString:
		_, ok := i.(string)
		return ok
	case FieldTypeNumber:
		_, ok := i.(float64)
		return ok
	case FieldTypeBool:
		_, ok := i.(bool)
		return ok
	case FieldTypeArray:
		_, ok := i.([]interface{})
		return ok
	case FieldTypeObject:
		_, ok := i.(map[string]interface{})
		return ok
	default:
		return false
	}
}

func (fieldType FieldType) TypeAsserter() func(interface{}) bool {
	switch fieldType {
	case FieldTypeString:
		return func(i interface{}) bool {
			_, ok := i.(string)
			return ok
		}
	case FieldTypeNumber:
		return func(i interface{}) bool {
			_, ok := i.(float64)
			return ok
		}
	case FieldTypeBool:
		return func(i interface{}) bool {
			_, ok := i.(bool)
			return ok
		}
	case FieldTypeArray:
		return func(i interface{}) bool {
			_, ok := i.([]interface{})
			return ok
		}
	case FieldTypeObject:
		return func(i interface{}) bool {
			_, ok := i.(map[string]interface{})
			return ok
		}
	default:
		return func(interface{}) bool { return false }
	}
}

func AsFieldType(s string) (FieldType, bool) {
	switch s {
	case "string":
		return FieldTypeString, true
	case "number":
		return FieldTypeNumber, true
	case "bool":
		return FieldTypeBool, true
	case "array":
		return FieldTypeArray, true
	case "object":
		return FieldTypeObject, true
	default:
		return 0, false
	}
}

func (fieldType *FieldType) UnmarshalJSON(b []byte) error {
	var str string
	if e := json.Unmarshal(b, &str); e != nil {
		return e
	}
	if assumedFieldType, ok := AsFieldType(str); ok {
		*fieldType = assumedFieldType
		return nil
	} else {
		return NewMetaError("", "json_unmarshal", ErrJsonUnmarshal, "Incorrect field type: %str", str)
	}
}

func (fieldType FieldType) MarshalJSON() ([]byte, error) {
	if s, ok := fieldType.String(); ok {
		return json.Marshal(s)
	} else {
		return nil, NewMetaError("", "json_marshal", ErrJsonMarshal, "Incorrect filed type: %v", fieldType)
	}
}

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

//Field description.
type field struct {
	Name           string      `json:"name"`
	Type           FieldType   `json:"type"`
	LinkMeta       string      `json:"linkMeta,omitempty"` //only for array and objects
	LinkType       LinkType    `json:"linkType,omitempty"`
	OuterLinkField string      `json:"outerLinkField,omitempty"`
	Optional       bool        `json:"optional"`
	Def            interface{} `json:"default,omitempty"`
}

func (f *field) IsSimple() bool {
	return f.Type != FieldTypeObject && f.Type != FieldTypeArray
}

type Def interface{}
type DefConstStr struct{ Value string }
type DefConstNum struct{ Value float64 }
type DefConstBool struct{ Value bool }
type DefExpr struct {
	Func string
	Args []interface{}
}

type Field struct {
	*field
	Meta           *Meta
	LinkMeta       *Meta
	OuterLinkField *Field
}

func (f *Field) Default() Def {
	switch t := f.field.Def.(type) {
	case string:
		return DefConstStr{t}
	case float64:
		return DefConstNum{t}
	case bool:
		return DefConstBool{t}
	case map[string]interface{}:
		var args []interface{}
		if a, ok := t["args"]; ok {
			args = a.([]interface{})
		}
		return DefExpr{Func: t["func"].(string), Args: args}
	default:
		return nil
	}
}

func (f *Field) IsValueTypeValid(v interface{}) bool {
	switch f.Type {
	case FieldTypeString:
		_, ok := v.(string)
		return ok
	case FieldTypeNumber:
		_, ok := v.(float64)
		return ok
	case FieldTypeBool:
		_, ok := v.(bool)
		return ok
	default:
		return false
	}

}

func (f *Field) ValueFromString(v string) (interface{}, error) {
	switch f.Type {
	case FieldTypeString:
		return v, nil
	case FieldTypeNumber:
		return strconv.ParseFloat(v, 64)
	case FieldTypeBool:
		return strconv.ParseBool(v)
	default:
		return nil, NewMetaError(f.Meta.Name, "pk_from_string_conversion", ErrInternal, "Unsupported conversion from 'string' for the field type '%s'", f.Type)
	}
}

func (f *Field) ValueAsString(v interface{}) (string, error) {
	switch f.Type {
	case FieldTypeString:
		if str, ok := v.(string); !ok {
			return "", NewMetaError(f.Meta.Name, "conversion", ErrInternal,
				"Wrong input value type '%s'. For field '%s' expects 'string' type", reflect.TypeOf(v).String(), f.Name)
		} else {
			return str, nil
		}
	case FieldTypeNumber:
		if flt, ok := v.(float64); !ok {
			return "", NewMetaError(f.Meta.Name, "conversion", ErrInternal,
				"Wrong input value type '%s'. For field '%s' expects 'float64' type", reflect.TypeOf(v).String(), f.Name)
		} else {
			return strconv.FormatFloat(flt, 'f', -1, 64), nil
		}
	case FieldTypeBool:
		if b, ok := v.(bool); !ok {
			return "", NewMetaError(f.Meta.Name, "conversion", ErrInternal,
				"Wrong input value type '%s'. For field '%s' expects 'bool' type", reflect.TypeOf(v).String(), f.Name)
		} else {
			return strconv.FormatBool(b), nil
		}
	case FieldTypeObject, FieldTypeArray:
		if f.LinkType == LinkTypeInner {
			return f.LinkMeta.Key.ValueAsString(v)
		} else {
			return f.OuterLinkField.ValueAsString(v)
		}
	default:
		return "", NewMetaError(f.Meta.Name, "conversion", ErrInternal, "Unknown field type '%s'", f.Type)
	}

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

type Action struct {
	*action
}

type action struct {
	Method   Method
	Protocol string
	Args     []string
}

//Object metadata description.
type Meta struct {
	*meta
	Key     *Field
	Fields  []Field
	Actions []Action
}

func (m *Meta) FindField(name string) *Field {
	for i, _ := range m.Fields {
		if m.Fields[i].Name == name {
			return &m.Fields[i]
		}
	}
	return nil
}

func (m Meta) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.meta)
}

//The shadow struct of the Meta struct.
type meta struct {
	Name    string   `json:"name"`
	Key     string   `json:"key"`
	Fields  []field  `json:"fields"`
	Actions []action `json:"actions,omitempty"`
	Cas     bool     `json:"cas"`
}

const (
	ErrDuplicated    = "duplicated"
	ErrNotFound      = "not_found"
	ErrNotValid      = "not_valid"
	ErrInternal      = "internal"
	ErrJsonUnmarshal = "json_unmarshal"
	ErrJsonMarshal   = "json_marshal"
)

type metaError struct {
	code string
	msg  string
	meta string
	op   string
}

func (e *metaError) Error() string {
	return fmt.Sprintf("Meta error:  meta = '%s', operation = '%s', code='%s'  msg = '%s'", e.meta, e.op, e.code, e.msg)
}

func (e *metaError) Json() []byte {
	j, _ := json.Marshal(map[string]string{
		"meta": e.meta,
		"op":   e.op,
		"code": "meta:" + e.code,
		"msg":  e.msg,
	})
	return j
}

func NewMetaError(meta string, op string, code string, msg string, a ...interface{}) *metaError {
	return &metaError{meta: meta, op: op, code: code, msg: fmt.Sprintf(msg, a...)}
}

/*
   Metadata store of objects persisted in DB.
*/
type MetaStore struct {
	drv         MetaDriver
	cache       map[string]*Meta
	cacheMutex  sync.RWMutex
	syncer      MetaDbSyncer
	syncerMutex sync.RWMutex
}

func (metaStore *MetaStore) UnmarshalJSON(r io.ReadCloser) (*Meta, error) {
	var metaObj meta
	if e := json.NewDecoder(r).Decode(&metaObj); e != nil {
		return nil, NewMetaError("", "unmarshal", ErrNotValid, e.Error())
	}
	return metaStore.newMeta(&metaObj)
}

func (metaStore *MetaStore) unmarshalMeta(b []byte) (*Meta, error) {
	var m meta
	if e := json.Unmarshal(b, &m); e != nil {
		return nil, e
	}
	return metaStore.newMeta(&m)
}

func (f *Field) canBeLinkTo(m *Meta) bool {
	return (f.IsSimple() && f.Type == m.Key.Type) || (f.Type == FieldTypeObject && f.LinkMeta.Name == m.Name && f.LinkType == LinkTypeInner)
}

func (metaStore *MetaStore) newMeta(metaObj *meta) (*Meta, error) {
	createdMeta := &Meta{meta: metaObj}
	notResolved := []*Meta{createdMeta}
	shadowCache := map[string]*Meta{metaObj.Name: createdMeta}
	for ; len(notResolved) > 0; notResolved = notResolved[1:] {
		bm := notResolved[0]
		fieldsLen := len(bm.meta.Fields)
		bm.Fields = make([]Field, fieldsLen, fieldsLen)
		for i := 0; i < fieldsLen; i++ {
			bm.Fields[i].Meta = bm
			f := &bm.meta.Fields[i]
			bm.Fields[i].field = f
			if f.LinkMeta != "" {
				var ok bool
				if bm.Fields[i].LinkMeta, ok = metaStore.cache[f.LinkMeta]; ok {
					continue
				}
				if bm.Fields[i].LinkMeta, ok = shadowCache[f.LinkMeta]; ok {
					continue
				}

				lm, _, err := metaStore.drv.Get(f.LinkMeta)
				if err != nil {
					return nil, NewMetaError(metaObj.Name, "new_meta", ErrNotValid, "Field '%s' has iccorect link meta: %s", f.Name, err.Error())
				}
				bm.Fields[i].LinkMeta = &Meta{meta: lm}
				shadowCache[f.LinkMeta] = bm.Fields[i].LinkMeta
				notResolved = append(notResolved, bm.Fields[i].LinkMeta)
			}
		}

		actionsLen := len(bm.meta.Actions)
		bm.Actions = make([]Action, actionsLen, actionsLen)
		for i, _ := range bm.meta.Actions {
			bm.Actions[i].action = &bm.meta.Actions[i]
		}

		if bm.Key = bm.FindField(bm.meta.Key); bm.Key == nil {
			return nil, NewMetaError(metaObj.Name, "new_meta", ErrNotValid, "Meta '%s' is inccorrect. The specified key '%s' field not found", bm.meta.Name, bm.meta.Key)
		} else if !bm.Key.IsSimple() {
			return nil, NewMetaError(metaObj.Name, "new_meta", ErrNotValid, "Meta '%s' is inccorrect. The key field '%s' is not simple", bm.meta.Name, bm.meta.Key)
		}

		if bm.Cas {
			if cas := bm.FindField("cas"); cas != nil {
				if cas.Type != FieldTypeNumber {
					return nil, NewMetaError(metaObj.Name, "new_meta", ErrNotValid, "The filed 'cas' specified in the meta '%s' as CAS must be type of 'number'", bm.meta.Cas, bm.meta.Name)
				}
			} else {
				return nil, NewMetaError(metaObj.Name, "new_meta", ErrNotValid, "Meta '%s' has CAS defined but the filed 'cas' it refers to is absent", bm.meta.Name, bm.meta.Cas)
			}
		}
	}

	for _, bm := range shadowCache {
		//processing outer links
		fieldsLen := len(bm.meta.Fields)
		for i := 0; i < fieldsLen; i++ {
			f := &bm.Fields[i]
			if f.LinkType != LinkTypeOuter {
				continue
			}
			if f.OuterLinkField = f.LinkMeta.FindField(f.field.OuterLinkField); f.OuterLinkField == nil {
				return nil, NewMetaError(metaObj.Name, "new_meta", ErrNotValid, "Filed '%s' has incorrect outer link. Meta '%s' doesn't have a field '%s'", f.Name, f.LinkMeta.Name, f.field.OuterLinkField)
			} else if !f.OuterLinkField.canBeLinkTo(f.Meta) {
				return nil, NewMetaError(metaObj.Name, "new_meta", ErrNotValid, "Filed '%s' has incorrect outer link. Field '%s' of meta '%s' can't refer to meta '%s'", f.Name, f.OuterLinkField.Name, f.OuterLinkField.Meta.Name, f.Meta.Name)
			}
		}
	}

	for k, bm := range shadowCache {
		metaStore.cache[k] = bm
	}

	return createdMeta, nil
}

func NewStore(md MetaDriver, mds MetaDbSyncer) *MetaStore {
	return &MetaStore{drv: md, syncer: mds, cache: make(map[string]*Meta)}
}

/*
   Retrives the list of metadata objects from the underlying store.
*/
func (metaStore *MetaStore) List() (*[]*meta, bool, error) {
	metaStore.syncerMutex.RLock()
	defer metaStore.syncerMutex.RUnlock()
	metaList, isFound, err := metaStore.drv.List()

	if err != nil {
		return &[]*meta{}, isFound, err
	}

	return metaList, isFound, nil
}

/*
   Retrives object metadata from the underlying store.
*/
func (metaStore *MetaStore) Get(name string) (*Meta, bool, error) {
	//try to get business object from cache
	//metaStore.cacheMutex.RLock()
	//if businessObject, ok := metaStore.cache[name]; ok {
	//	metaStore.cacheMutex.RUnlock()
	//	return businessObject, true, nil
	//}
	//metaStore.cacheMutex.RUnlock()
	metaStore.syncerMutex.RLock()
	defer metaStore.syncerMutex.RUnlock()

	//otherwise
	//retrieve business object metadata from the storage
	metaData, isFound, err := metaStore.drv.Get(name)

	if err != nil {
		return nil, isFound, err
	}
	//assemble the new business object with the given metadata
	businessObject, err := metaStore.newMeta(metaData)
	if err != nil {
		return nil, isFound, err
	}
	//validate the newly created business object against existing one in the database
	ok, err := metaStore.syncer.ValidateObj(businessObject)
	if ok {
		metaStore.cacheMutex.Lock()
		metaStore.cache[name] = businessObject
		metaStore.cacheMutex.Unlock()
		return businessObject, isFound, nil
	}

	logger.Error("Error while validating a meta from store against the object in DB: %v", err)
	return nil, false, err
}

// Creates a new object type described by passed metadata.
func (metaStore *MetaStore) Create(m *Meta) error {
	metaStore.syncerMutex.Lock()
	defer metaStore.syncerMutex.Unlock()

	if e := metaStore.syncer.CreateObj(m); e == nil {
		if e := metaStore.drv.Create(*m.meta); e == nil {
			return nil
		} else {
			var e2 = metaStore.syncer.RemoveObj(m.Name)
			logger.Error("Error while compenstaion of object '%s' metadata creation: %v", m.Name, e2)
			return e
		}
	} else {
		return e
	}
}

// Deletes an existing object metadata from the store.
func (metaStore *MetaStore) Remove(name string) (bool, error) {
	//remove object from the database
	if e := metaStore.syncer.RemoveObj(name); e == nil {
		//remove object`s description *.json file
		metaStore.syncerMutex.Lock()
		defer metaStore.syncerMutex.Unlock()
		ok, err := metaStore.drv.Remove(name)
		//remove object from cache
		metaStore.cacheMutex.Lock()
		delete(metaStore.cache, name)
		metaStore.cacheMutex.Unlock()
		//
		return ok, err
	} else {
		return false, e
	}

}

// Updates an existing object metadata.
func (metaStore *MetaStore) Update(name string, businessObj *Meta) (bool, error) {
	if currentBusinessObj, ok, err := metaStore.Get(name); err == nil {
		metaStore.syncerMutex.Lock()
		defer metaStore.syncerMutex.Unlock()
		ok, e := metaStore.drv.Update(name, *businessObj.meta)
		metaStore.cacheMutex.Lock()
		delete(metaStore.cache, name)
		metaStore.cacheMutex.Unlock()

		if e != nil {
			return ok, e
		}

		if !ok {
			return ok, e
		}
		//TODO: This logic tells NOTHING about error if it was successfully rolled back. This
		//behaviour should be fixed
		if e := metaStore.syncer.UpdateObj(currentBusinessObj, businessObj); e == nil {
			return true, nil
		} else {
			e2 := metaStore.syncer.UpdateObjTo(currentBusinessObj)
			if e2 != nil {
				logger.Error("Error while rolling back an update of meta '%s': %v", name, e2)
				return false, e

			}
			_, e2 = metaStore.drv.Update(name, *currentBusinessObj.meta)
			if e2 != nil {
				logger.Error("Error while rolling back an update of meta '%s': %v", name, e2)
				return false, e
			}
			return false, nil
		}
	} else {
		return ok, err
	}
}

/*
   Meta driver interface.
*/
type MetaDriver interface {
	List() (*[]*meta, bool, error)
	Get(name string) (*meta, bool, error)
	Create(m meta) error
	Remove(name string) (bool, error)
	Update(name string, m meta) (bool, error)
}

type MetaDbSyncer interface {
	CreateObj(*Meta) error
	RemoveObj(string) error
	UpdateObj(old, new *Meta) error
	UpdateObjTo(*Meta) error
	ValidateObj(*Meta) (bool, error)
}
