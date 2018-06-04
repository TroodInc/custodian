package meta

import (
	"encoding/json"
	"fmt"
	"logger"
	"server/noti"
	"io"
	"sync"
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
type DefConstNum struct{ Value float64 }
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
	return fmt.Sprintf("Meta error:  MetaDescription = '%s', operation = '%s', code='%s'  msg = '%s'", e.meta, e.op, e.code, e.msg)
}

func (e *metaError) Json() []byte {
	j, _ := json.Marshal(map[string]string{
		"MetaDescription": e.meta,
		"op":              e.op,
		"code":            "MetaDescription:" + e.code,
		"msg":             e.msg,
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
	var metaObj MetaDescription
	if e := json.NewDecoder(r).Decode(&metaObj); e != nil {
		return nil, NewMetaError("", "unmarshal", ErrNotValid, e.Error())
	}
	return metaStore.NewMeta(&metaObj)
}

func (metaStore *MetaStore) unmarshalMeta(b []byte) (*Meta, error) {
	var m MetaDescription
	if e := json.Unmarshal(b, &m); e != nil {
		return nil, e
	}
	return metaStore.NewMeta(&m)
}

func (f *FieldDescription) canBeLinkTo(m *Meta) bool {
	return (f.IsSimple() && f.Type == m.Key.Type) || (f.Type == FieldTypeObject && f.LinkMeta.Name == m.Name && f.LinkType == LinkTypeInner)
}

func (metaStore *MetaStore) NewMeta(metaObj *MetaDescription) (*Meta, error) {
	if ok, err := (&ValidationService{}).Validate(metaObj); !ok {
		return nil, err
	}
	createdMeta := &Meta{MetaDescription: metaObj}
	notResolved := []*Meta{createdMeta}
	shadowCache := map[string]*Meta{metaObj.Name: createdMeta}
	for ; len(notResolved) > 0; notResolved = notResolved[1:] {
		bm := notResolved[0]
		fieldsLen := len(bm.MetaDescription.Fields)
		bm.Fields = make([]FieldDescription, fieldsLen, fieldsLen)
		for i := 0; i < fieldsLen; i++ {
			bm.Fields[i].Meta = bm
			field := &bm.MetaDescription.Fields[i]
			bm.Fields[i].Field = field
			if field.LinkMeta != "" {
				var ok bool
				if bm.Fields[i].LinkMeta, ok = shadowCache[field.LinkMeta]; ok {
					continue
				}

				linkedMeta, _, err := metaStore.drv.Get(field.LinkMeta)
				if err != nil {
					return nil, NewMetaError(metaObj.Name, "new_meta", ErrNotValid, "FieldDescription '%s' has iccorect link MetaDescription: %s", field.Name, err.Error())
				}
				bm.Fields[i].LinkMeta = &Meta{MetaDescription: linkedMeta}
				shadowCache[field.LinkMeta] = bm.Fields[i].LinkMeta
				notResolved = append(notResolved, bm.Fields[i].LinkMeta)
			}
		}

		if a, err := newActions(bm.MetaDescription.Actions); err == nil {
			bm.Actions = a
		} else {
			return nil, err
		}

		if bm.Key = bm.FindField(bm.MetaDescription.Key); bm.Key == nil {
			return nil, NewMetaError(metaObj.Name, "new_meta", ErrNotValid, "Meta '%s' is inccorrect. The specified key '%s' Field not found", bm.MetaDescription.Name, bm.MetaDescription.Key)
		} else if !bm.Key.IsSimple() {
			return nil, NewMetaError(metaObj.Name, "new_meta", ErrNotValid, "Meta '%s' is inccorrect. The key Field '%s' is not simple", bm.MetaDescription.Name, bm.MetaDescription.Key)
		}

		if bm.Cas {
			if cas := bm.FindField("cas"); cas != nil {
				if cas.Type != FieldTypeNumber {
					return nil, NewMetaError(metaObj.Name, "new_meta", ErrNotValid, "The filed 'cas' specified in the MetaDescription '%s' as CAS must be type of 'number'", bm.MetaDescription.Cas, bm.MetaDescription.Name)
				}
			} else {
				return nil, NewMetaError(metaObj.Name, "new_meta", ErrNotValid, "Meta '%s' has CAS defined but the filed 'cas' it refers to is absent", bm.MetaDescription.Name, bm.MetaDescription.Cas)
			}
		}
	}

	for _, bm := range shadowCache {
		//processing outer links
		fieldsLen := len(bm.MetaDescription.Fields)
		for i := 0; i < fieldsLen; i++ {
			f := &bm.Fields[i]
			if f.LinkType != LinkTypeOuter {
				continue
			}
			if f.OuterLinkField = f.LinkMeta.FindField(f.Field.OuterLinkField); f.OuterLinkField == nil {
				return nil, NewMetaError(metaObj.Name, "new_meta", ErrNotValid, "Filed '%s' has incorrect outer link. Meta '%s' doesn't have a Field '%s'", f.Name, f.LinkMeta.Name, f.Field.OuterLinkField)
			} else if !f.OuterLinkField.canBeLinkTo(f.Meta) {
				return nil, NewMetaError(metaObj.Name, "new_meta", ErrNotValid, "Filed '%s' has incorrect outer link. FieldDescription '%s' of MetaDescription '%s' can't refer to MetaDescription '%s'", f.Name, f.OuterLinkField.Name, f.OuterLinkField.Meta.Name, f.Meta.Name)
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
func (metaStore *MetaStore) List() (*[]*MetaDescription, bool, error) {
	metaStore.syncerMutex.RLock()
	defer metaStore.syncerMutex.RUnlock()
	metaList, isFound, err := metaStore.drv.List()

	if err != nil {
		return &[]*MetaDescription{}, isFound, err
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
	businessObject, err := metaStore.NewMeta(metaData)
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

	return nil, false, err
}

// Creates a new object type described by passed metadata.
func (metaStore *MetaStore) Create(m *Meta) error {
	metaStore.syncerMutex.Lock()
	defer metaStore.syncerMutex.Unlock()

	if e := metaStore.syncer.CreateObj(m); e == nil {
		if e := metaStore.drv.Create(*m.MetaDescription); e == nil {
			return nil
		} else {
			var e2 = metaStore.syncer.RemoveObj(m.Name, false)
			logger.Error("Error while compenstaion of object '%s' metadata creation: %v", m.Name, e2)
			return e
		}
	} else {
		return e
	}
}

// Deletes an existing object metadata from the store.
func (metaStore *MetaStore) Remove(name string, force bool) (bool, error) {
	//remove object from the database
	meta, _, _ := metaStore.Get(name)
	metaStore.removeRelatedOuterLinks(meta)
	metaStore.removeRelatedInnerLinks(meta)
	if e := metaStore.syncer.RemoveObj(name, force); e == nil {
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

//Remove all outer fields linking to given Meta
func (metaStore *MetaStore) removeRelatedOuterLinks(targetMeta *Meta) {
	for _, field := range targetMeta.Fields {
		if field.Type == FieldTypeObject && field.LinkType == LinkTypeInner {
			metaStore.removeRelatedOuterLink(targetMeta, field)
		}
	}
}

//Remove outer field from related object if it links to the given field
func (metaStore *MetaStore) removeRelatedOuterLink(targetMeta *Meta, innerLinkFieldDescription FieldDescription) {
	relatedObjectMeta := innerLinkFieldDescription.LinkMeta
	for i, relatedObjectField := range relatedObjectMeta.Fields {
		if relatedObjectField.LinkType == LinkTypeOuter &&
			relatedObjectField.LinkMeta.Name == targetMeta.Name &&
			relatedObjectField.OuterLinkField.Field.Name == innerLinkFieldDescription.Field.Name {
			//omit outer field and update related object
			relatedObjectMeta.Fields = append(relatedObjectMeta.Fields[:i], relatedObjectMeta.Fields[i+1:]...)
			relatedObjectMeta.MetaDescription.Fields = append(relatedObjectMeta.MetaDescription.Fields[:i], relatedObjectMeta.MetaDescription.Fields[i+1:]...)
			metaStore.Update(relatedObjectMeta.Name, relatedObjectMeta)
		}
	}
}

//Remove inner fields linking to given Meta
func (metaStore *MetaStore) removeRelatedInnerLinks(targetMeta *Meta) {
	metaDescriptionList, _, _ := metaStore.List()
	for _, objectMetaDescription := range *metaDescriptionList {

		if targetMeta.Name != objectMetaDescription.Name {
			objectMeta, _, _ := metaStore.Get(objectMetaDescription.Name)
			objectMetaFields := make([]Field, 0)
			objectMetaFieldDescriptions := make([]FieldDescription, 0)

			for i, fieldDescription := range objectMeta.Fields {
				if fieldDescription.LinkType != LinkTypeInner || fieldDescription.LinkMeta.Name != targetMeta.Name {
					objectMetaFields = append(objectMetaFields, objectMeta.MetaDescription.Fields[i])
					objectMetaFieldDescriptions = append(objectMetaFieldDescriptions, objectMeta.Fields[i])
				}
			}
			// it means that related object should be updated
			if len(objectMetaFieldDescriptions) != len(objectMeta.Fields) {
				objectMeta.Fields = objectMetaFieldDescriptions
				objectMeta.MetaDescription.Fields = objectMetaFields
				metaStore.Update(objectMeta.Name, objectMeta)
			}
		}
	}
}

// Updates an existing object metadata.
func (metaStore *MetaStore) Update(name string, newBusinessObj *Meta) (bool, error) {
	if currentBusinessObj, ok, err := metaStore.Get(name); err == nil {
		// remove possible outer links before main update processing
		metaStore.processInnerLinksRemoval(currentBusinessObj, newBusinessObj)
		metaStore.syncerMutex.Lock()
		defer metaStore.syncerMutex.Unlock()
		ok, e := metaStore.drv.Update(name, *newBusinessObj.MetaDescription)
		metaStore.cacheMutex.Lock()
		delete(metaStore.cache, name)
		metaStore.cacheMutex.Unlock()

		if e != nil || !ok {
			return ok, e
		}

		//TODO: This logic tells NOTHING about error if it was successfully rolled back. This
		//behaviour should be fixed
		if updateError := metaStore.syncer.UpdateObj(currentBusinessObj, newBusinessObj); updateError == nil {
			return true, nil
		} else {
			//rollback to the previous version
			rollbackError := metaStore.syncer.UpdateObjTo(currentBusinessObj)
			if rollbackError != nil {
				logger.Error("Error while rolling back an update of MetaDescription '%s': %v", name, rollbackError)
				return false, updateError

			}
			_, rollbackError = metaStore.drv.Update(name, *currentBusinessObj.MetaDescription)
			if rollbackError != nil {
				logger.Error("Error while rolling back an update of MetaDescription '%s': %v", name, rollbackError)
				return false, updateError
			}
			return false, updateError
		}
	} else {
		return ok, err
	}
}

// compare current object`s version to the version is being updated and remove outer links
// if any inner link is being removed
func (metaStore *MetaStore) processInnerLinksRemoval(currentMeta *Meta, metaToBeUpdated *Meta) {
	for _, currentFieldDescription := range currentMeta.Fields {
		if currentFieldDescription.LinkType == LinkTypeInner {
			fieldIsBeingRemoved := true
			for _, fieldDescriptionToBeUpdated := range metaToBeUpdated.Fields {
				if fieldDescriptionToBeUpdated.Name == fieldDescriptionToBeUpdated.Name &&
					fieldDescriptionToBeUpdated.LinkType == LinkTypeInner &&
					fieldDescriptionToBeUpdated.LinkMeta.Name == fieldDescriptionToBeUpdated.LinkMeta.Name {
					fieldIsBeingRemoved = false
				}
			}
			if fieldIsBeingRemoved {
				metaStore.removeRelatedOuterLink(currentMeta, currentFieldDescription)
			}
		}
	}
}

// Updates an existing object metadata.
func (metaStore *MetaStore) Flush() error {
	metaList, _, _ := metaStore.List()
	for _, meta := range *metaList {
		metaStore.Remove(meta.Name, true)
	}
	return nil
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
}

type MetaDbSyncer interface {
	CreateObj(*Meta) error
	RemoveObj(string, bool) error
	UpdateObj(old, new *Meta) error
	UpdateObjTo(*Meta) error
	ValidateObj(*Meta) (bool, error)
}
