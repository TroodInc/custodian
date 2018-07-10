package data

import (
	"encoding/json"
	"fmt"

	"server/meta"
	"github.com/Q-CIS-DEV/go-rql-parser"
	"strings"
	"server/auth"
)

const (
	ErrDataInternal         = "internal_data_error"
	ErrObjectClassNotFound  = "object_class_not_found"
	ErrMandatoryFiledAbsent = "mandatory_filed_absent"
	ErrWrongFiledType       = "wrong_field_type"
	ErrWrongRQL             = "wrong_rql"
	ErrKeyValueNotFound     = "key_value_not_found"
	ErrCasFailed            = "cas_failed"
)

type DataError struct {
	Code        string
	Msg         string
	objectClass string
}

func (e *DataError) Error() string {
	return fmt.Sprintf("Data error:  object class = '%s', code='%s'  msg = '%s'", e.objectClass, e.Code, e.Msg)
}

func (e *DataError) Json() []byte {
	j, _ := json.Marshal(map[string]string{
		"objectClass": e.objectClass,
		"code":        "table:" + e.Code,
		"msg":         e.Msg,
	})
	return j
}

func NewDataError(objectClass string, code string, msg string, a ...interface{}) *DataError {
	return &DataError{objectClass: objectClass, Code: code, Msg: fmt.Sprintf(msg, a...)}
}

type objectClassValidator func(Tuple2) ([]Tuple2, error)

type OperationContext interface{}
type Operation func(ctx OperationContext) error
type ExecuteContext interface {
	Execute(operations []Operation) error
	Complete() error
	Close() error
}

type DataManager interface {
	GetRql(dataNode *Node, rqlNode *rqlParser.RqlRootNode, fields []*meta.FieldDescription) ([]map[string]interface{}, error)
	GetIn(m *meta.Meta, fields []*meta.FieldDescription, key string, in []interface{}) ([]map[string]interface{}, error)
	Get(m *meta.Meta, fields []*meta.FieldDescription, key string, val interface{}) (map[string]interface{}, error)
	GetAll(m *meta.Meta, fileds []*meta.FieldDescription, key string, val interface{}) ([]map[string]interface{}, error)
	PrepareDeletes(n *DNode, keys []interface{}) (Operation, []interface{}, error)
	PreparePuts(m *meta.Meta, objs []map[string]interface{}) (Operation, error)
	PrepareUpdates(m *meta.Meta, objs []map[string]interface{}) (Operation, error)
	Execute(operations []Operation) error
	ExecuteContext() (ExecuteContext, error)
}

type Processor struct {
	metaStore   *meta.MetaStore
	dataManager DataManager
	vCache      map[string]objectClassValidator
}

func NewProcessor(m *meta.MetaStore, d DataManager) (*Processor, error) {
	return &Processor{metaStore: m, dataManager: d, vCache: make(map[string]objectClassValidator)}, nil
}

type Tuple2 struct {
	First  *meta.Meta
	Second map[string]interface{}
}

type ALink struct {
	Field *meta.FieldDescription
	outer bool
	Obj   map[string]interface{}
}

type DLink struct {
	Field *meta.FieldDescription
	outer bool
	Id    interface{}
}

func AssertLink(i interface{}) bool {
	switch i.(type) {
	case DLink:
		return true
	case ALink:
		return true
	default:
		return false
	}
}

func (processor *Processor) validate(t2 *Tuple2, mandatoryCheck bool) ([]Tuple2, error) {
	toCheck := make([]Tuple2, 0)
	for k, _ := range t2.Second {
		if f := t2.First.FindField(k); f == nil {
			delete(t2.Second, k)
		}
	}

	for i := 0; i < len(t2.First.Fields); i++ {
		k := t2.First.Fields[i].Name
		fieldDescription := &t2.First.Fields[i]

		value, valueIsSet := t2.Second[k]
		if mandatoryCheck && !valueIsSet && !fieldDescription.Optional {
			return nil, NewDataError(t2.First.Name, ErrMandatoryFiledAbsent, "Not optional field '%s' is absent", k)
		}
		//skip validation if field is optional and value is null
		//perform validation otherwise
		if valueIsSet && !(value == nil && fieldDescription.Optional) {
			switch {
			case fieldDescription.Type == meta.FieldTypeString && meta.FieldTypeNumber.AssertType(value):
				break
			case fieldDescription.Type.AssertType(value):
				if fieldDescription.Type == meta.FieldTypeArray {
					var a = value.([]interface{})
					for _, av := range a {
						if m, ok := av.(map[string]interface{}); ok {
							m[fieldDescription.OuterLinkField.Name] = ALink{Field: fieldDescription, outer: true, Obj: t2.Second}
							toCheck = append(toCheck, Tuple2{fieldDescription.LinkMeta, m})
						} else {
							return nil, NewDataError(t2.First.Name, ErrWrongFiledType, "Array in field '%s' must contain only JSON object", k)
						}
					}
					delete(t2.Second, k)
				} else if fieldDescription.Type == meta.FieldTypeObject {
					var of = value.(map[string]interface{})
					if fieldDescription.LinkType == meta.LinkTypeOuter {
						of[fieldDescription.OuterLinkField.Name] = ALink{Field: fieldDescription, outer: true, Obj: t2.Second}
						delete(t2.Second, k)
					} else if fieldDescription.LinkType == meta.LinkTypeInner {
						t2.Second[fieldDescription.Name] = ALink{Field: fieldDescription.LinkMeta.Key, outer: false, Obj: of}
					} else {
						return nil, NewDataError(t2.First.Name, ErrWrongFiledType, "Unknown link type %s", fieldDescription.LinkType)
					}
					toCheck = append(toCheck, Tuple2{fieldDescription.LinkMeta, of})
				} else if fieldDescription.IsSimple() && fieldDescription.LinkType == meta.LinkTypeInner {
					t2.Second[fieldDescription.Name] = DLink{Field: fieldDescription.LinkMeta.Key, outer: false, Id: value}
				}
			case fieldDescription.LinkType == meta.LinkTypeInner && fieldDescription.LinkMeta.Key.Type.AssertType(value):
				t2.Second[fieldDescription.Name] = DLink{Field: fieldDescription.LinkMeta.Key, outer: false, Id: value}
			case fieldDescription.LinkType == meta.LinkTypeInner && AssertLink(value):
			default:
				return nil, NewDataError(t2.First.Name, ErrWrongFiledType, "Field '%s' has a wrong type", k)
			}

		}
	}
	return toCheck, nil
}

func (processor *Processor) getValidator(vk string, preValidator func(pt2 *Tuple2) (*Tuple2, bool, error)) (objectClassValidator, error) {
	if v, ex := processor.vCache[vk]; ex {
		return v, nil
	}
	validator := func(t2 Tuple2) ([]Tuple2, error) {
		preValidatedT2, mandatoryCheck, err := preValidator(&t2)
		if err != nil {
			return nil, err
		}
		if toCheck, e := processor.validate(preValidatedT2, mandatoryCheck); e != nil {
			return nil, e
		} else {
			return toCheck, nil
		}
	}
	processor.vCache[vk] = validator
	return validator, nil

}

func (processor *Processor) flatten(objectMeta *meta.Meta, recordValues map[string]interface{}, validatorFactory func(mn string) (objectClassValidator, error)) ([]Tuple2, error) {
	tc := []Tuple2{{objectMeta, recordValues}}
	for tail := tc; len(tail) > 0; tail = tail[1:] {
		if v, e := validatorFactory(tail[0].First.Name); e != nil {
			return nil, e
		} else if t, e := v(tail[0]); e != nil {
			return nil, e
		} else {
			tc = append(tc, t...)
			tail = append(tail, t...)
		}
	}
	return tc, nil
}

type Tuple2a struct {
	First  *meta.Meta
	Second []map[string]interface{}
}

func (processor *Processor) spreadByLevelLader(m *meta.Meta, objs []map[string]interface{}, validatorFactory func(mn string) (objectClassValidator, error)) ([][]*Tuple2a, error) {
	var levelLader = [][]*Tuple2a{[]*Tuple2a{&Tuple2a{m, objs}}}

	for curLevel := levelLader[0]; curLevel != nil; {
		next := make(map[string]*Tuple2a)
		for tail := curLevel; len(tail) > 0; tail = tail[1:] {
			v, e := validatorFactory(tail[0].First.Name)
			if e != nil {
				return nil, e
			}
			for _, o := range tail[0].Second {
				t, e := v(Tuple2{tail[0].First, o})
				if e != nil {
					return nil, e
				}
				for _, t2 := range t {
					if pt2a, ok := next[t2.First.Name]; ok {
						pt2a.Second = append(pt2a.Second, t2.Second)
					} else {
						next[t2.First.Name] = &Tuple2a{t2.First, []map[string]interface{}{t2.Second}}
					}
				}
			}
		}
		if len(next) > 0 {
			nextLevel := make([]*Tuple2a, 0, len(next))
			for _, pt2a := range next {
				nextLevel = append(nextLevel, pt2a)
			}
			levelLader = append(levelLader, nextLevel)
			curLevel = nextLevel
		} else {
			curLevel = nil
		}
	}
	return levelLader, nil
}

func collapseLinks(obj map[string]interface{}) {
	for k, v := range obj {
		switch l := v.(type) {
		case ALink:
			if l.outer {
				if l.Field.Type == meta.FieldTypeArray {
					if a, prs := l.Obj[l.Field.Name]; !prs || a == nil {
						l.Obj[l.Field.Name] = []interface{}{obj}
					} else {
						l.Obj[l.Field.Name] = append(a.([]interface{}), obj)
					}
				} else if l.Field.Type == meta.FieldTypeObject {
					l.Obj[l.Field.Name] = obj
				}
				delete(obj, k)
			} else {
				obj[k] = l.Obj
			}
		case DLink:
			if !l.outer {
				obj[k] = l.Id
			}
		}
	}
}

func putValidator(t *Tuple2) (*Tuple2, bool, error) {
	t.Second["cas"] = 1.0
	return t, true, nil
}

func (processor *Processor) Put(objectClass string, obj map[string]interface{}, user auth.User) (retObj map[string]interface{}, err error) {
	m, ok, e := processor.metaStore.Get(objectClass, true)
	if e != nil {
		return nil, e
	}
	if !ok {
		return nil, NewDataError(objectClass, ErrObjectClassNotFound, "Object class '%s' not found", objectClass)
	}

	tc, e := processor.flatten(m, obj, func(mn string) (objectClassValidator, error) {
		return processor.getValidator("put:"+mn, putValidator)
	})
	if e != nil {
		return nil, e
	}

	var ops = make([]Operation, 0)
	for _, t := range tc {
		if op, e := processor.dataManager.PreparePuts(t.First, []map[string]interface{}{t.Second}); e != nil {
			return nil, e
		} else {
			ops = append(ops, op)
		}
	}

	if e := processor.dataManager.Execute(ops); e != nil {
		return nil, e
	}

	//process notifications
	notificationSender := newNotificationSender(meta.MethodCreate)
	defer func() {
		notificationSender.complete(err)
	}()
	for i, _ := range tc {
		collapseLinks(tc[i].Second)
		notificationSender.push(NOTIFICATION_CREATE, tc[i].First, tc[i].Second, user, i == 0)
	}

	return obj, nil
}

func (processor *Processor) PutBulk(objectClass string, next func() (map[string]interface{}, error), sink func(map[string]interface{}) error, user auth.User) (response error) {
	m, ok, e := processor.metaStore.Get(objectClass, true)
	if e != nil {
		return e
	}
	if !ok {
		return NewDataError(objectClass, ErrObjectClassNotFound, "Object class '%s' not found", objectClass)
	}

	exCtx, e := processor.dataManager.ExecuteContext()
	if e != nil {
		return e
	}
	defer exCtx.Close()

	var buf = make([]map[string]interface{}, 0, 100)
	notification := newNotificationSender(meta.MethodCreate)
	defer func() {
		notification.complete(response)
	}()
	for {
		for o, e := next(); e != nil || (o != nil && len(buf) < 100); o, e = next() {
			if e != nil {
				return e
			}
			buf = append(buf, o)
		}

		if len(buf) > 0 {
			levelLader, e := processor.spreadByLevelLader(m, buf, func(mn string) (objectClassValidator, error) {
				return processor.getValidator("put:"+mn, putValidator)
			})
			if e != nil {
				return e
			}

			for levelIdx, level := range levelLader {
				isRoot := levelIdx == 0
				for _, item := range level {
					op, e := processor.dataManager.PreparePuts(item.First, item.Second)
					if e != nil {
						return e
					}

					if e := exCtx.Execute([]Operation{op}); e != nil {
						return e
					}

					for _, recordData := range item.Second {
						collapseLinks(recordData)
						notification.push(NOTIFICATION_CREATE, item.First, recordData, user, isRoot)
					}
				}
			}
			for _, roots := range levelLader[0] {
				for _, root := range roots.Second {
					if e := sink(root); e != nil {
						return e
					}
				}
			}
		}

		if len(buf) < 100 {
			if e := exCtx.Complete(); e != nil {
				return e
			} else {
				return nil
			}
		} else {
			buf = buf[:0]
		}
	}
}

type SearchContext struct {
	depthLimit int
	dm         DataManager
	lazyPath   string
}

func isBackLink(m *meta.Meta, f *meta.FieldDescription) bool {
	for i, _ := range m.Fields {
		if m.Fields[i].LinkType == meta.LinkTypeOuter && m.Fields[i].OuterLinkField.Name == f.Name && m.Fields[i].LinkMeta.Name == f.Meta.Name {
			return true
		}
	}
	return false
}

func (processor *Processor) Get(objectClass, key string, depth int) (map[string]interface{}, error) {
	if m, ok, e := processor.metaStore.Get(objectClass, true); e != nil {
		return nil, e
	} else if !ok {
		return nil, NewDataError(objectClass, ErrObjectClassNotFound, "Object class '%s' not found", objectClass)
	} else {
		if pk, e := m.Key.ValueFromString(key); e != nil {
			return nil, e
		} else {
			ctx := SearchContext{depthLimit: depth, dm: processor.dataManager, lazyPath: "/custodian/data/single"}

			root := &Node{KeyField: m.Key, Meta: m, ChildNodes: make(map[string]*Node), Depth: 1, OnlyLink: false, plural: false, Parent: nil}
			root.RecursivelyFillChildNodes(ctx.depthLimit)

			if o, e := root.Resolve(ctx, pk); e != nil {
				return nil, e
			} else if o == nil {
				return nil, nil
			} else {
				obj := o.(map[string]interface{})
				for nodeResults := []NodeResult{{root, obj}}; len(nodeResults) > 0; nodeResults = nodeResults[1:] {
					if childNodesResults, e := nodeResults[0].getFilledChildNodes(ctx); e != nil {
						return nil, e
					} else {
						nodeResults = append(nodeResults, childNodesResults...)
					}
				}
				return obj, nil
			}
		}
	}
}

func (processor *Processor) GetBulk(objectName, filter string, depth int, sink func(map[string]interface{}) error) error {
	if businessObject, ok, e := processor.metaStore.Get(objectName, true); e != nil {
		return e
	} else if !ok {
		return NewDataError(objectName, ErrObjectClassNotFound, "Object class '%s' not found", objectName)
	} else {
		searchContext := SearchContext{depthLimit: depth, dm: processor.dataManager, lazyPath: "/custodian/data/bulk"}
		root := &Node{
			KeyField:   businessObject.Key,
			Meta:       businessObject,
			ChildNodes: make(map[string]*Node),
			Depth:      1,
			OnlyLink:   false,
			plural:     false,
			Parent:     nil,
		}
		root.RecursivelyFillChildNodes(searchContext.depthLimit)

		parser := rqlParser.NewParser()
		rqlNode, err := parser.Parse(strings.NewReader(filter))
		if err != nil {
			return NewDataError(objectName, ErrWrongRQL, err.Error())
		}

		records, e := root.ResolveByRql(searchContext, rqlNode)

		if e != nil {
			return e
		}
		for _, record := range records {
			root.FillRecordValues(&record, searchContext)
			sink(record)
		}
		return nil
	}
}

type DNode struct {
	KeyField   *meta.FieldDescription
	Meta       *meta.Meta
	ChildNodes map[string]*DNode
	Plural     bool
}

func (dn *DNode) fillOuterChildNodes() {
	for _, f := range dn.Meta.Fields {
		if f.LinkType == meta.LinkTypeOuter {
			var plural = false
			if f.Type == meta.FieldTypeArray {
				plural = true
			}
			dn.ChildNodes[f.Name] = &DNode{KeyField: f.OuterLinkField,
				Meta: f.LinkMeta,
				ChildNodes: make(map[string]*DNode),
				Plural: plural}
		}
	}
}

type tuple2d struct {
	n    *DNode
	keys []interface{}
}

func (processor *Processor) Delete(objectClass, key string, user auth.User) (isDeleted bool, err error) {
	if m, ok, e := processor.metaStore.Get(objectClass, true); e != nil {
		return false, e
	} else if !ok {
		return false, NewDataError(objectClass, ErrObjectClassNotFound, "Object class '%s' not found", objectClass)
	} else {
		if pk, e := m.Key.ValueFromString(key); e != nil {
			return false, e
		} else {
			root := &DNode{KeyField: m.Key, Meta: m, ChildNodes: make(map[string]*DNode), Plural: false}
			root.fillOuterChildNodes()
			for v := []map[string]*DNode{root.ChildNodes}; len(v) > 0; v = v[1:] {
				for _, n := range v[0] {
					n.fillOuterChildNodes()
					if len(n.ChildNodes) > 0 {
						v = append(v, n.ChildNodes)
					}
				}
			}
			//process notifications
			notificationSender := newNotificationSender(meta.MethodRemove)
			defer func() {
				notificationSender.complete(err)
			}()
			if op, keys, e := processor.dataManager.PrepareDeletes(root, []interface{}{pk}); e != nil {
				return false, e
			} else {
				//process root records notificationSender
				notificationSender.push(NOTIFICATION_DELETE, root.Meta, map[string]interface{}{root.KeyField.Name: pk}, user, true)

				ops := []Operation{op}
				for t2d := []tuple2d{tuple2d{root, keys}}; len(t2d) > 0; t2d = t2d[1:] {
					for _, v := range t2d[0].n.ChildNodes {
						if op, keys, e := processor.dataManager.PrepareDeletes(v, t2d[0].keys); e != nil {
							return false, e
						} else {
							ops = append(ops, op)
							t2d = append(t2d, tuple2d{v, keys})

							//process affected records notifications
							for _, primaryKeyValue := range t2d[0].keys {
								notificationSender.push(NOTIFICATION_DELETE, v.Meta, map[string]interface{}{v.KeyField.Name: primaryKeyValue}, user, false)
							}

						}
					}
				}
				for i := 0; i < len(ops)>>2; i++ {
					ops[i], ops[len(ops)-1] = ops[len(ops)-1], ops[i]
				}
				if e := processor.dataManager.Execute(ops); e != nil {
					return false, e
				} else {
					return true, nil
				}
			}

		}
	}
}

func (processor *Processor) DeleteBulk(objectClass string, next func() (map[string]interface{}, error), user auth.User) (err error) {
	if m, ok, e := processor.metaStore.Get(objectClass, true); e != nil {
		return e
	} else if !ok {
		return NewDataError(objectClass, ErrObjectClassNotFound, "Object class '%s' not found", objectClass)
	} else {
		ts := m.Key.Type.TypeAsserter()

		root := &DNode{KeyField: m.Key, Meta: m, ChildNodes: make(map[string]*DNode), Plural: false}
		root.fillOuterChildNodes()
		for v := []map[string]*DNode{root.ChildNodes}; len(v) > 0; v = v[1:] {
			for _, n := range v[0] {
				n.fillOuterChildNodes()
				if len(n.ChildNodes) > 0 {
					v = append(v, n.ChildNodes)
				}
			}
		}

		exCtx, e := processor.dataManager.ExecuteContext()
		if e != nil {
			return e
		}
		defer exCtx.Close()
		var buf = make([]interface{}, 0, 100)
		notificationSender := newNotificationSender(meta.MethodRemove)
		defer func() {
			notificationSender.complete(err)
		}()
		for {
			for o, e := next(); e != nil || (o != nil && len(buf) < 100); o, e = next() {
				if e != nil {
					return e
				}
				k, ok := o[m.Key.Name]
				if !ok || !ts(k) {
					return NewDataError(objectClass, ErrKeyValueNotFound, "Key value not found or has a wrong type", objectClass)
				}
				buf = append(buf, k)
			}

			if len(buf) > 0 {
				if op, keys, e := processor.dataManager.PrepareDeletes(root, buf); e != nil {
					return e
				} else {
					ops := []Operation{op}
					for t2d := []tuple2d{tuple2d{root, keys}}; len(t2d) > 0; t2d = t2d[1:] {
						for _, v := range t2d[0].n.ChildNodes {
							if len(t2d[0].keys) > 0 {
								if op, keys, e := processor.dataManager.PrepareDeletes(v, t2d[0].keys); e != nil {
									return e
								} else {
									for _, primaryKeyValue := range t2d[0].keys {
										notificationSender.push(NOTIFICATION_DELETE, v.Meta, map[string]interface{}{v.KeyField.Name: primaryKeyValue}, user, false)
									}

									ops = append(ops, op)
									t2d = append(t2d, tuple2d{v, keys})
								}
							}
						}
					}
					for i := 0; i < len(ops)>>2; i++ {
						ops[i], ops[len(ops)-1] = ops[len(ops)-1], ops[i]
					}
					if e := processor.dataManager.Execute(ops); e != nil {
						return e
					}
				}
			}

			if len(buf) < 100 {
				if e := exCtx.Complete(); e != nil {
					return e
				} else {
					return nil
				}
			} else {
				buf = buf[:0]
			}
		}
	}
}

func updateValidator(t *Tuple2) (*Tuple2, bool, error) {
	return t, false, nil
}

func (processor *Processor) Update(objectClass, key string, obj map[string]interface{}, user auth.User) (retObj map[string]interface{}, err error) {
	m, ok, e := processor.metaStore.Get(objectClass, true)
	if e != nil {
		return nil, e
	}
	if !ok {
		return nil, NewDataError(objectClass, ErrObjectClassNotFound, "Object class '%s' not found", objectClass)
	}

	if pkValue, e := m.Key.ValueFromString(key); e != nil {
		return nil, e
	} else {
		//record data must contain valid record`s PK value
		obj[m.Key.Name] = pkValue
	}

	tc, e := processor.flatten(m, obj, func(mn string) (objectClassValidator, error) {
		return processor.getValidator("upd:"+mn, updateValidator)
	})
	if e != nil {
		return nil, e
	}

	var ops = make([]Operation, 0)

	//process notifications
	notificationSender := newNotificationSender(meta.MethodUpdate)
	defer func() {
		notificationSender.complete(err)
	}()
	for i, t := range tc {
		if op, e := processor.dataManager.PrepareUpdates(t.First, []map[string]interface{}{t.Second}); e != nil {
			return nil, e
		} else {
			notificationSender.push(NOTIFICATION_UPDATE, t.First, t.Second, user, i == 0)
			ops = append(ops, op)
		}
	}

	if e := processor.dataManager.Execute(ops); e != nil {
		return nil, e
	}

	for ; len(tc) > 0; tc = tc[1:] {
		collapseLinks(tc[0].Second)
	}
	return obj, nil
}

func (processor *Processor) UpdateBulk(objectClass string, next func() (map[string]interface{}, error), sink func(map[string]interface{}) error, user auth.User) (err error) {
	m, ok, e := processor.metaStore.Get(objectClass, true)
	if e != nil {
		return e
	}
	if !ok {
		return NewDataError(objectClass, ErrObjectClassNotFound, "Object class '%s' not found", objectClass)
	}

	exCtx, e := processor.dataManager.ExecuteContext()
	if e != nil {
		return e
	}
	defer func() {
		exCtx.Close()
	}()

	var buf = make([]map[string]interface{}, 0, 100)
	notificationSender := newNotificationSender(meta.MethodUpdate)
	defer func() {
		notificationSender.complete(err)
	}()
	for {
		for o, e := next(); e != nil || (o != nil && len(buf) < 100); o, e = next() {
			if e != nil {
				return e
			}
			buf = append(buf, o)
		}

		if len(buf) > 0 {
			levelLader, e := processor.spreadByLevelLader(m, buf, func(mn string) (objectClassValidator, error) {
				return processor.getValidator("upd:"+mn, updateValidator)
			})
			if e != nil {
				return e
			}

			for levelIdx, level := range levelLader {
				isRoot := levelIdx == 0
				for _, item := range level {
					op, e := processor.dataManager.PrepareUpdates(item.First, item.Second)
					if e != nil {
						return e
					}

					if e := exCtx.Execute([]Operation{op}); e != nil {
						return e
					}

					for _, recordData := range item.Second {
						collapseLinks(recordData)
						notificationSender.push(NOTIFICATION_UPDATE, item.First, recordData, user, isRoot)
					}

				}
			}
			for _, roots := range levelLader[0] {
				for _, root := range roots.Second {
					if e := sink(root); e != nil {
						return e
					}
				}
			}
		}

		if len(buf) < 100 {
			if e := exCtx.Complete(); e != nil {
				return e
			} else {
				return nil
			}
		} else {
			buf = buf[:0]
		}
	}
}
