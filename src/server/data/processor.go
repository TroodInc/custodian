package data

import (
	"encoding/json"
	"fmt"

	"logger"
	"server/meta"
	"server/noti"
	"github.com/WhackoJacko/go-rql-parser"
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
		v := &t2.First.Fields[i]

		f, ex := t2.Second[k]
		if mandatoryCheck && !ex && !v.Optional {
			return nil, NewDataError(t2.First.Name, ErrMandatoryFiledAbsent, "Not optional field '%s' is absent", k)
		}

		if ex {
			switch {
			case v.Type.AssertType(f):
				if v.Type == meta.FieldTypeArray {
					var a = f.([]interface{})
					for _, av := range a {
						if m, ok := av.(map[string]interface{}); ok {
							m[v.OuterLinkField.Name] = ALink{Field: v, outer: true, Obj: t2.Second}
							toCheck = append(toCheck, Tuple2{v.LinkMeta, m})
						} else {
							return nil, NewDataError(t2.First.Name, ErrWrongFiledType, "Array in field '%s' must contain only JSON object", k)
						}
					}
					delete(t2.Second, k)
				} else if v.Type == meta.FieldTypeObject {
					var of = f.(map[string]interface{})
					if v.LinkType == meta.LinkTypeOuter {
						of[v.OuterLinkField.Name] = ALink{Field: v, outer: true, Obj: t2.Second}
						delete(t2.Second, k)
					} else if v.LinkType == meta.LinkTypeInner {
						t2.Second[v.Name] = ALink{Field: v.LinkMeta.Key, outer: false, Obj: of}
					} else {
						return nil, NewDataError(t2.First.Name, ErrWrongFiledType, "Unknown link type %s", v.LinkType)
					}
					toCheck = append(toCheck, Tuple2{v.LinkMeta, of})
				} else if v.IsSimple() && v.LinkType == meta.LinkTypeInner {
					t2.Second[v.Name] = DLink{Field: v.LinkMeta.Key, outer: false, Id: f}
				}
			case v.LinkType == meta.LinkTypeInner && v.LinkMeta.Key.Type.AssertType(f):
				t2.Second[v.Name] = DLink{Field: v.LinkMeta.Key, outer: false, Id: f}
			case v.LinkType == meta.LinkTypeInner && AssertLink(f):
			default:
				return nil, NewDataError(t2.First.Name, ErrWrongFiledType, "Filed '%s' has a wrong type", k)
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

func (processor *Processor) flatten(m *meta.Meta, obj map[string]interface{}, validatorFactory func(mn string) (objectClassValidator, error)) ([]Tuple2, error) {
	tc := []Tuple2{Tuple2{m, obj}}
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

type notification struct {
	method meta.Method
	notifs map[string]chan *noti.Event
}

func newNotification(m meta.Method) *notification {
	return &notification{method: m, notifs: make(map[string]chan *noti.Event)}
}

func (n *notification) push(m *meta.Meta, obj map[string]interface{}, isRoot bool) {
	notifchan, ok := n.notifs[m.Name]
	if !ok {
		notifchan = m.Actions.StartNotification(n.method)
		n.notifs[m.Name] = notifchan
	}
	notifchan <- noti.NewObjectEvent(obj, isRoot)
}

func (n *notification) pushAll(m *meta.Meta, objs []map[string]interface{}, isRoot bool) {
	notifchan, ok := n.notifs[m.Name]
	if !ok {
		notifchan = m.Actions.StartNotification(n.method)
		n.notifs[m.Name] = notifchan
	}
	for i, _ := range objs {
		notifchan <- noti.NewObjectEvent(objs[i], isRoot)
	}
}

func (n *notification) complete(err error) {
	if err == nil {
		n.close()
	} else {
		n.failed(err)
	}
}

func (n *notification) close() {
	for _, c := range n.notifs {
		close(c)
	}
}

func (n *notification) failed(err error) {
	for _, c := range n.notifs {
		c <- noti.NewErrorEvent(err)
		close(c)
	}
}

func (processor *Processor) Put(objectClass string, obj map[string]interface{}, actor auth.User) (retObj map[string]interface{}, err error) {
	m, ok, e := processor.metaStore.Get(objectClass)
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
	n := newNotification(meta.MethodCreate)
	defer func() {
		n.complete(err)
	}()
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

	for i, _ := range tc {
		collapseLinks(tc[i].Second)

		notificaion_data := make(map[string]interface{})

		notificaion_data["actor"] = actor
		notificaion_data["data"] = tc[i].Second

		n.push(tc[i].First, notificaion_data, i == 0)
	}

	return obj, nil
}

func (processor *Processor) PutBulk(objectClass string, next func() (map[string]interface{}, error), sink func(map[string]interface{}) error) (response error) {
	m, ok, e := processor.metaStore.Get(objectClass)
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
	n := newNotification(meta.MethodCreate)
	defer func() {
		n.complete(response)
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

					for _, obj := range item.Second {
						collapseLinks(obj)
					}
					n.pushAll(item.First, item.Second, isRoot)
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

type Node struct {
	LinkField *meta.FieldDescription
	KeyFiled  *meta.FieldDescription
	Meta      *meta.Meta
	Branches  map[string]*Node
	Depth     int
	OnlyLink  bool
	plural    bool
	Parent    *Node
}

func (n *Node) keyAsString(obj map[string]interface{}) (string, error) {
	v := obj[n.Meta.Key.Name]
	str, err := n.Meta.Key.ValueAsString(v)
	return str, err
}

func (n *Node) ResolveByRql(sc SearchContext, rqlNode *rqlParser.RqlRootNode) ([]map[string]interface{}, error) {
	return sc.dm.GetRql(n, rqlNode, nil)
}

func (n *Node) Resolve2(sc SearchContext, keys []interface{}) (map[interface{}]interface{}, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	var fields []*meta.FieldDescription = nil
	if n.OnlyLink {
		fields = []*meta.FieldDescription{n.Meta.Key, n.KeyFiled}
	}

	objs, err := sc.dm.GetIn(n.Meta, fields, n.KeyFiled.Name, keys)
	if err != nil {
		return nil, err
	}

	res := make(map[interface{}]interface{})
	for i := range objs {
		if n.OnlyLink {
			keyStr, err := n.keyAsString(objs[i])
			if err != nil {
				return nil, err
			}
			res[objs[i][n.KeyFiled.Name]] = keyStr
		} else {
			res[objs[i][n.KeyFiled.Name]] = objs[i]
		}
	}

	return res, nil
}

func (n *Node) ResolvePlural2(sc SearchContext, keys []interface{}) (map[interface{}][]interface{}, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	var fields []*meta.FieldDescription = nil
	if n.OnlyLink {
		fields = []*meta.FieldDescription{n.Meta.Key, n.KeyFiled}
	}

	objs, err := sc.dm.GetIn(n.Meta, fields, n.KeyFiled.Name, keys)
	if err != nil {
		return nil, err
	}

	res := make(map[interface{}][]interface{})
	for i := range objs {
		key := objs[i][n.KeyFiled.Name]
		arr, ok := res[key]
		if !ok {
			arr = make([]interface{}, 0)
		}
		if n.OnlyLink {
			keyStr, err := n.keyAsString(objs[i])
			if err != nil {
				return nil, err
			}
			res[key] = append(arr, fmt.Sprint(sc.lazyPath, "/", n.Meta.Name, "/", keyStr))
		} else {
			res[key] = append(arr, objs[i])
		}
	}

	return res, nil
}

func (n *Node) Resolve(sc SearchContext, key interface{}) (interface{}, error) {
	var fields []*meta.FieldDescription = nil
	if n.OnlyLink {
		fields = []*meta.FieldDescription{n.Meta.Key}
	}

	obj, err := sc.dm.Get(n.Meta, fields, n.KeyFiled.Name, key)
	if err != nil {
		return nil, err
	}

	if obj == nil {
		return nil, nil
	}

	if !n.OnlyLink {
		return obj, nil
	}

	keyStr, err := n.keyAsString(obj)
	if err != nil {
		return nil, err
	}

	return fmt.Sprint(sc.lazyPath, "/", n.Meta.Name, "/", keyStr), nil
}

func (n *Node) ResolvePlural(sc SearchContext, key interface{}) ([]interface{}, error) {
	logger.Debug("Resolving plural: node [meta=%s, depth=%s, plural=%s], sc=%s, key=%s", n.Meta.Name, n.Depth, n.plural, sc, key)
	var fields []*meta.FieldDescription = nil
	if n.OnlyLink {
		fields = []*meta.FieldDescription{n.Meta.Key}
	}

	objs, err := sc.dm.GetAll(n.Meta, fields, n.KeyFiled.Name, key)
	if err != nil {
		return nil, err
	}

	objsLength := len(objs)
	result := make([]interface{}, objsLength, objsLength)
	if n.OnlyLink {
		for i, obj := range objs {
			keyStr, err := n.keyAsString(obj)
			if err != nil {
				return nil, err
			}
			result[i] = fmt.Sprint(sc.lazyPath, "/", n.Meta.Name, "/", keyStr)
		}
	} else {
		for i, obj := range objs {
			result[i] = obj
		}
	}

	return result, nil
}

func isBackLink(m *meta.Meta, f *meta.FieldDescription) bool {
	for i, _ := range m.Fields {
		if m.Fields[i].LinkType == meta.LinkTypeOuter && m.Fields[i].OuterLinkField.Name == f.Name && m.Fields[i].LinkMeta.Name == f.Meta.Name {
			return true
		}
	}
	return false
}

func (n *Node) fillBranches(ctx SearchContext) {
	for i, f := range n.Meta.Fields {
		var onlyLink = false
		var branches map[string]*Node = nil
		if n.Depth == ctx.depthLimit {
			onlyLink = true
		} else {
			branches = make(map[string]*Node)
		}
		var plural = false
		var keyFiled *meta.FieldDescription = nil

		if f.LinkType == meta.LinkTypeInner && (n.Parent == nil || !isBackLink(n.Parent.Meta, &f)) {
			keyFiled = f.LinkMeta.Key
			n.Branches[f.Name] = &Node{LinkField: &n.Meta.Fields[i],
				KeyFiled: keyFiled,
				Meta: f.LinkMeta,
				Branches: branches,
				Depth: n.Depth + 1,
				OnlyLink: onlyLink,
				plural: plural,
				Parent: n}
		} else if f.LinkType == meta.LinkTypeOuter {
			keyFiled = f.OuterLinkField
			if f.Type == meta.FieldTypeArray {
				plural = true
			}
			n.Branches[f.Name] = &Node{LinkField: &n.Meta.Fields[i],
				KeyFiled: keyFiled,
				Meta: f.LinkMeta,
				Branches: branches,
				Depth: n.Depth + 1,
				OnlyLink: onlyLink,
				plural: plural,
				Parent: n}
		}
	}
}

type tuple2n struct {
	first  *Node
	second map[string]interface{}
}

type tuple2na struct {
	first  *Node
	second []map[string]interface{}
}

func (t2 tuple2n) resolveBranches(ctx SearchContext) ([]tuple2n, error) {
	tn := make([]tuple2n, 0)
	for _, v := range t2.first.Branches {
		if v.LinkField.LinkType == meta.LinkTypeOuter && v.LinkField.Type == meta.FieldTypeArray {
			k := t2.second[v.Meta.Key.Name]
			if arr, e := v.ResolvePlural(ctx, k); e != nil {
				return nil, e
			} else if arr != nil {
				t2.second[v.LinkField.Name] = arr
				for _, m := range arr {
					if !v.OnlyLink {
						tn = append(tn, tuple2n{v, m.(map[string]interface{})})
					}
				}
			} else {
				delete(t2.second, v.LinkField.Name)
			}
		} else if v.LinkField.LinkType == meta.LinkTypeOuter {
			k := t2.second[v.Meta.Key.Name]
			if i, e := v.Resolve(ctx, k); e != nil {
				return nil, e
			} else if i != nil {
				t2.second[v.LinkField.Name] = i
				if !v.OnlyLink {
					tn = append(tn, tuple2n{v, i.(map[string]interface{})})
				}
			} else {
				delete(t2.second, v.LinkField.Name)
			}
		} else if v.LinkField.LinkType == meta.LinkTypeInner {
			k := t2.second[v.LinkField.Name]
			if i, e := v.Resolve(ctx, k); e != nil {
				return nil, e
			} else if i != nil {
				t2.second[v.LinkField.Name] = i
				if !v.OnlyLink {
					tn = append(tn, tuple2n{v, i.(map[string]interface{})})
				}
			} else {
				delete(t2.second, v.LinkField.Name)
			}
		}
	}
	return tn, nil
}

func (t2 tuple2na) resolveBranches2(ctx SearchContext) ([]tuple2na, error) {
	tn := make([]tuple2na, 0)
	for _, v := range t2.first.Branches {
		if v.LinkField.LinkType == meta.LinkTypeOuter && v.LinkField.Type == meta.FieldTypeArray {
			keys := make([]interface{}, 0, len(t2.second))
			refs := make(map[interface{}]map[string]interface{})
			for _, m := range t2.second {
				k := m[v.Meta.Key.Name]
				keys = append(keys, k)
				refs[k] = m
			}
			if arrs, e := v.ResolvePlural2(ctx, keys); e != nil {
				return nil, e
			} else {
				t := make([]map[string]interface{}, 0)
				for i, a := range arrs {
					if a != nil {
						refs[i][v.LinkField.Name] = a
						for _, m := range a {
							if !v.OnlyLink {
								t = append(t, m.(map[string]interface{}))
							}
						}
					} else {
						delete(refs[i], v.LinkField.Name)
					}
					tn = append(tn, tuple2na{v, t})
				}
			}
		} else if v.LinkField.LinkType == meta.LinkTypeOuter {
			keys := make([]interface{}, 0, len(t2.second))
			refs := make(map[interface{}]map[string]interface{})
			for _, m := range t2.second {
				k := m[v.Meta.Key.Name]
				keys = append(keys, k)
				refs[k] = m
			}
			if arr, e := v.Resolve2(ctx, keys); e != nil {
				return nil, e
			} else {
				t := make([]map[string]interface{}, 0)
				for i, o := range arr {
					if o != nil {
						refs[i][v.LinkField.Name] = o
						if !v.OnlyLink {
							t = append(t, o.(map[string]interface{}))
						}
					} else {
						delete(refs[i], v.LinkField.Name)
					}
					tn = append(tn, tuple2na{v, t})
				}
			}
		} else if v.LinkField.LinkType == meta.LinkTypeInner {
			keys := make([]interface{}, 0, len(t2.second))
			refs := make(map[interface{}]map[string]interface{})
			for _, m := range t2.second {
				k := m[v.LinkField.Name]
				keys = append(keys, k)
				refs[k] = m
			}
			if arr, e := v.Resolve2(ctx, keys); e != nil {
				return nil, e
			} else {
				t := make([]map[string]interface{}, 0)
				for i, o := range arr {
					if o != nil {
						refs[i][v.LinkField.Name] = o
						if !v.OnlyLink {
							t = append(t, o.(map[string]interface{}))
						}
					} else {
						delete(refs[i], v.LinkField.Name)
					}
					tn = append(tn, tuple2na{v, t})
				}
			}
		}
	}
	return tn, nil
}

func (processor *Processor) Get(objectClass, key string, depth int) (map[string]interface{}, error) {
	if m, ok, e := processor.metaStore.Get(objectClass); e != nil {
		return nil, e
	} else if !ok {
		return nil, NewDataError(objectClass, ErrObjectClassNotFound, "Object class '%s' not found", objectClass)
	} else {
		if pk, e := m.Key.ValueFromString(key); e != nil {
			return nil, e
		} else {
			ctx := SearchContext{depthLimit: depth, dm: processor.dataManager, lazyPath: "/custodian/data/single"}
			root := &Node{KeyFiled: m.Key, Meta: m, Branches: make(map[string]*Node), Depth: 1, OnlyLink: false, plural: false, Parent: nil}
			root.fillBranches(ctx)
			bs := make([]*Node, 0)
			for _, v := range root.Branches {
				bs = append(bs, v)
			}
			for ; len(bs) > 0; bs = bs[1:] {
				if !bs[0].OnlyLink {
					bs[0].fillBranches(ctx)
					for _, v := range bs[0].Branches {
						bs = append(bs, v)
					}
				}
			}

			if o, e := root.Resolve(ctx, pk); e != nil {
				return nil, e
			} else if o == nil {
				return nil, nil
			} else {
				obj := o.(map[string]interface{})
				for tn := []tuple2n{tuple2n{root, obj}}; len(tn) > 0; tn = tn[1:] {
					if t, e := tn[0].resolveBranches(ctx); e != nil {
						return nil, e
					} else {
						tn = append(tn, t...)
					}
				}
				return obj, nil
			}
		}
	}
}

func (processor *Processor) GetBulk(objectName, filter string, depth int, sink func(map[string]interface{}) error) error {
	if businessObject, ok, e := processor.metaStore.Get(objectName); e != nil {
		return e
	} else if !ok {
		return NewDataError(objectName, ErrObjectClassNotFound, "Object class '%s' not found", objectName)
	} else {
		searchContext := SearchContext{depthLimit: depth, dm: processor.dataManager, lazyPath: "/custodian/data/bulk"}
		root := &Node{
			KeyFiled: businessObject.Key,
			Meta:     businessObject,
			Branches: make(map[string]*Node),
			Depth:    1,
			OnlyLink: false,
			plural:   false,
			Parent:   nil,
		}
		root.fillBranches(searchContext)
		branches := make([]*Node, 0)
		for _, branch := range root.Branches {
			branches = append(branches, branch)
		}
		for ; len(branches) > 0; branches = branches[1:] {
			if !branches[0].OnlyLink {
				branches[0].fillBranches(searchContext)
				for _, v := range branches[0].Branches {
					branches = append(branches, v)
				}
			}
		}

		parser := rqlParser.NewParser()
		rqlNode, err := parser.Parse(strings.NewReader(filter))
		if err != nil {
			return NewDataError(objectName, ErrWrongRQL, err.Error())
		}

		objs, e := root.ResolveByRql(searchContext, rqlNode)

		if e != nil {
			return e
		}
		//recursively resolves "branches"
		for tn := []tuple2na{tuple2na{root, objs}}; len(tn) > 0; tn = tn[1:] {
			if t, e := tn[0].resolveBranches2(searchContext); e != nil {
				return e
			} else {
				tn = append(tn, t...)
			}
		}
		for _, v := range objs {
			sink(v)
		}
		return nil
	}
}

type DNode struct {
	KeyFiled *meta.FieldDescription
	Meta     *meta.Meta
	Branches map[string]*DNode
	Plural   bool
}

func (dn *DNode) fillOuterBranches() {
	for _, f := range dn.Meta.Fields {
		if f.LinkType == meta.LinkTypeOuter {
			var plural = false
			if f.Type == meta.FieldTypeArray {
				plural = true
			}
			dn.Branches[f.Name] = &DNode{KeyFiled: f.OuterLinkField,
				Meta: f.LinkMeta,
				Branches: make(map[string]*DNode),
				Plural: plural}
		}
	}
}

type tuple2d struct {
	n    *DNode
	keys []interface{}
}

func (processor *Processor) Delete(objectClass, key string, actor auth.User) (isDeleted bool, err error) {
	if m, ok, e := processor.metaStore.Get(objectClass); e != nil {
		return false, e
	} else if !ok {
		return false, NewDataError(objectClass, ErrObjectClassNotFound, "Object class '%s' not found", objectClass)
	} else {
		if pk, e := m.Key.ValueFromString(key); e != nil {
			return false, e
		} else {
			root := &DNode{KeyFiled: m.Key, Meta: m, Branches: make(map[string]*DNode), Plural: false}
			root.fillOuterBranches()
			for v := []map[string]*DNode{root.Branches}; len(v) > 0; v = v[1:] {
				for _, n := range v[0] {
					n.fillOuterBranches()
					if len(n.Branches) > 0 {
						v = append(v, n.Branches)
					}
				}
			}
			n := newNotification(meta.MethodRemove)
			defer func() {
				n.complete(err)
			}()
			if op, keys, e := processor.dataManager.PrepareDeletes(root, []interface{}{pk}); e != nil {
				return false, e
			} else {

				notificaion_data := make(map[string]interface{})

				notificaion_data["actor"] = actor
				notificaion_data["data"] = map[string]interface{}{root.KeyFiled.Name: pk}

				n.push(root.Meta, notificaion_data, true)
				ops := []Operation{op}
				for t2d := []tuple2d{tuple2d{root, keys}}; len(t2d) > 0; t2d = t2d[1:] {
					for _, v := range t2d[0].n.Branches {
						if op, keys, e := processor.dataManager.PrepareDeletes(v, t2d[0].keys); e != nil {
							return false, e
						} else {
							objs := make([]map[string]interface{}, len(t2d[0].keys))
							for i, k := range t2d[0].keys {
								objs[i] = map[string]interface{}{v.KeyFiled.Name: k}
							}

							notificaion_data := make(map[string]interface{})

							notificaion_data["actor"] = actor
							notificaion_data["data"] = objs

							n.push(v.Meta, notificaion_data, false)
							ops = append(ops, op)
							t2d = append(t2d, tuple2d{v, keys})
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

func (processor *Processor) DeleteBulk(objectClass string, next func() (map[string]interface{}, error)) (err error) {
	if m, ok, e := processor.metaStore.Get(objectClass); e != nil {
		return e
	} else if !ok {
		return NewDataError(objectClass, ErrObjectClassNotFound, "Object class '%s' not found", objectClass)
	} else {
		ts := m.Key.Type.TypeAsserter()

		root := &DNode{KeyFiled: m.Key, Meta: m, Branches: make(map[string]*DNode), Plural: false}
		root.fillOuterBranches()
		for v := []map[string]*DNode{root.Branches}; len(v) > 0; v = v[1:] {
			for _, n := range v[0] {
				n.fillOuterBranches()
				if len(n.Branches) > 0 {
					v = append(v, n.Branches)
				}
			}
		}

		exCtx, e := processor.dataManager.ExecuteContext()
		if e != nil {
			return e
		}
		defer exCtx.Close()
		var buf = make([]interface{}, 0, 100)
		n := newNotification(meta.MethodRemove)
		defer func() {
			n.complete(err)
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
						for _, v := range t2d[0].n.Branches {
							if len(t2d[0].keys) > 0 {
								if op, keys, e := processor.dataManager.PrepareDeletes(v, t2d[0].keys); e != nil {
									return e
								} else {
									objs := make([]map[string]interface{}, len(t2d[0].keys))
									for i, k := range t2d[0].keys {
										objs[i] = map[string]interface{}{v.KeyFiled.Name: k}
									}
									n.pushAll(v.Meta, objs, false)
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

func (processor *Processor) Update(objectClass, key string, obj map[string]interface{}, actor auth.User) (retObj map[string]interface{}, err error) {
	m, ok, e := processor.metaStore.Get(objectClass)
	if e != nil {
		return nil, e
	}
	if !ok {
		return nil, NewDataError(objectClass, ErrObjectClassNotFound, "Object class '%s' not found", objectClass)
	}

	if _, e := m.Key.ValueFromString(key); e != nil {
		return nil, e
	}

	tc, e := processor.flatten(m, obj, func(mn string) (objectClassValidator, error) {
		return processor.getValidator("upd:"+mn, updateValidator)
	})
	if e != nil {
		return nil, e
	}

	var ops = make([]Operation, 0)
	n := newNotification(meta.MethodUpdate)
	defer func() {
		n.complete(err)
	}()
	for i, t := range tc {
		if op, e := processor.dataManager.PrepareUpdates(t.First, []map[string]interface{}{t.Second}); e != nil {
			return nil, e
		} else {
			notificaion_data := make(map[string]interface{})

			notificaion_data["actor"] = actor
			notificaion_data["data"] = tc[i].Second

			n.push(t.First, notificaion_data, i == 0)
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

func (processor *Processor) UpdateBulk(objectClass string, next func() (map[string]interface{}, error), sink func(map[string]interface{}) error) (err error) {
	m, ok, e := processor.metaStore.Get(objectClass)
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
	n := newNotification(meta.MethodUpdate)
	defer func() {
		n.complete(err)
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

					for _, obj := range item.Second {
						collapseLinks(obj)
					}
					n.pushAll(item.First, item.Second, isRoot)
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
