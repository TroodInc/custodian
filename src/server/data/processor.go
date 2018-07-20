package data

import (
	"server/meta"
	"github.com/Q-CIS-DEV/go-rql-parser"
	"strings"
	"server/auth"
	"server/data/errors"
	"fmt"
	. "server/data/record"
	"server/data/notifications"
)

type objectClassValidator func(Record) ([]Record, error)

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
	GetAll(m *meta.Meta, fileds []*meta.FieldDescription, filters map[string]interface{}) ([]map[string]interface{}, error)
	PrepareDeletes(n *DNode, keys []interface{}) (Operation, []interface{}, error)
	PreparePuts(m *meta.Meta, objs []map[string]interface{}) (Operation, error)
	PrepareUpdates(m *meta.Meta, objs []map[string]interface{}) (Operation, error)
	Execute(operations []Operation) error
	NewExecuteContext() (ExecuteContext, error)
}

type Processor struct {
	metaStore   *meta.MetaStore
	dataManager DataManager
	vCache      map[string]objectClassValidator
}

func NewProcessor(m *meta.MetaStore, d DataManager) (*Processor, error) {
	return &Processor{metaStore: m, dataManager: d, vCache: make(map[string]objectClassValidator)}, nil
}

type RecordUpdateTask struct {
	Record       *Record
	ShouldReturn bool
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

func (processor *Processor) getValidator(vk string, preValidator func(pt2 *Record) (*Record, bool, error)) (objectClassValidator, error) {
	if v, ex := processor.vCache[vk]; ex {
		return v, nil
	}
	validator := func(t2 Record) ([]Record, error) {
		preValidatedT2, mandatoryCheck, err := preValidator(&t2)
		if err != nil {
			return nil, err
		}
		if toCheck, e := NewValidationService(processor.metaStore, processor).Validate(preValidatedT2, mandatoryCheck); e != nil {
			return nil, e
		} else {
			return toCheck, nil
		}
	}
	processor.vCache[vk] = validator
	return validator, nil

}

func (processor *Processor) flatten(objectMeta *meta.Meta, recordValues map[string]interface{}, validatorFactory func(mn string) (objectClassValidator, error)) ([]Record, error) {
	tc := []Record{{objectMeta, recordValues}}
	for tail := tc; len(tail) > 0; tail = tail[1:] {
		if v, e := validatorFactory(tail[0].Meta.Name); e != nil {
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

func (processor *Processor) splitNestedRecordsByObjects(meta *meta.Meta, recordsData []map[string]interface{}, validatorFactory func(mn string) (objectClassValidator, error)) ([][]*RecordSet, error) {
	var recordSetsSplitByLevels = [][]*RecordSet{{&RecordSet{meta, recordsData}}}

	for currentLevel := recordSetsSplitByLevels[0]; currentLevel != nil; {
		next := make(map[string]*RecordSet)
		for tail := currentLevel; len(tail) > 0; tail = tail[1:] {
			if validator, e := validatorFactory(tail[0].Meta.Name); e != nil {
				return nil, e
			} else {
				for _, recordData := range tail[0].DataSet {
					if nestedRecords, e := validator(Record{tail[0].Meta, recordData}); e != nil {
						return nil, e
					} else {
						for _, record := range nestedRecords {
							if recordSet, ok := next[record.Meta.Name]; ok {
								recordSet.DataSet = append(recordSet.DataSet, record.Data)
							} else {
								next[record.Meta.Name] = &RecordSet{record.Meta, []map[string]interface{}{record.Data}}
							}
						}
					}
				}
			}
		}
		if len(next) > 0 {
			nextLevel := make([]*RecordSet, 0, len(next))
			for _, pt2a := range next {
				nextLevel = append(nextLevel, pt2a)
			}
			recordSetsSplitByLevels = append(recordSetsSplitByLevels, nextLevel)
			currentLevel = nextLevel
		} else {
			currentLevel = nil
		}
	}
	return recordSetsSplitByLevels, nil
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

func putValidator(t *Record) (*Record, bool, error) {
	t.Data["cas"] = 1.0
	return t, true, nil
}

func (processor *Processor) Put(objectClass string, obj map[string]interface{}, user auth.User) (retObj map[string]interface{}, err error) {
	m, ok, e := processor.metaStore.Get(objectClass, true)
	if e != nil {
		return nil, e
	}
	if !ok {
		return nil, errors.NewDataError(objectClass, errors.ErrObjectClassNotFound, "Object class '%s' not found", objectClass)
	}

	tc, e := processor.flatten(m, obj, func(mn string) (objectClassValidator, error) {
		return processor.getValidator("put:"+mn, putValidator)
	})
	if e != nil {
		return nil, e
	}

	var ops = make([]Operation, 0)
	for _, t := range tc {
		if op, e := processor.dataManager.PreparePuts(t.Meta, []map[string]interface{}{t.Data}); e != nil {
			return nil, e
		} else {
			ops = append(ops, op)
		}
	}

	if e := processor.dataManager.Execute(ops); e != nil {
		return nil, e
	}

	//process notifications
	//notificationSender := newNotificationSender()
	//defer func() {
	//	notificationSender.complete(err)
	//}()
	for i, _ := range tc {
		collapseLinks(tc[i].Data)
		//notificationSender.push(NOTIFICATION_CREATE, tc[i].Meta, tc[i].Data, user, i == 0)
	}

	return obj, nil
}

func (processor *Processor) PutBulk(objectClass string, next func() (map[string]interface{}, error), sink func(map[string]interface{}) error, user auth.User) (response error) {
	m, ok, e := processor.metaStore.Get(objectClass, true)
	if e != nil {
		return e
	}
	if !ok {
		return errors.NewDataError(objectClass, errors.ErrObjectClassNotFound, "Object class '%s' not found", objectClass)
	}

	exCtx, e := processor.dataManager.NewExecuteContext()
	if e != nil {
		return e
	}
	defer exCtx.Close()

	var buf = make([]map[string]interface{}, 0, 100)
	//notificationSender := newNotificationSender()
	//defer func() {
	//	notificationSender.complete(response)
	//}()
	for {
		for o, e := next(); e != nil || (o != nil && len(buf) < 100); o, e = next() {
			if e != nil {
				return e
			}
			buf = append(buf, o)
		}

		if len(buf) > 0 {
			levelLader, e := processor.splitNestedRecordsByObjects(m, buf, func(mn string) (objectClassValidator, error) {
				return processor.getValidator("put:"+mn, putValidator)
			})
			if e != nil {
				return e
			}

			for levelIdx, level := range levelLader {
				isRoot := levelIdx == 0
				fmt.Println(isRoot)
				for _, item := range level {
					op, e := processor.dataManager.PreparePuts(item.Meta, item.DataSet)
					if e != nil {
						return e
					}

					if e := exCtx.Execute([]Operation{op}); e != nil {
						return e
					}

					for _, recordData := range item.DataSet {
						collapseLinks(recordData)

						//notificationSender.push(NOTIFICATION_CREATE, item.Meta, recordData, user, isRoot)
					}
				}
			}
			for _, roots := range levelLader[0] {
				for _, root := range roots.DataSet {
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
	if objectMeta, ok, e := processor.metaStore.Get(objectClass, true); e != nil {
		return nil, e
	} else if !ok {
		return nil, errors.NewDataError(objectClass, errors.ErrObjectClassNotFound, "Object class '%s' not found", objectClass)
	} else {
		if pk, e := objectMeta.Key.ValueFromString(key); e != nil {
			return nil, e
		} else {
			ctx := SearchContext{depthLimit: depth, dm: processor.dataManager, lazyPath: "/custodian/data/single"}

			root := &Node{KeyField: objectMeta.Key, Meta: objectMeta, ChildNodes: make(map[string]*Node), Depth: 1, OnlyLink: false, plural: false, Parent: nil, Type: NodeTypeRegular}
			root.RecursivelyFillChildNodes(ctx.depthLimit)

			if o, e := root.Resolve(ctx, pk); e != nil {
				return nil, e
			} else if o == nil {
				return nil, nil
			} else {
				recordValues := o.(map[string]interface{})
				for resultNodes := []ResultNode{{root, recordValues}}; len(resultNodes) > 0; resultNodes = resultNodes[1:] {
					if childResultNodes, e := resultNodes[0].getFilledChildNodes(ctx); e != nil {
						return nil, e
					} else {
						resultNodes = append(resultNodes, childResultNodes...)
					}
				}
				return recordValues, nil
			}
		}
	}
}

func (processor *Processor) GetBulk(objectName string, filter string, depth int, sink func(map[string]interface{}) error) error {
	if businessObject, ok, e := processor.metaStore.Get(objectName, true); e != nil {
		return e
	} else if !ok {
		return errors.NewDataError(objectName, errors.ErrObjectClassNotFound, "Object class '%s' not found", objectName)
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
			Type:       NodeTypeRegular,
		}
		root.RecursivelyFillChildNodes(searchContext.depthLimit)

		parser := rqlParser.NewParser()
		rqlNode, err := parser.Parse(strings.NewReader(filter))
		if err != nil {
			return errors.NewDataError(objectName, errors.ErrWrongRQL, err.Error())
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
			dn.ChildNodes[f.Name] = &DNode{KeyField: f.OuterLinkField,
				Meta: f.LinkMeta,
				ChildNodes: make(map[string]*DNode),
				Plural: true}
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
		return false, errors.NewDataError(objectClass, errors.ErrObjectClassNotFound, "Object class '%s' not found", objectClass)
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
			//notificationSender := newNotificationSender()
			//defer func() {
			//	notificationSender.complete(err)
			//}()
			if op, keys, e := processor.dataManager.PrepareDeletes(root, []interface{}{pk}); e != nil {
				return false, e
			} else {
				//process root records notificationSender
				//notificationSender.push(NOTIFICATION_DELETE, root.Meta, map[string]interface{}{root.KeyField.Name: pk}, user, true)

				ops := []Operation{op}
				for t2d := []tuple2d{tuple2d{root, keys}}; len(t2d) > 0; t2d = t2d[1:] {
					for _, v := range t2d[0].n.ChildNodes {
						if op, keys, e := processor.dataManager.PrepareDeletes(v, t2d[0].keys); e != nil {
							return false, e
						} else {
							ops = append(ops, op)
							t2d = append(t2d, tuple2d{v, keys})

							//process affected records notifications
							//for _, primaryKeyValue := range t2d[0].keys {
							//	//notificationSender.push(NOTIFICATION_DELETE, v.Meta, map[string]interface{}{v.KeyField.Name: primaryKeyValue}, user, false)
							//}

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
		return errors.NewDataError(objectClass, errors.ErrObjectClassNotFound, "Object class '%s' not found", objectClass)
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

		exCtx, e := processor.dataManager.NewExecuteContext()
		if e != nil {
			return e
		}
		defer exCtx.Close()
		var buf = make([]interface{}, 0, 100)
		//notificationSender := newNotificationSender()
		//defer func() {
		//	notificationSender.complete(err)
		//}()
		for {
			for o, e := next(); e != nil || (o != nil && len(buf) < 100); o, e = next() {
				if e != nil {
					return e
				}
				k, ok := o[m.Key.Name]
				if !ok || !ts(k) {
					return errors.NewDataError(objectClass, errors.ErrKeyValueNotFound, "Key value not found or has a wrong type", objectClass)
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
									//for _, primaryKeyValue := range t2d[0].keys {
									//	notificationSender.push(NOTIFICATION_DELETE, v.Meta, map[string]interface{}{v.KeyField.Name: primaryKeyValue}, user, false)
									//}

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

func updateValidator(t *Record) (*Record, bool, error) {
	return t, false, nil
}

func (processor *Processor) getMeta(objectName string) (*meta.Meta, error) {
	objectMeta, ok, err := processor.metaStore.Get(objectName, true)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.NewDataError(objectName, errors.ErrObjectClassNotFound, "Object class '%s' not found", objectName)
	}
	return objectMeta, nil
}

func (processor *Processor) Update(objectName, key string, recordData map[string]interface{}, user auth.User) (updatedRecordData map[string]interface{}, err error) {
	// get Meta
	objectMeta, err := processor.getMeta(objectName)
	if err != nil {
		return nil, err
	}
	// get and fill key value
	if pkValue, e := objectMeta.Key.ValueFromString(key); e != nil {
		return nil, e
	} else {
		//recordData data must contain valid recordData`s PK value
		recordData[objectMeta.Key.Name] = pkValue
	}
	// extract list of records from nested record data
	records, err := processor.flatten(objectMeta, recordData, func(mn string) (objectClassValidator, error) {
		return processor.getValidator("upd:"+mn, updateValidator)
	})
	// make list of RecordUpdateTasks
	recordUpdateTasks := make([]*RecordUpdateTask, len(records))
	for i, record := range records {
		shouldReturn := true
		if i > 0 {
			shouldReturn = false
		}
		recordUpdateTasks[i] = &RecordUpdateTask{ShouldReturn: shouldReturn, Record: &record}
	}
	//perform update
	updatedRecordsData, err := processor.updateRecords(recordUpdateTasks)
	if err != nil {
		return nil, err
	}

	return updatedRecordsData[0].Data, nil
}

// get list of RecordUpdateTasks, perform update and return list of records
func (processor *Processor) updateRecords(recordUpdateTasks []*RecordUpdateTask) ([]*Record, error) {

	var operations = make([]Operation, 0)

	for _, recordUpdateTask := range recordUpdateTasks {
		if operation, e := processor.dataManager.PrepareUpdates(recordUpdateTask.Record.Meta, []map[string]interface{}{recordUpdateTask.Record.Data}); e != nil {
			return nil, e
		} else {
			operations = append(operations, operation)
		}
	}
	//
	if e := processor.dataManager.Execute(operations); e != nil {
		return nil, e
	}

	records := make([]*Record, 0)
	for _, recordUpdateTask := range recordUpdateTasks {
		collapseLinks(recordUpdateTask.Record.Data)
		if recordUpdateTask.ShouldReturn {
			records = append(records, recordUpdateTask.Record)
		}
	}
	return records, nil
}

func (processor *Processor) UpdateBulk(objectName string, next func() (map[string]interface{}, error), sink func(map[string]interface{}) error, user auth.User) (err error) {
	//get meta
	objectMeta, ok, err := processor.metaStore.Get(objectName, true)
	if err != nil || !ok {
		if !ok {
			return errors.NewDataError(objectName, errors.ErrObjectClassNotFound, "Object '%s' not found", objectName)
		}
		return err
	}

	//start transaction
	executeContext, err := processor.dataManager.NewExecuteContext()
	if err != nil {
		return err
	}
	defer executeContext.Close()

	// create notification pool
	recordSetNotificationPool := notifications.NewRecordSetNotificationPool()
	defer recordSetNotificationPool.CompleteSend(err)

	// collect records` data to update
	recordDataSet, err := processor.consumeRecordDataSet(next)
	if err != nil {
		return err
	}

	//assemble RecordSets
	recordSetsSplitByObject, err := processor.splitNestedRecordsByObjects(objectMeta, recordDataSet, func(mn string) (objectClassValidator, error) { return processor.getValidator("upd:"+mn, updateValidator) })
	if err != nil {
		return err
	}

	//perform update
	for i, recordsSets := range recordSetsSplitByObject {
		isRoot := i == 0
		for _, recordSet := range recordsSets {
			processor.processRecordSetUpdate(recordSet, executeContext, isRoot, recordSetNotificationPool)
		}
	}

	// feed updated data to the sink
	processor.feedRecordSets(recordSetsSplitByObject[0], sink)

	// push notifications if needed
	if recordSetNotificationPool.ShouldBeProcessed() {
		//capture updated state of all records in the pool
		recordSetNotificationPool.CaptureCurrentState()
		recordSetNotificationPool.Push(user)
	}

	if err = executeContext.Complete(); err != nil {
		return err
	} else {
		return nil
	}

}

//consume all records from callback function
func (processor *Processor) consumeRecordDataSet(nextCallback func() (map[string]interface{}, error)) ([]map[string]interface{}, error) {
	var recordDataSet = make([]map[string]interface{}, 0)
	// collect records to update
	for recordData, err := nextCallback(); err != nil || (recordData != nil); recordData, err = nextCallback() {
		if err != nil {
			return nil, err
		}
		recordDataSet = append(recordDataSet, recordData)
	}
	return recordDataSet, nil
}

//feed recordSet`s data to the sink
func (processor *Processor) feedRecordSets(recordSets []*RecordSet, sink func(map[string]interface{}) error) error {
	for _, recordsSet := range recordSets {
		for _, recordData := range recordsSet.DataSet {
			if err := sink(recordData); err != nil {
				return err
			}
		}
	}
	return nil
}

//process recordSet update
func (processor *Processor) processRecordSetUpdate(recordSet *RecordSet, executeContext ExecuteContext, isRoot bool, recordSetNotificationPool *notifications.RecordSetNotificationPool) error {

	// create notification, capture current recordData state and Add notification to notification pool
	recordSetNotification := notifications.NewRecordSetStateNotification(recordSet, isRoot, meta.MethodUpdate, processor.GetBulk)
	if recordSetNotification.ShouldBeProcessed() {
		recordSetNotification.CapturePreviousState()
		recordSetNotificationPool.Add(recordSetNotification)
	}

	//process record update
	op, err := processor.dataManager.PrepareUpdates(recordSet.Meta, recordSet.DataSet)
	if err != nil {
		return err
	}

	// perform update
	if e := executeContext.Execute([]Operation{op}); e != nil {
		return e
	}

	// some magic to perform with recordSet after previous operation execution =/
	for _, recordData := range recordSet.DataSet {
		collapseLinks(recordData)
	}
	return nil
}
