package data

import (
	"server/meta"
	"github.com/Q-CIS-DEV/go-rql-parser"
	"strings"
	"server/auth"
	"server/data/errors"
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

func putValidator(t *Record) (*Record, bool, error) {
	t.Data["cas"] = 1.0
	return t, true, nil
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

func (dn *DNode) recursivelyFillOuterChildNodes() {
	dn.fillOuterChildNodes()
	for v := []map[string]*DNode{dn.ChildNodes}; len(v) > 0; v = v[1:] {
		for _, n := range v[0] {
			n.fillOuterChildNodes()
			if len(n.ChildNodes) > 0 {
				v = append(v, n.ChildNodes)
			}
		}
	}
}

type tuple2d struct {
	n    *DNode
	keys []interface{}
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

func (processor *Processor) CreateRecord(objectClass string, obj map[string]interface{}, user auth.User) (retObj map[string]interface{}, err error) {
	objectMeta, ok, e := processor.metaStore.Get(objectClass, true)
	if e != nil || !ok {
		if e != nil {
			return nil, e
		} else {
			return nil, errors.NewDataError(objectClass, errors.ErrObjectClassNotFound, "Object class '%s' not found", objectClass)
		}
	}

	// create notification pool
	recordSetNotificationPool := notifications.NewRecordSetNotificationPool()
	defer func() { recordSetNotificationPool.CompleteSend(err) }()

	//start transaction
	executeContext, err := processor.dataManager.NewExecuteContext()
	if err != nil {
		return nil, err
	}
	defer executeContext.Close()

	// assemble records
	records, e := processor.flatten(objectMeta, obj, func(mn string) (objectClassValidator, error) {
		return processor.getValidator("put:"+mn, putValidator)
	})
	if e != nil {
		return nil, e
	}

	// create records
	for i, record := range records {
		if _, err := processor.createRecordSet(
			&RecordSet{Meta: record.Meta, DataSet: []map[string]interface{}{record.Data}},
			executeContext,
			i == 0,
			recordSetNotificationPool,
		); err != nil {
			return nil, err
		}
	}

	// push notifications if needed
	if recordSetNotificationPool.ShouldBeProcessed() {
		//capture updated state of all records in the pool
		recordSetNotificationPool.CaptureCurrentState()
		recordSetNotificationPool.Push(user)
	}

	//commit transaction
	if err = executeContext.Complete(); err != nil {
		return nil, err
	} else {
		return obj, nil
	}
}

func (processor *Processor) BulkCreateRecords(objectClass string, next func() (map[string]interface{}, error), sink func(map[string]interface{}) error, user auth.User) (err error) {
	//get meta
	objectMeta, ok, e := processor.metaStore.Get(objectClass, true)
	if e != nil || !ok {
		if e != nil {
			return e
		} else {
			return errors.NewDataError(objectClass, errors.ErrObjectClassNotFound, "Object class '%s' not found", objectClass)
		}
	}

	// create notification pool
	recordSetNotificationPool := notifications.NewRecordSetNotificationPool()
	defer func() { recordSetNotificationPool.CompleteSend(err) }()

	//start transaction
	executeContext, e := processor.dataManager.NewExecuteContext()
	if e != nil {
		return e
	}
	defer executeContext.Close()

	// collect records` data to update
	recordDataSet, err := processor.consumeRecordDataSet(next, objectMeta, false)
	if err != nil {
		return err
	}

	//assemble RecordSets
	recordSetsSplitByObject, e := processor.splitNestedRecordsByObjects(objectMeta, recordDataSet, func(mn string) (objectClassValidator, error) {
		return processor.getValidator("put:"+mn, putValidator)
	})
	if e != nil {
		return e
	}

	for i, recordSets := range recordSetsSplitByObject {
		isRoot := i == 0
		for _, recordSet := range recordSets {
			if _, err := processor.createRecordSet(recordSet, executeContext, isRoot, recordSetNotificationPool); err != nil {
				return err
			}
		}

	}

	// feed created data to the sink
	if err = processor.feedRecordSets(recordSetsSplitByObject[0], sink); err != nil {
		return err
	}

	// push notifications if needed
	if recordSetNotificationPool.ShouldBeProcessed() {
		//capture updated state of all records in the pool
		recordSetNotificationPool.CaptureCurrentState()
		recordSetNotificationPool.Push(user)
	}

	//commit transaction
	if e := executeContext.Complete(); e != nil {
		return e
	} else {
		return nil
	}
}

func (processor *Processor) UpdateRecord(objectName, key string, recordData map[string]interface{}, user auth.User) (updatedRecordData map[string]interface{}, err error) {
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

	// create notification pool
	recordSetNotificationPool := notifications.NewRecordSetNotificationPool()
	defer func() { recordSetNotificationPool.CompleteSend(err) }()

	//start transaction
	executeContext, err := processor.dataManager.NewExecuteContext()
	if err != nil {
		return nil, err
	}
	defer executeContext.Close()

	//perform update
	var rootRecordData map[string]interface{}
	for i, record := range records {
		isRoot := i == 0

		if recordSet, err := processor.updateRecordSet(
			&RecordSet{Meta: objectMeta, DataSet: []map[string]interface{}{record.Data}},
			executeContext,
			isRoot,
			recordSetNotificationPool,
		); err != nil {
			return nil, err
		} else {
			if isRoot {
				rootRecordData = recordSet.DataSet[0]
			}
		}
	}

	if err = executeContext.Complete(); err != nil {
		return nil, err
	} else {
		return rootRecordData, nil
	}
}

func (processor *Processor) BulkUpdateRecords(objectName string, next func() (map[string]interface{}, error), sink func(map[string]interface{}) error, user auth.User) (err error) {
	// get Meta
	objectMeta, err := processor.getMeta(objectName)
	if err != nil {
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
	defer func() { recordSetNotificationPool.CompleteSend(err) }()

	// collect records` data to update
	recordDataSet, err := processor.consumeRecordDataSet(next, objectMeta, true)
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
			processor.updateRecordSet(recordSet, executeContext, isRoot, recordSetNotificationPool)
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

func (processor *Processor) DeleteRecord(objectName, key string, user auth.User) (isDeleted bool, err error) {
	// get Meta
	objectMeta, err := processor.getMeta(objectName)
	if err != nil {
		return false, err
	}

	//get pk
	var pk interface{}
	pk, err = objectMeta.Key.ValueFromString(key)
	if err != nil {
		return false, err
	}

	//fill node
	root := &DNode{KeyField: objectMeta.Key, Meta: objectMeta, ChildNodes: make(map[string]*DNode), Plural: false}
	root.recursivelyFillOuterChildNodes()

	// create notification pool
	recordSetNotificationPool := notifications.NewRecordSetNotificationPool()
	defer func() { recordSetNotificationPool.CompleteSend(err) }()

	//start transaction
	var executeContext ExecuteContext
	executeContext, err = processor.dataManager.NewExecuteContext()
	if err != nil {
		return false, err
	}
	defer executeContext.Close()

	//prepare operation
	var op Operation
	var keys []interface{}
	op, keys, err = processor.dataManager.PrepareDeletes(root, []interface{}{pk})
	if err != nil {
		return false, err
	}
	//process root records notificationSender

	// create notification, capture current recordData state and Add notification to notification pool
	recordSetNotification := notifications.NewRecordSetStateNotification(&RecordSet{Meta: root.Meta, DataSet: []map[string]interface{}{{root.KeyField.Name: pk}}}, true, meta.MethodRemove, processor.GetBulk)
	if recordSetNotification.ShouldBeProcessed() {
		recordSetNotification.CapturePreviousState()
		recordSetNotificationPool.Add(recordSetNotification)
	}

	ops := []Operation{op}
	for t2d := []tuple2d{{root, keys}}; len(t2d) > 0; t2d = t2d[1:] {
		for _, v := range t2d[0].n.ChildNodes {
			if op, keys, err = processor.dataManager.PrepareDeletes(v, t2d[0].keys); err != nil {
				return false, err
			} else {
				ops = append(ops, op)
				t2d = append(t2d, tuple2d{v, keys})

				//process affected records notifications
				for _, primaryKeyValue := range t2d[0].keys {

					// create notification, capture current recordData state and Add notification to notification pool
					recordSetNotification := notifications.NewRecordSetStateNotification(&RecordSet{Meta: root.Meta, DataSet: []map[string]interface{}{{root.KeyField.Name: primaryKeyValue}}}, false, meta.MethodRemove, processor.GetBulk)
					if recordSetNotification.ShouldBeProcessed() {
						recordSetNotification.CapturePreviousState()
						recordSetNotificationPool.Add(recordSetNotification)
					}
				}
			}
		}
	}
	for i := 0; i < len(ops)>>2; i++ {
		ops[i], ops[len(ops)-1] = ops[len(ops)-1], ops[i]
	}
	err = executeContext.Execute(ops)
	if err != nil {
		return false, err
	}

	//commit transaction
	if err = executeContext.Complete(); err != nil {
		return false, err
	} else {
		return true, nil
	}
}

func (processor *Processor) BulkDeleteRecords(objectName string, next func() (map[string]interface{}, error), user auth.User) (err error) {
	// get Meta
	objectMeta, err := processor.getMeta(objectName)
	if err != nil {
		return err
	}

	//prepare node
	root := &DNode{KeyField: objectMeta.Key, Meta: objectMeta, ChildNodes: make(map[string]*DNode), Plural: false}
	root.recursivelyFillOuterChildNodes()

	//start transaction
	exCtx, e := processor.dataManager.NewExecuteContext()
	if e != nil {
		return e
	}
	defer exCtx.Close()

	// create notification pool
	recordSetNotificationPool := notifications.NewRecordSetNotificationPool()
	defer func() { recordSetNotificationPool.CompleteSend(err) }()

	// collect records` data to update
	recordDataSet, err := processor.consumeRecordDataSet(next, objectMeta, true)
	if err != nil {
		return err
	}
	keys := make([]interface{}, 0)
	for _, recordData := range recordDataSet {
		keys = append(keys, recordData[objectMeta.Key.Name])
	}

	if op, keys, e := processor.dataManager.PrepareDeletes(root, keys); e != nil {
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

							// create notification, capture current recordData state and Add notification to notification pool
							recordSetNotification := notifications.NewRecordSetStateNotification(&RecordSet{Meta: root.Meta, DataSet: []map[string]interface{}{{v.KeyField.Name: primaryKeyValue}}}, false, meta.MethodRemove, processor.GetBulk)
							if recordSetNotification.ShouldBeProcessed() {
								recordSetNotification.CapturePreviousState()
								recordSetNotificationPool.Add(recordSetNotification)
							}

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

	if e := exCtx.Complete(); e != nil {
		return e
	} else {
		return nil
	}

}

//consume all records from callback function
func (processor *Processor) consumeRecordDataSet(nextCallback func() (map[string]interface{}, error), objectMeta *meta.Meta, strictPkCheck bool) ([]map[string]interface{}, error) {
	var recordDataSet = make([]map[string]interface{}, 0)
	// collect records
	for recordData, err := nextCallback(); err != nil || (recordData != nil); recordData, err = nextCallback() {
		if err != nil {
			return nil, err
		}
		if strictPkCheck {
			keyValue, ok := recordData[objectMeta.Key.Name]
			if !ok || !objectMeta.Key.Type.TypeAsserter()(keyValue) {
				return nil, errors.NewDataError(objectMeta.Name, errors.ErrKeyValueNotFound, "Key value not found or has a wrong type", objectMeta.Name)
			}
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

// get list of RecordUpdateTasks, perform update and return list of records
func (processor *Processor) updateRecordSet(recordSet *RecordSet, executeContext ExecuteContext, isRoot bool, recordSetNotificationPool *notifications.RecordSetNotificationPool) (*RecordSet, error) {

	// create notification, capture current recordData state and Add notification to notification pool
	recordSetNotification := notifications.NewRecordSetStateNotification(recordSet, isRoot, meta.MethodUpdate, processor.GetBulk)
	if recordSetNotification.ShouldBeProcessed() {
		recordSetNotification.CapturePreviousState()
		recordSetNotificationPool.Add(recordSetNotification)
	}

	var operations = make([]Operation, 0)

	if operation, e := processor.dataManager.PrepareUpdates(recordSet.Meta, recordSet.DataSet); e != nil {
		return nil, e
	} else {
		operations = append(operations, operation)
	}
	//
	if e := executeContext.Execute(operations); e != nil {
		return nil, e
	}

	recordSet.CollapseLinks()

	return recordSet, nil
}

// get list of RecordUpdateTasks, perform create and return list of records
func (processor *Processor) createRecordSet(recordSet *RecordSet, executeContext ExecuteContext, isRoot bool, recordSetNotificationPool *notifications.RecordSetNotificationPool) (*RecordSet, error) {

	// create notification, capture current recordData state and Add notification to notification pool
	recordSetNotification := notifications.NewRecordSetStateNotification(recordSet, isRoot, meta.MethodCreate, processor.GetBulk)
	if recordSetNotification.ShouldBeProcessed() {
		recordSetNotification.CapturePreviousState()
		recordSetNotificationPool.Add(recordSetNotification)
	}

	var operations = make([]Operation, 0)

	if operation, e := processor.dataManager.PreparePuts(recordSet.Meta, recordSet.DataSet); e != nil {
		return nil, e
	} else {
		operations = append(operations, operation)
	}
	//
	if e := executeContext.Execute(operations); e != nil {
		return nil, e
	}

	recordSet.CollapseLinks()

	return recordSet, nil
}
