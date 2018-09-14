package data

import (
	"server/object/meta"
	"github.com/Q-CIS-DEV/go-rql-parser"
	"strings"
	"server/auth"
	"server/data/errors"
	. "server/data/record"
	"server/data/notifications"
	"server/object/description"
	"server/transactions"
)

type objectClassValidator func(*Record) ([]*Record, error)

type ExecuteContext interface {
	Execute(operations []transactions.Operation) error
	Complete() error
	Close() error
}

type DataManager interface {
	Db() (interface{})
	GetRql(dataNode *Node, rqlNode *rqlParser.RqlRootNode, fields []*meta.FieldDescription, dbTransaction transactions.DbTransaction) ([]map[string]interface{}, error)
	GetIn(m *meta.Meta, fields []*meta.FieldDescription, key string, in []interface{}, dbTransaction transactions.DbTransaction) ([]map[string]interface{}, error)
	Get(m *meta.Meta, fields []*meta.FieldDescription, key string, val interface{}, dbTransaction transactions.DbTransaction) (map[string]interface{}, error)
	GetAll(m *meta.Meta, fileds []*meta.FieldDescription, filters map[string]interface{}, dbTransaction transactions.DbTransaction) ([]map[string]interface{}, error)
	PerformRemove(recordNode *RecordRemovalNode, dbTransaction transactions.DbTransaction, notificationPool *notifications.RecordSetNotificationPool, processor *Processor) (error)
	PrepareCreateOperation(m *meta.Meta, objs []map[string]interface{}) (transactions.Operation, error)
	PrepareUpdateOperation(m *meta.Meta, objs []map[string]interface{}) (transactions.Operation, error)
}

type Processor struct {
	metaStore   *meta.MetaStore
	dataManager DataManager
	vCache      map[string]objectClassValidator
}

func NewProcessor(m *meta.MetaStore, d DataManager) (*Processor, error) {
	return &Processor{metaStore: m, dataManager: d, vCache: make(map[string]objectClassValidator)}, nil
}

type SearchContext struct {
	depthLimit    int
	dm            DataManager
	lazyPath      string
	DbTransaction transactions.DbTransaction
}

func isBackLink(m *meta.Meta, f *meta.FieldDescription) bool {
	for i, _ := range m.Fields {
		if m.Fields[i].LinkType == description.LinkTypeOuter && m.Fields[i].OuterLinkField.Name == f.Name && m.Fields[i].LinkMeta.Name == f.Meta.Name {
			return true
		}
	}
	return false
}
func (processor *Processor) Get(transaction transactions.DbTransaction, objectClass, key string, depth int) (*Record, error) {
	if objectMeta, e := processor.GetMeta(transaction, objectClass); e != nil {
		return nil, e
	} else {
		if pk, e := objectMeta.Key.ValueFromString(key); e != nil {
			return nil, e
		} else {
			ctx := SearchContext{depthLimit: depth, dm: processor.dataManager, lazyPath: "/custodian/data/single", DbTransaction: transaction}

			root := &Node{KeyField: objectMeta.Key, Meta: objectMeta, ChildNodes: make(map[string]*Node), Depth: 1, OnlyLink: false, plural: false, Parent: nil, Type: NodeTypeRegular}
			root.RecursivelyFillChildNodes(ctx.depthLimit)

			if recordData, e := root.Resolve(ctx, pk); e != nil {
				return nil, e
			} else if recordData == nil {
				return nil, nil
			} else {
				recordData := recordData.(map[string]interface{})
				return NewRecord(objectMeta, root.FillRecordValues(recordData, ctx)), nil
			}
		}
	}
}

func (processor *Processor) GetBulk(transaction transactions.DbTransaction, objectName string, filter string, depth int, sink func(map[string]interface{}) error) error {
	if businessObject, ok, e := processor.metaStore.Get(&transactions.GlobalTransaction{DbTransaction: transaction}, objectName); e != nil {
		return e
	} else if !ok {
		return errors.NewDataError(objectName, errors.ErrObjectClassNotFound, "Object class '%s' not found", objectName)
	} else {
		searchContext := SearchContext{depthLimit: depth, dm: processor.dataManager, lazyPath: "/custodian/data/bulk", DbTransaction: transaction}
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
			record = root.FillRecordValues(record, searchContext)
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
		if f.LinkType == description.LinkTypeOuter {
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

func (processor *Processor) GetMeta(transaction transactions.DbTransaction, objectName string) (*meta.Meta, error) {
	objectMeta, ok, err := processor.metaStore.Get(&transactions.GlobalTransaction{DbTransaction: transaction}, objectName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.NewDataError(objectName, errors.ErrObjectClassNotFound, "Object class '%s' not found", objectName)
	}
	return objectMeta, nil
}

func (processor *Processor) CreateRecord(dbTransaction transactions.DbTransaction, objectName string, recordData map[string]interface{}, user auth.User) (retObj map[string]interface{}, err error) {
	// get Meta
	objectMeta, err := processor.GetMeta(dbTransaction, objectName)
	if err != nil {
		return nil, err
	}

	// extract processing node
	recordProcessingNode, err := new(RecordProcessingTreeBuilder).Build(&Record{Meta: objectMeta, Data: recordData}, processor, dbTransaction)
	if err != nil {
		return nil, err
	}

	// create notification pool
	recordSetNotificationPool := notifications.NewRecordSetNotificationPool()
	defer func() { recordSetNotificationPool.CompleteSend(err) }()

	//perform update
	rootRecordSet, recordSets := recordProcessingNode.RecordSets()

	// create records

	for _, recordSet := range recordSets {
		isRoot := recordSet == rootRecordSet
		if !recordSet.IsPhantom() {
			if _, err := processor.updateRecordSet(
				dbTransaction,
				recordSet,
				isRoot,
				recordSetNotificationPool,
			); err != nil {
				return nil, err
			} else {
				if isRoot {
					rootRecordSet = recordSet
				}
			}
		} else {
			if _, err := processor.createRecordSet(
				dbTransaction,
				recordSet,
				isRoot,
				recordSetNotificationPool,
			); err != nil {
				return nil, err
			} else {
				if isRoot {
					rootRecordSet = recordSet
				}
			}
		}
	}

	//it is important to CollapseLinks after all operations done, because intermediate calls may use inconsistent data
	for _, recordSet := range recordSets {
		recordSet.CollapseLinks()
	}

	// push notifications if needed
	if recordSetNotificationPool.ShouldBeProcessed() {
		//capture updated state of all records in the pool
		recordSetNotificationPool.CaptureCurrentState()
		recordSetNotificationPool.Push(user)
	}
	return recordData, nil
}

func (processor *Processor) BulkCreateRecords(dbTransaction transactions.DbTransaction, objectName string, next func() (map[string]interface{}, error), sink func(map[string]interface{}) error, user auth.User) (err error) {
	// get Meta
	objectMeta, err := processor.GetMeta(dbTransaction, objectName)
	if err != nil {
		return err
	}

	// create notification pool
	recordSetNotificationPool := notifications.NewRecordSetNotificationPool()
	defer func() { recordSetNotificationPool.CompleteSend(err) }()

	// collect records` data to update
	recordDataSet, err := processor.consumeRecordDataSet(next, objectMeta, false)
	if err != nil {
		return err
	}

	//assemble RecordSets
	var recordProcessingNode *RecordProcessingNode
	rootRecordSets := make([] *RecordSet, 0)
	for _, recordData := range recordDataSet {
		// extract processing node
		recordProcessingNode, err = new(RecordProcessingTreeBuilder).Build(&Record{Meta: objectMeta, Data: recordData}, processor, dbTransaction)
		if err != nil {
			return err
		}
		rootRecordSet, recordSets := recordProcessingNode.RecordSets()

		for _, recordSet := range recordSets {
			isRoot := recordSet == rootRecordSet
			recordSet.PrepareData()
			if !recordSet.IsPhantom() {
				if _, err := processor.updateRecordSet(
					dbTransaction,
					recordSet,
					isRoot,
					recordSetNotificationPool,
				); err != nil {
					return err
				} else {
					recordSet.MergeData()
					if isRoot {
						rootRecordSet = recordSet
					}
				}
			} else {
				if _, err := processor.createRecordSet(
					dbTransaction,
					recordSet,
					isRoot,
					recordSetNotificationPool,
				); err != nil {
					return err
				} else {
					recordSet.MergeData()
					if isRoot {
						rootRecordSet = recordSet
					}
				}
			}
		}
		rootRecordSets = append(rootRecordSets, rootRecordSet)

		//collapse links
		for _, recordSet := range recordSets {
			recordSet.CollapseLinks()
		}
	}

	if err != nil {
		return err
	}

	// feed updated data to the sink
	processor.feedRecordSets(rootRecordSets, sink)

	// push notifications if needed
	if recordSetNotificationPool.ShouldBeProcessed() {
		//capture updated state of all records in the pool
		recordSetNotificationPool.CaptureCurrentState()
		recordSetNotificationPool.Push(user)
	}

	return nil
}

func (processor *Processor) UpdateRecord(dbTransaction transactions.DbTransaction, objectName, key string, recordData map[string]interface{}, user auth.User) (updatedRecordData map[string]interface{}, err error) {
	// get Meta
	objectMeta, err := processor.GetMeta(dbTransaction, objectName)
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
	// extract processing node
	recordProcessingNode, err := new(RecordProcessingTreeBuilder).Build(&Record{Meta: objectMeta, Data: recordData}, processor, dbTransaction)
	if err != nil {
		return nil, err
	}

	// create notification pool
	recordSetNotificationPool := notifications.NewRecordSetNotificationPool()
	defer func() { recordSetNotificationPool.CompleteSend(err) }()

	//perform update
	rootRecordSet, recordSets := recordProcessingNode.RecordSets()

	for _, recordSet := range recordSets {
		isRoot := recordSet == rootRecordSet
		if !recordSet.IsPhantom() {
			if _, err := processor.updateRecordSet(
				dbTransaction,
				recordSet,
				isRoot,
				recordSetNotificationPool,
			); err != nil {
				return nil, err
			} else {
				if isRoot {
					rootRecordSet = recordSet
				}
			}
		} else {
			if _, err := processor.createRecordSet(
				dbTransaction,
				recordSet,
				isRoot,
				recordSetNotificationPool,
			); err != nil {
				return nil, err
			} else {
				if isRoot {
					rootRecordSet = recordSet
				}
			}
		}
	}
	//it is important to CollapseLinks after all operations done, because intermediate calls may use inconsistent data
	for _, recordSet := range recordSets {
		recordSet.CollapseLinks()
	}

	// push notifications if needed
	if recordSetNotificationPool.ShouldBeProcessed() {
		//capture updated state of all recordSetsSplitByObjects in the pool
		recordSetNotificationPool.CaptureCurrentState()
		recordSetNotificationPool.Push(user)
	}

	return rootRecordSet.Records[0].Data, nil
}

func (processor *Processor) BulkUpdateRecords(dbTransaction transactions.DbTransaction, objectName string, next func() (map[string]interface{}, error), sink func(map[string]interface{}) error, user auth.User) (err error) {

	//get Meta
	objectMeta, err := processor.GetMeta(dbTransaction, objectName)
	if err != nil {
		return err
	}

	// create notification pool
	recordSetNotificationPool := notifications.NewRecordSetNotificationPool()
	defer func() { recordSetNotificationPool.CompleteSend(err) }()

	// collect records` data to update
	recordDataSet, err := processor.consumeRecordDataSet(next, objectMeta, true)
	if err != nil {
		return err
	}

	//assemble RecordSets
	var recordProcessingNode *RecordProcessingNode
	rootRecordSets := make([] *RecordSet, 0)
	for _, recordData := range recordDataSet {
		// extract processing node
		recordProcessingNode, err = new(RecordProcessingTreeBuilder).Build(&Record{Meta: objectMeta, Data: recordData}, processor, dbTransaction)
		if err != nil {
			return err
		}
		rootRecordSet, recordSets := recordProcessingNode.RecordSets()

		for _, recordSet := range recordSets {
			isRoot := recordSet == rootRecordSet
			recordSet.PrepareData()
			if !recordSet.IsPhantom() {
				if _, err := processor.updateRecordSet(
					dbTransaction,
					recordSet,
					isRoot,
					recordSetNotificationPool,
				); err != nil {
					return err
				} else {
					recordSet.MergeData()
					if isRoot {
						rootRecordSet = recordSet
					}
				}
			} else {
				if _, err := processor.createRecordSet(
					dbTransaction,
					recordSet,
					isRoot,
					recordSetNotificationPool,
				); err != nil {
					return err
				} else {
					recordSet.MergeData()
					if isRoot {
						rootRecordSet = recordSet
					}
				}
			}
		}
		rootRecordSets = append(rootRecordSets, rootRecordSet)

		//collapse links
		for _, recordSet := range recordSets {
			recordSet.CollapseLinks()
		}
	}

	if err != nil {
		return err
	}

	// feed updated data to the sink
	processor.feedRecordSets(rootRecordSets, sink)

	//push notifications if needed
	if recordSetNotificationPool.ShouldBeProcessed() {
		//capture updated state of all records in the pool
		recordSetNotificationPool.CaptureCurrentState()
		recordSetNotificationPool.Push(user)
	}

	return nil

}

//TODO: Refactor this method similarly to UpdateRecord, so notifications could be tested properly, it should affect PrepareDeletes method
func (processor *Processor) RemoveRecord(dbTransaction transactions.DbTransaction, objectName string, key string, user auth.User) (map[string]interface{}, error) {
	var err error

	//get pk
	recordToRemove, err := processor.Get(dbTransaction, objectName, key, 1)
	if err != nil {
		return nil, err
	}
	if recordToRemove == nil {
		return nil, &errors.DataError{"RecordNotFound", "Record not found", objectName}
	}

	// create notification pool
	recordSetNotificationPool := notifications.NewRecordSetNotificationPool()
	defer func() { recordSetNotificationPool.CompleteSend(err) }()

	//fill node
	removalRootNode, err := new(RecordRemovalTreeBuilder).Extract(recordToRemove, processor, dbTransaction)
	if err != nil {
		return nil, err
	}

	err = processor.dataManager.PerformRemove(removalRootNode, dbTransaction, recordSetNotificationPool, processor)
	if err != nil {
		return nil, err
	}

	// push notifications if needed
	if recordSetNotificationPool.ShouldBeProcessed() {
		//capture updated state of all records in the pool
		recordSetNotificationPool.Push(user)
	}
	return removalRootNode.Data(), nil
}

//TODO: Refactor this method similarly to BulkUpdateRecords, so notifications could be tested properly, it should affect PrepareDeletes method
func (processor *Processor) BulkDeleteRecords(dbTransaction transactions.DbTransaction, objectName string, next func() (map[string]interface{}, error), user auth.User) (err error) {

	// get Meta
	objectMeta, err := processor.GetMeta(dbTransaction, objectName)
	if err != nil {
		return err
	}

	//prepare node
	root := &DNode{KeyField: objectMeta.Key, Meta: objectMeta, ChildNodes: make(map[string]*DNode), Plural: false}
	root.recursivelyFillOuterChildNodes()

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

	//var op transactions.Operation
	//if op, keys, err = processor.dataManager.PerformRemove(root, keys, dbTransaction); err != nil {
	//	return err
	//} else {
	//	ops := []transactions.Operation{op}
	//	for t2d := []tuple2d{tuple2d{root, keys}}; len(t2d) > 0; t2d = t2d[1:] {
	//
	//		for _, v := range t2d[0].n.ChildNodes {
	//			if len(t2d[0].keys) > 0 {
	//				if op, keys, err = processor.dataManager.PrepareDeletes(v, t2d[0].keys, dbTransaction); err != nil {
	//					return err
	//				} else {
	//					for _, primaryKeyValue := range t2d[0].keys {
	//
	//						// create notification, capture current recordData state and Add notification to notification pool
	//						recordSetNotification := notifications.NewRecordSetNotification(dbTransaction, &RecordSet{Meta: root.Meta, DataSet: []map[string]interface{}{{v.KeyField.Name: primaryKeyValue}}}, false, description.MethodRemove, processor.GetBulk, processor.Get)
	//						if recordSetNotification.ShouldBeProcessed() {
	//							recordSetNotification.CapturePreviousState()
	//							recordSetNotificationPool.Add(recordSetNotification)
	//						}
	//
	//					}
	//
	//					ops = append(ops, op)
	//					t2d = append(t2d, tuple2d{v, keys})
	//				}
	//			}
	//		}
	//	}
	//	for i := 0; i < len(ops)>>2; i++ {
	//		ops[i], ops[len(ops)-1] = ops[len(ops)-1], ops[i]
	//	}
	//	if err = dbTransaction.Execute(ops); err != nil {
	//		return err
	//	}
	//}

	// push notifications if needed
	if recordSetNotificationPool.ShouldBeProcessed() {
		//capture updated state of all records in the pool
		recordSetNotificationPool.CaptureCurrentState()
		recordSetNotificationPool.Push(user)
	}

	return nil
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
		for _, recordData := range recordsSet.Data() {
			if err := sink(recordData); err != nil {
				return err
			}
		}
	}
	return nil
}

// perform update and return list of records
func (processor *Processor) updateRecordSet(dbTransaction transactions.DbTransaction, recordSet *RecordSet, isRoot bool, recordSetNotificationPool *notifications.RecordSetNotificationPool) (*RecordSet, error) {
	recordSet.PrepareData()
	// create notification, capture current recordData state and Add notification to notification pool
	recordSetNotification := notifications.NewRecordSetNotification(dbTransaction, recordSet, isRoot, description.MethodUpdate, processor.GetBulk, processor.Get)
	if recordSetNotification.ShouldBeProcessed() {
		recordSetNotification.CapturePreviousState()
		recordSetNotificationPool.Add(recordSetNotification)
	}

	var operations = make([]transactions.Operation, 0)

	if operation, e := processor.dataManager.PrepareUpdateOperation(recordSet.Meta, recordSet.RawData()); e != nil {
		return nil, e
	} else {
		operations = append(operations, operation)
	}
	//
	if e := dbTransaction.Execute(operations); e != nil {
		return nil, e
	}
	recordSet.MergeData()
	return recordSet, nil
}

// perform create and return list of records
func (processor *Processor) createRecordSet(dbTransaction transactions.DbTransaction, recordSet *RecordSet, isRoot bool, recordSetNotificationPool *notifications.RecordSetNotificationPool) (*RecordSet, error) {
	recordSet.PrepareData()
	// create notification, capture current recordData state and Add notification to notification pool
	recordSetNotification := notifications.NewRecordSetNotification(dbTransaction, recordSet, isRoot, description.MethodCreate, processor.GetBulk, processor.Get)
	if recordSetNotification.ShouldBeProcessed() {
		recordSetNotification.CapturePreviousState()
		recordSetNotificationPool.Add(recordSetNotification)
	}

	var operations = make([]transactions.Operation, 0)

	if operation, e := processor.dataManager.PrepareCreateOperation(recordSet.Meta, recordSet.RawData()); e != nil {
		return nil, e
	} else {
		operations = append(operations, operation)
	}
	//

	if e := dbTransaction.Execute(operations); e != nil {
		return nil, e
	}
	recordSet.MergeData()
	return recordSet, nil
}
