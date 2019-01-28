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
	GetRql(dataNode *Node, rqlNode *rqlParser.RqlRootNode, fields []*meta.FieldDescription, dbTransaction transactions.DbTransaction) ([]map[string]interface{}, int, error)
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
	omitOuters    bool
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
func (processor *Processor) Get(transaction transactions.DbTransaction, objectClass, key string, depth int, omitOuters bool) (*Record, error) {
	if objectMeta, e := processor.GetMeta(transaction, objectClass); e != nil {
		return nil, e
	} else {
		if pk, e := objectMeta.Key.ValueFromString(key); e != nil {
			return nil, e
		} else {
			ctx := SearchContext{depthLimit: depth, dm: processor.dataManager, lazyPath: "/custodian/data/single", DbTransaction: transaction, omitOuters: omitOuters}

			root := &Node{KeyField: objectMeta.Key, Meta: objectMeta, ChildNodes: make(map[string]*Node), Depth: 1, OnlyLink: false, plural: false, Parent: nil, Type: NodeTypeRegular}
			root.RecursivelyFillChildNodes(ctx.depthLimit, description.FieldModeRetrieve)

			if recordData, e := root.Resolve(ctx, pk); e != nil {
				return nil, e
			} else if recordData == nil {
				return nil, nil
			} else {
				return NewRecord(objectMeta, root.FillRecordValues(recordData.(map[string]interface{}), ctx)), nil
			}
		}
	}
}

func (processor *Processor) GetBulk(transaction transactions.DbTransaction, objectName string, filter string, depth int, omitOuters bool, sink func(map[string]interface{}) error) (int, error) {
	if businessObject, ok, e := processor.metaStore.Get(&transactions.GlobalTransaction{DbTransaction: transaction}, objectName, true); e != nil {
		return 0, e
	} else if !ok {
		return 0, errors.NewDataError(objectName, errors.ErrObjectClassNotFound, "Object class '%s' not found", objectName)
	} else {
		searchContext := SearchContext{depthLimit: depth, dm: processor.dataManager, lazyPath: "/custodian/data/bulk", DbTransaction: transaction, omitOuters: omitOuters}
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
		root.RecursivelyFillChildNodes(searchContext.depthLimit, description.FieldModeRetrieve)

		parser := rqlParser.NewParser()
		rqlNode, err := parser.Parse(strings.NewReader(filter))
		if err != nil {
			return 0, errors.NewDataError(objectName, errors.ErrWrongRQL, err.Error())
		}

		records, recordsCount, e := root.ResolveByRql(searchContext, rqlNode)

		if e != nil {
			return recordsCount, e
		}
		for _, record := range records {
			record = root.FillRecordValues(record, searchContext)
			sink(record)
		}
		return recordsCount, nil
	}
}

//Todo: this method is a shadow of GetBulk, the only difference is that it gets Meta object, not meta`s name
//perhaps it should become public and replace current GetBulk
func (processor *Processor) ShadowGetBulk(transaction transactions.DbTransaction, metaObj *meta.Meta, filter string, depth int, omitOuters bool, sink func(map[string]interface{}) error) (int, error) {
	searchContext := SearchContext{depthLimit: depth, dm: processor.dataManager, lazyPath: "/custodian/data/bulk", DbTransaction: transaction, omitOuters: omitOuters}
	root := &Node{
		KeyField:   metaObj.Key,
		Meta:       metaObj,
		ChildNodes: make(map[string]*Node),
		Depth:      1,
		OnlyLink:   false,
		plural:     false,
		Parent:     nil,
		Type:       NodeTypeRegular,
	}
	root.RecursivelyFillChildNodes(searchContext.depthLimit, description.FieldModeRetrieve)

	parser := rqlParser.NewParser()
	rqlNode, err := parser.Parse(strings.NewReader(filter))
	if err != nil {
		return 0, errors.NewDataError(metaObj.Name, errors.ErrWrongRQL, err.Error())
	}

	records, recordsCount, e := root.ResolveByRql(searchContext, rqlNode)

	if e != nil {
		return recordsCount, e
	}
	for _, record := range records {
		record = root.FillRecordValues(record, searchContext)
		sink(record)
	}
	return recordsCount, nil
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
	objectMeta, ok, err := processor.metaStore.Get(&transactions.GlobalTransaction{DbTransaction: transaction}, objectName, true)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.NewDataError(objectName, errors.ErrObjectClassNotFound, "Object class '%s' not found", objectName)
	}
	return objectMeta, nil
}

func (processor *Processor) CreateRecord(dbTransaction transactions.DbTransaction, objectName string, recordData map[string]interface{}, user auth.User) (*Record, error) {
	// get MetaDescription
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
	rootRecordSet, recordSetsOperations := recordProcessingNode.RecordSetOperations()

	// create records

	for _, recordSetOperation := range recordSetsOperations {
		isRoot := recordSetOperation.RecordSet == rootRecordSet
		if recordSetOperation.Type == RecordOperationTypeCreate || isRoot {
			if _, err := processor.createRecordSet(
				dbTransaction,
				recordSetOperation.RecordSet,
				isRoot,
				recordSetNotificationPool,
			); err != nil {
				return nil, err
			}
		} else if recordSetOperation.Type == RecordOperationTypeUpdate {
			if _, err := processor.updateRecordSet(
				dbTransaction,
				recordSetOperation.RecordSet,
				isRoot,
				recordSetNotificationPool,
			); err != nil {
				return nil, err
			}
		} else if recordSetOperation.Type == RecordOperationTypeRemove {
			for _, record := range recordSetOperation.RecordSet.Records {
				recordPkAsStr, _ := record.Meta.Key.ValueAsString(record.Pk())
				if _, err := processor.RemoveRecord(
					dbTransaction,
					record.Meta.Name,
					recordPkAsStr,
					user,
				); err != nil {
					return nil, err
				}
			}
		} else if recordSetOperation.Type == RecordOperationTypeRetrive {
			for i, record := range recordSetOperation.RecordSet.Records {
				retrievedRecord, err := processor.Get(dbTransaction, record.Meta.Name, record.PkAsString(), 1, true)
				if err != nil {
					return nil, err
				} else {
					retrievedRecord.Links = record.Links
					recordSetOperation.RecordSet.Records[i] = retrievedRecord
				}
			}
		}
	}

	//it is important to CollapseLinks after all operations done, because intermediate calls may use inconsistent data
	for _, recordSetOperation := range recordSetsOperations {
		if recordSetOperation.Type != RecordOperationTypeRemove {
			recordSetOperation.RecordSet.CollapseLinks()
		}
	}

	// push notifications if needed
	if recordSetNotificationPool.ShouldBeProcessed() {
		//capture updated state of all records in the pool
		recordSetNotificationPool.CaptureCurrentState()
		recordSetNotificationPool.Push(user)
	}
	return NewRecord(objectMeta, recordData), nil
}

func (processor *Processor) BulkCreateRecords(dbTransaction transactions.DbTransaction, objectName string, next func() (map[string]interface{}, error), sink func(map[string]interface{}) error, user auth.User) (err error) {
	// get MetaDescription
	objectMeta, err := processor.GetMeta(dbTransaction, objectName)
	if err != nil {
		return err
	}

	// create notification pool
	recordSetNotificationPool := notifications.NewRecordSetNotificationPool()
	defer func() { recordSetNotificationPool.CompleteSend(err) }()

	// collect records` data to update
	records, err := processor.consumeRecords(next, objectMeta, false)
	if err != nil {
		return err
	}

	//assemble RecordSetOperations
	var recordProcessingNode *RecordProcessingNode
	rootRecordSets := make([] *RecordSet, 0)
	for _, record := range records {
		// extract processing node
		recordProcessingNode, err = new(RecordProcessingTreeBuilder).Build(record, processor, dbTransaction)
		if err != nil {
			return err
		}
		rootRecordSet, recordSetOperations := recordProcessingNode.RecordSetOperations()

		for _, recordSetOperation := range recordSetOperations {
			isRoot := recordSetOperation.RecordSet == rootRecordSet
			if recordSetOperation.Type == RecordOperationTypeUpdate {
				if _, err := processor.updateRecordSet(
					dbTransaction,
					recordSetOperation.RecordSet,
					isRoot,
					recordSetNotificationPool,
				); err != nil {
					return err
				}
			} else if recordSetOperation.Type == RecordOperationTypeCreate {
				if _, err := processor.createRecordSet(
					dbTransaction,
					recordSetOperation.RecordSet,
					isRoot,
					recordSetNotificationPool,
				); err != nil {
					return err
				}
			} else if recordSetOperation.Type == RecordOperationTypeRemove {
				for _, record := range recordSetOperation.RecordSet.Records {
					recordPkAsStr, _ := record.Meta.Key.ValueAsString(record.Pk())
					if _, err := processor.RemoveRecord(
						dbTransaction,
						record.Meta.Name,
						recordPkAsStr,
						user,
					); err != nil {
						return err
					}
				}

			}
		}
		rootRecordSets = append(rootRecordSets, rootRecordSet)

		//it is important to CollapseLinks after all operations done, because intermediate calls may use inconsistent data
		for _, recordSetOperation := range recordSetOperations {
			if recordSetOperation.Type != RecordOperationTypeRemove {
				recordSetOperation.RecordSet.CollapseLinks()
			}
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

func (processor *Processor) UpdateRecord(dbTransaction transactions.DbTransaction, objectName, key string, recordData map[string]interface{}, user auth.User) (updatedRecord *Record, err error) {
	// get MetaDescription
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
	rootRecordSet, recordSetOperations := recordProcessingNode.RecordSetOperations()

	for _, recordSetOperation := range recordSetOperations {
		isRoot := recordSetOperation.RecordSet == rootRecordSet
		if recordSetOperation.Type == RecordOperationTypeUpdate {
			if _, err := processor.updateRecordSet(
				dbTransaction,
				recordSetOperation.RecordSet,
				isRoot,
				recordSetNotificationPool,
			); err != nil {
				return nil, err
			}
		} else if recordSetOperation.Type == RecordOperationTypeCreate {
			if _, err := processor.createRecordSet(
				dbTransaction,
				recordSetOperation.RecordSet,
				isRoot,
				recordSetNotificationPool,
			); err != nil {
				return nil, err
			}
		} else if recordSetOperation.Type == RecordOperationTypeRemove {
			for _, record := range recordSetOperation.RecordSet.Records {
				recordPkAsStr, _ := record.Meta.Key.ValueAsString(record.Pk())
				if _, err := processor.RemoveRecord(
					dbTransaction,
					record.Meta.Name,
					recordPkAsStr,
					user,
				); err != nil {
					return nil, err
				}
			}

		} else if recordSetOperation.Type == RecordOperationTypeRetrive {
			for i, record := range recordSetOperation.RecordSet.Records {
				retrievedRecord, err := processor.Get(dbTransaction, record.Meta.Name, record.PkAsString(), 1, true)
				if err != nil {
					return nil, err
				} else {
					retrievedRecord.Links = record.Links
					recordSetOperation.RecordSet.Records[i] = retrievedRecord
				}
			}
		}
	}
	//it is important to CollapseLinks after all operations done, because intermediate calls may use inconsistent data
	for _, recordSetOperation := range recordSetOperations {
		if recordSetOperation.Type != RecordOperationTypeRemove {
			recordSetOperation.RecordSet.CollapseLinks()
		}
	}
	// push notifications if needed
	if recordSetNotificationPool.ShouldBeProcessed() {
		//capture updated state of all recordSetsSplitByObjects in the pool
		recordSetNotificationPool.CaptureCurrentState()
		recordSetNotificationPool.Push(user)
	}

	return rootRecordSet.Records[0], nil
}

func (processor *Processor) BulkUpdateRecords(dbTransaction transactions.DbTransaction, objectName string, next func() (map[string]interface{}, error), sink func(map[string]interface{}) error, user auth.User) (err error) {

	//get MetaDescription
	objectMeta, err := processor.GetMeta(dbTransaction, objectName)
	if err != nil {
		return err
	}

	// create notification pool
	recordSetNotificationPool := notifications.NewRecordSetNotificationPool()
	defer func() { recordSetNotificationPool.CompleteSend(err) }()

	// collect records` data to update
	records, err := processor.consumeRecords(next, objectMeta, true)
	if err != nil {
		return err
	}

	//assemble RecordSetOperations
	var recordProcessingNode *RecordProcessingNode
	rootRecordSets := make([] *RecordSet, 0)
	for _, record := range records {
		// extract processing node
		recordProcessingNode, err = new(RecordProcessingTreeBuilder).Build(record, processor, dbTransaction)
		if err != nil {
			return err
		}
		//perform update
		rootRecordSet, recordSetOperations := recordProcessingNode.RecordSetOperations()

		for _, recordSetOperation := range recordSetOperations {
			isRoot := recordSetOperation.RecordSet == rootRecordSet
			if recordSetOperation.Type == RecordOperationTypeUpdate {
				if _, err := processor.updateRecordSet(
					dbTransaction,
					recordSetOperation.RecordSet,
					isRoot,
					recordSetNotificationPool,
				); err != nil {
					return err
				}
			} else if recordSetOperation.Type == RecordOperationTypeCreate {
				if _, err := processor.createRecordSet(
					dbTransaction,
					recordSetOperation.RecordSet,
					isRoot,
					recordSetNotificationPool,
				); err != nil {
					return err
				}
			} else if recordSetOperation.Type == RecordOperationTypeRemove {
				for _, record := range recordSetOperation.RecordSet.Records {
					recordPkAsStr, _ := record.Meta.Key.ValueAsString(record.Pk())
					if _, err := processor.RemoveRecord(
						dbTransaction,
						record.Meta.Name,
						recordPkAsStr,
						user,
					); err != nil {
						return err
					}
				}

			}
		}
		rootRecordSets = append(rootRecordSets, rootRecordSet)

		//it is important to CollapseLinks after all operations done, because intermediate calls may use inconsistent data
		for _, recordSetOperation := range recordSetOperations {
			if recordSetOperation.Type != RecordOperationTypeRemove {
				recordSetOperation.RecordSet.CollapseLinks()
			}
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
	recordToRemove, err := processor.Get(dbTransaction, objectName, key, 1, false)
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
	// get MetaDescription
	objectMeta, err := processor.GetMeta(dbTransaction, objectName)
	if err != nil {
		return err
	}
	//

	// create notification pool
	recordSetNotificationPool := notifications.NewRecordSetNotificationPool()
	defer func() { recordSetNotificationPool.CompleteSend(err) }()

	// collect records` data to update
	records, err := processor.consumeRecords(next, objectMeta, true)
	if err != nil {
		return err
	}
	for _, record := range records {

		//get pk
		recordToRemove, err := processor.Get(dbTransaction, objectName, record.PkAsString(), 1, false)
		if err != nil {
			return err
		}
		if recordToRemove == nil {
			return &errors.DataError{"RecordNotFound", "Record not found", objectName}
		}

		//fill node
		removalRootNode, err := new(RecordRemovalTreeBuilder).Extract(recordToRemove, processor, dbTransaction)
		if err != nil {
			return err
		}

		err = processor.dataManager.PerformRemove(removalRootNode, dbTransaction, recordSetNotificationPool, processor)
		if err != nil {
			return err
		}

		// push notifications if needed
		if recordSetNotificationPool.ShouldBeProcessed() {
			//capture updated state of all records in the pool
			recordSetNotificationPool.Push(user)
		}

	}

	// push notifications if needed
	if recordSetNotificationPool.ShouldBeProcessed() {
		//capture updated state of all records in the pool
		recordSetNotificationPool.CaptureCurrentState()
		recordSetNotificationPool.Push(user)
	}

	return nil
}

//consume all records from callback function
func (processor *Processor) consumeRecords(nextCallback func() (map[string]interface{}, error), objectMeta *meta.Meta, strictPkCheck bool) ([]*Record, error) {
	var records = make([]*Record, 0)
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

		records = append(records, NewRecord(objectMeta, recordData))
	}
	return records, nil
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
