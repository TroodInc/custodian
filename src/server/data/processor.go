package data

import (
	"github.com/Q-CIS-DEV/go-rql-parser"
	"server/auth"
	"server/data/errors"
	"server/data/notifications"
	. "server/data/record"
	"server/object/description"
	"server/object/meta"
	"server/transactions"
	"strings"
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
	PerformRemove(recordNode *RecordRemovalNode, dbTransaction transactions.DbTransaction, processor *Processor) (error)
	PrepareCreateOperation(m *meta.Meta, objs []map[string]interface{}) (transactions.Operation, error)
	PrepareUpdateOperation(m *meta.Meta, objs []map[string]interface{}) (transactions.Operation, error)
}

type Processor struct {
	metaStore                 *meta.MetaStore
	dataManager               DataManager
	transactionManager        transactions.DbTransactionManager
	vCache                    map[string]objectClassValidator
	RecordSetNotificationPool *notifications.RecordSetNotificationPool
}

func NewProcessor(m *meta.MetaStore, d DataManager, t transactions.DbTransactionManager) (*Processor, error) {
	recordSetNotificationPool := notifications.NewRecordSetNotificationPool(m.GetActions())
	return &Processor{
		m,
		d,
		t,
		make(map[string]objectClassValidator),
		recordSetNotificationPool,
	}, nil
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
func (processor *Processor) Get(objectClass, key string, includePaths []string, excludePaths []string, depth int, omitOuters bool) (*Record, error) {
	if objectMeta, e := processor.GetMeta(objectClass); e != nil {
		return nil, e
	} else {
		if pk, e := objectMeta.Key.ValueFromString(key); e != nil {
			return nil, e
		} else {
			transaction, _ := processor.transactionManager.BeginTransaction()
			ctx := SearchContext{depthLimit: depth, dm: processor.dataManager, lazyPath: "/custodian/data", DbTransaction: transaction, omitOuters: omitOuters}

			//
			root := &Node{
				KeyField:       objectMeta.Key,
				Meta:           objectMeta,
				ChildNodes:     *NewChildNodes(),
				Depth:          1,
				OnlyLink:       false,
				plural:         false,
				Parent:         nil,
				Type:           NodeTypeRegular,
				SelectFields:   *NewSelectFields(objectMeta.Key, objectMeta.TableFields()),
				RetrievePolicy: new(AggregatedRetrievePolicyFactory).Factory(includePaths, excludePaths),
			}

			err := root.RecursivelyFillChildNodes(ctx.depthLimit, description.FieldModeRetrieve)
			if err != nil {
				processor.transactionManager.RollbackTransaction(transaction)
				return nil, err
			}

			if recordData, e := root.Resolve(ctx, pk); e != nil {
				processor.transactionManager.RollbackTransaction(transaction)
				return nil, e
			} else if recordData == nil {
				processor.transactionManager.RollbackTransaction(transaction)
				return nil, nil
			} else {
				processor.transactionManager.CommitTransaction(transaction)
				return recordData, nil
			}
		}
	}
}

func (processor *Processor) GetBulk(objectName string, filter string, includePaths []string, excludePaths []string, depth int, omitOuters bool) (int, []*Record, error) {
	if businessObject, ok, e := processor.metaStore.Get(objectName, true); e != nil {
		return 0, nil, e
	} else if !ok {
		return 0, nil, errors.NewDataError(objectName, errors.ErrObjectClassNotFound, "Object class '%s' not found", objectName)
	} else {
		transaction, _ := processor.transactionManager.BeginTransaction()
		searchContext := SearchContext{depthLimit: depth, dm: processor.dataManager, lazyPath: "/custodian/data/bulk", DbTransaction: transaction, omitOuters: omitOuters}

		//make and apply retrieves policy
		retrievePolicy := new(AggregatedRetrievePolicyFactory).Factory(includePaths, excludePaths)
		//
		root := &Node{
			KeyField:       businessObject.Key,
			Meta:           businessObject,
			ChildNodes:     *NewChildNodes(),
			Depth:          1,
			OnlyLink:       false,
			plural:         false,
			Parent:         nil,
			Type:           NodeTypeRegular,
			SelectFields:   *NewSelectFields(businessObject.Key, businessObject.TableFields()),
			RetrievePolicy: retrievePolicy,
		}
		root.RecursivelyFillChildNodes(searchContext.depthLimit, description.FieldModeRetrieve)

		parser := rqlParser.NewParser()

		rqlNode, err := parser.Parse(strings.NewReader(filter))
		if err != nil {
			processor.transactionManager.RollbackTransaction(transaction)
			return 0, nil, errors.NewDataError(objectName, errors.ErrWrongRQL, err.Error())
		}

		records, recordsCount, e := root.ResolveByRql(searchContext, rqlNode)

		if e != nil {
			processor.transactionManager.RollbackTransaction(transaction)
			return recordsCount, nil, e
		}
		processor.transactionManager.CommitTransaction(transaction)
		return recordsCount, records, nil
	}
}

//Todo: this method is a shadow of GetBulk, the only difference is that it gets Meta object, not meta`s name
//perhaps it should become public and replace current GetBulk
func (processor *Processor) ShadowGetBulk(transaction transactions.DbTransaction, metaObj *meta.Meta, filter string, depth int, omitOuters bool, sink func(record *Record) error) (int, error) {
	searchContext := SearchContext{depthLimit: depth, dm: processor.dataManager, lazyPath: "/custodian/data/bulk", DbTransaction: transaction, omitOuters: omitOuters}
	root := &Node{
		KeyField:     metaObj.Key,
		Meta:         metaObj,
		ChildNodes:   *NewChildNodes(),
		Depth:        1,
		OnlyLink:     false,
		plural:       false,
		Parent:       nil,
		Type:         NodeTypeRegular,
		SelectFields: *NewSelectFields(metaObj.Key, metaObj.TableFields()),
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

func (processor *Processor) GetMeta(objectName string) (*meta.Meta, error) {
	objectMeta, ok, err := processor.metaStore.Get(objectName, true)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.NewDataError(objectName, errors.ErrObjectClassNotFound, "Object class '%s' not found", objectName)
	}
	return objectMeta, nil
}

func (processor *Processor) CreateRecord(objectName string, recordData map[string]interface{}, user auth.User) (*Record, error) {
	// get MetaDescription
	objectMeta, err := processor.GetMeta(objectName)
	if err != nil {
		return nil, err
	}

	// extract processing node
	dbTransaction, err := processor.transactionManager.BeginTransaction()
	recordProcessingNode, err := new(RecordProcessingTreeBuilder).Build(&Record{Meta: objectMeta, Data: recordData}, processor, dbTransaction)
	if err != nil {
		processor.transactionManager.RollbackTransaction(dbTransaction)
		return nil, err
	}

	// create notification pool
	defer func() { processor.RecordSetNotificationPool.CompleteSend(err) }()

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
			); err != nil {
				processor.transactionManager.RollbackTransaction(dbTransaction)
				return nil, err
			}
		} else if recordSetOperation.Type == RecordOperationTypeUpdate {
			if _, err := processor.updateRecordSet(
				dbTransaction,
				recordSetOperation.RecordSet,
				isRoot,
			); err != nil {
				processor.transactionManager.RollbackTransaction(dbTransaction)
				return nil, err
			}
		} else if recordSetOperation.Type == RecordOperationTypeRemove {
			for _, record := range recordSetOperation.RecordSet.Records {
				recordPkAsStr, _ := record.Meta.Key.ValueAsString(record.Pk())
				if _, err := processor.RemoveRecord(
					record.Meta.Name,
					recordPkAsStr,
					user,
				); err != nil {
					processor.transactionManager.RollbackTransaction(dbTransaction)
					return nil, err
				}
			}
		} else if recordSetOperation.Type == RecordOperationTypeRetrieve {
			for i, record := range recordSetOperation.RecordSet.Records {
				retrievedRecord, err := processor.Get(record.Meta.Name, record.PkAsString(), nil, nil, 1, true)
				if err != nil {
					processor.transactionManager.RollbackTransaction(dbTransaction)
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

	processor.transactionManager.CommitTransaction(dbTransaction)

	// push notifications if needed
	if processor.RecordSetNotificationPool.ShouldBeProcessed() {
		//capture updated state of all records in the pool
		processor.RecordSetNotificationPool.Push(user)
	}
	return NewRecord(objectMeta, recordData), nil
}

func (processor *Processor) BulkCreateRecords(objectName string, next func() (map[string]interface{}, error), user auth.User) ([]*Record, error) {

	// get MetaDescription
	objectMeta, err := processor.GetMeta(objectName)
	if err != nil {
		return nil, err
	}

	// create notification pool
	defer func() { processor.RecordSetNotificationPool.CompleteSend(err) }()

	// collect records` data to update
	records, err := processor.consumeRecords(next, objectMeta, false)
	if err != nil {
		return nil, err
	}

	//assemble RecordSetOperations
	var recordProcessingNode *RecordProcessingNode
	rootRecordSets := make([]interface{}, 0)
	dbTransaction, err := processor.transactionManager.BeginTransaction()
	for _, record := range records {
		// extract processing node
		recordProcessingNode, err = new(RecordProcessingTreeBuilder).Build(record, processor, dbTransaction)
		if err != nil {
			processor.transactionManager.RollbackTransaction(dbTransaction)
			return nil, err
		}
		rootRecordSet, recordSetOperations := recordProcessingNode.RecordSetOperations()

		for _, recordSetOperation := range recordSetOperations {
			isRoot := recordSetOperation.RecordSet == rootRecordSet
			if recordSetOperation.Type == RecordOperationTypeUpdate {
				if _, err := processor.updateRecordSet(
					dbTransaction,
					recordSetOperation.RecordSet,
					isRoot,
				); err != nil {
					processor.transactionManager.RollbackTransaction(dbTransaction)
					return nil, err
				}
			} else if recordSetOperation.Type == RecordOperationTypeCreate {
				if _, err := processor.createRecordSet(
					dbTransaction,
					recordSetOperation.RecordSet,
					isRoot,
				); err != nil {
					processor.transactionManager.RollbackTransaction(dbTransaction)
					return nil, err
				}
			} else if recordSetOperation.Type == RecordOperationTypeRemove {
				for _, record := range recordSetOperation.RecordSet.Records {
					recordPkAsStr, _ := record.Meta.Key.ValueAsString(record.Pk())
					if _, err := processor.RemoveRecord(
						record.Meta.Name,
						recordPkAsStr,
						user,
					); err != nil {
						processor.transactionManager.RollbackTransaction(dbTransaction)
						return nil, err
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

	var result []*Record
	for _, rs := range rootRecordSets {
		result = append(result, rs.(*RecordSet).Records...)
	}

	if err != nil {
		processor.transactionManager.RollbackTransaction(dbTransaction)
		return nil, err
	}

	processor.transactionManager.CommitTransaction(dbTransaction)

	// push notifications if needed
	if processor.RecordSetNotificationPool.ShouldBeProcessed() {
		//capture updated state of all records in the pool
		processor.RecordSetNotificationPool.Push(user)
	}

	return result, nil
}

func (processor *Processor) UpdateRecord(objectName, key string, recordData map[string]interface{}, user auth.User) (updatedRecord *Record, err error) {
	dbTransaction, _ := processor.transactionManager.BeginTransaction()

	// get MetaDescription
	objectMeta, err := processor.GetMeta(objectName)
	if err != nil {
		processor.transactionManager.RollbackTransaction(dbTransaction)
		return nil, err
	}
	// get and fill key value
	if pkValue, e := objectMeta.Key.ValueFromString(key); e != nil {
		processor.transactionManager.RollbackTransaction(dbTransaction)
		return nil, e
	} else {
		//recordData data must contain valid recordData`s PK value
		recordData[objectMeta.Key.Name] = pkValue
	}
	// extract processing node
	recordProcessingNode, err := new(RecordProcessingTreeBuilder).Build(&Record{Meta: objectMeta, Data: recordData}, processor, dbTransaction)
	if err != nil {
		processor.transactionManager.RollbackTransaction(dbTransaction)
		return nil, err
	}

	// create notification pool
	defer func() { processor.RecordSetNotificationPool.CompleteSend(err) }()

	//perform update
	rootRecordSet, recordSetOperations := recordProcessingNode.RecordSetOperations()

	for _, recordSetOperation := range recordSetOperations {
		isRoot := recordSetOperation.RecordSet == rootRecordSet
		if recordSetOperation.Type == RecordOperationTypeUpdate {
			if _, err := processor.updateRecordSet(
				dbTransaction,
				recordSetOperation.RecordSet,
				isRoot,
			); err != nil {
				processor.transactionManager.RollbackTransaction(dbTransaction)
				return nil, err
			}
		} else if recordSetOperation.Type == RecordOperationTypeCreate {
			if _, err := processor.createRecordSet(
				dbTransaction,
				recordSetOperation.RecordSet,
				isRoot,
			); err != nil {
				processor.transactionManager.RollbackTransaction(dbTransaction)
				return nil, err
			}
		} else if recordSetOperation.Type == RecordOperationTypeRemove {
			for _, record := range recordSetOperation.RecordSet.Records {
				recordPkAsStr, _ := record.Meta.Key.ValueAsString(record.Pk())
				if _, err := processor.RemoveRecord(
					record.Meta.Name,
					recordPkAsStr,
					user,
				); err != nil {
					processor.transactionManager.RollbackTransaction(dbTransaction)
					return nil, err
				}
			}

		} else if recordSetOperation.Type == RecordOperationTypeRetrieve {
			for i, record := range recordSetOperation.RecordSet.Records {
				retrievedRecord, err := processor.Get(record.Meta.Name, record.PkAsString(), nil, nil, 1, true)
				if err != nil {
					processor.transactionManager.RollbackTransaction(dbTransaction)
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

	processor.transactionManager.CommitTransaction(dbTransaction)

	// push notifications if needed
	if processor.RecordSetNotificationPool.ShouldBeProcessed() {
		//capture updated state of all recordSetsSplitByObjects in the pool
		processor.RecordSetNotificationPool.Push(user)
	}

	return rootRecordSet.Records[0], nil
}

func (processor *Processor) BulkUpdateRecords(dbTransaction transactions.DbTransaction, objectName string, next func() (map[string]interface{}, error), sink func(map[string]interface{}) error, user auth.User) (err error) {

	//get MetaDescription
	objectMeta, err := processor.GetMeta(objectName)
	if err != nil {
		return err
	}

	// create notification pool
	defer func() { processor.RecordSetNotificationPool.CompleteSend(err) }()

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
				); err != nil {
					return err
				}
			} else if recordSetOperation.Type == RecordOperationTypeCreate {
				if _, err := processor.createRecordSet(
					dbTransaction,
					recordSetOperation.RecordSet,
					isRoot,
				); err != nil {
					return err
				}
			} else if recordSetOperation.Type == RecordOperationTypeRemove {
				for _, record := range recordSetOperation.RecordSet.Records {
					recordPkAsStr, _ := record.Meta.Key.ValueAsString(record.Pk())
					if _, err := processor.RemoveRecord(
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
	if processor.RecordSetNotificationPool.ShouldBeProcessed() {
		//capture updated state of all records in the pool
		processor.RecordSetNotificationPool.Push(user)
	}

	return nil

}

//TODO: Refactor this method similarly to UpdateRecord, so notifications could be tested properly, it should affect PrepareDeletes method
func (processor *Processor) RemoveRecord(objectName string, key string, user auth.User) (map[string]interface{}, error) {
	var err error

	//get pk
	recordToRemove, err := processor.Get(objectName, key, nil, nil, 1, false)
	if err != nil {
		return nil, err
	}
	if recordToRemove == nil {
		return nil, &errors.DataError{"RecordNotFound", "Record not found", objectName}
	}

	//fill node
	dbTransaction, err := processor.transactionManager.BeginTransaction()
	removalRootNode, err := new(RecordRemovalTreeBuilder).Extract(recordToRemove, processor, dbTransaction)
	if err != nil {
		processor.transactionManager.RollbackTransaction(dbTransaction)
		return nil, err
	}


	err = processor.dataManager.PerformRemove(removalRootNode, dbTransaction, processor)
	if err != nil {
		processor.transactionManager.RollbackTransaction(dbTransaction)
		return nil, err
	}

	processor.transactionManager.CommitTransaction(dbTransaction)

	processor.RecordSetNotificationPool.Add(
		notifications.NewRecordNotification(
			recordToRemove.Meta.Name, notifications.MethodRemove, recordToRemove.GetDataForNotification(), nil, true,
		),
	)

	// push notifications if needed
	if processor.RecordSetNotificationPool.ShouldBeProcessed() {
		//capture updated state of all records in the pool
		processor.RecordSetNotificationPool.Push(user)
	}

	return removalRootNode.Data(), nil
}

//TODO: Refactor this method similarly to BulkUpdateRecords, so notifications could be tested properly, it should affect PrepareDeletes method
func (processor *Processor) BulkDeleteRecords(objectName string, next func() (map[string]interface{}, error), user auth.User) (err error) {
	// get MetaDescription
	objectMeta, err := processor.GetMeta(objectName)
	if err != nil {
		return err
	}

	// collect records` data to update
	records, err := processor.consumeRecords(next, objectMeta, true)
	if err != nil {
		return err
	}
	for _, record := range records {

		//get pk
		recordToRemove, err := processor.Get(objectName, record.PkAsString(), nil, nil, 1, false)
		if err != nil {
			return err
		}
		if recordToRemove == nil {
			return &errors.DataError{"RecordNotFound", "Record not found", objectName}
		}

		//fill node
		dbTransaction, err := processor.transactionManager.BeginTransaction()
		removalRootNode, err := new(RecordRemovalTreeBuilder).Extract(recordToRemove, processor, dbTransaction)
		if err != nil {
			processor.transactionManager.RollbackTransaction(dbTransaction)
			return err
		}

		err = processor.dataManager.PerformRemove(removalRootNode, dbTransaction, processor)
		if err != nil {
			processor.transactionManager.RollbackTransaction(dbTransaction)
			return err
		}

		processor.transactionManager.CommitTransaction(dbTransaction)
	}

	// push notifications if needed
	if processor.RecordSetNotificationPool.ShouldBeProcessed() {
		//capture updated state of all records in the pool
		processor.RecordSetNotificationPool.Push(user)
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
func (processor *Processor) updateRecordSet(dbTransaction transactions.DbTransaction, recordSet *RecordSet, isRoot bool) (*RecordSet, error) {
	notiToSend := make([]*notifications.RecordNotification, len(recordSet.Records))
	for i, record := range recordSet.Records {
		notiToSend[i] = notifications.NewRecordNotification(
			recordSet.Meta.Name, notifications.MethodUpdate, record.GetDataForNotification(), nil, isRoot,
		)
	}

	recordSet.PrepareData(RecordOperationTypeUpdate)
	// create notification, capture current recordData state and Add notification to notification pool

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

	for i, record := range recordSet.Records {
		notiToSend[i].CurrentState = record.GetDataForNotification()
		processor.RecordSetNotificationPool.Add(notiToSend[i])
	}

	return recordSet, nil
}

// perform create and return list of records
func (processor *Processor) createRecordSet(dbTransaction transactions.DbTransaction, recordSet *RecordSet, isRoot bool) (*RecordSet, error) {

	recordSet.PrepareData(RecordOperationTypeCreate)

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

	for _, record := range recordSet.Records {
		processor.RecordSetNotificationPool.Add(
			notifications.NewRecordNotification(
				recordSet.Meta.Name, notifications.MethodCreate, nil, record.GetDataForNotification(), isRoot,
			),
		)
	}

	return recordSet, nil
}
