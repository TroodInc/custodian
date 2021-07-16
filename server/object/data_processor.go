package object

import (
	"bytes"
	"custodian/logger"
	"custodian/server/auth"
	errors2 "custodian/server/errors"
	"custodian/server/object/description"
	"custodian/server/object/dml_info"
	"custodian/server/object/errors"
	"custodian/server/transactions"
	"custodian/utils"
	"strconv"

	"fmt"

	rqlParser "github.com/Q-CIS-DEV/go-rql-parser"
)

type objectClassValidator func(*Record) ([]*Record, error)

type ExecuteContext interface {
	Execute(operations []transactions.Operation) error
	Complete() error
	Close() error
}

type DataManager interface {
	GetRql(dataNode *Node, rqlNode *rqlParser.RqlRootNode, fields []*FieldDescription, dbTransaction transactions.DbTransaction) ([]map[string]interface{}, int, error)
	Get(m *Meta, fields []*FieldDescription, key string, val interface{}, dbTransaction transactions.DbTransaction) (map[string]interface{}, error)
	GetAll(m *Meta, fileds []*FieldDescription, filters map[string]interface{}, dbTransaction transactions.DbTransaction) ([]map[string]interface{}, error)
	PerformRemove(recordNode *RecordRemovalNode, dbTransaction transactions.DbTransaction, notificationPool *RecordSetNotificationPool, processor *Processor) error
	PrepareCreateOperation(m *Meta, objs []map[string]interface{}) (transactions.Operation, error)
	PrepareUpdateOperation(m *Meta, objs []map[string]interface{}) (transactions.Operation, error)
}

type Processor struct {
	metaStore          *MetaStore
	transactionManager transactions.DbTransactionManager
	vCache             map[string]objectClassValidator
}

func NewProcessor(m *MetaStore, t transactions.DbTransactionManager) (*Processor, error) {
	return &Processor{m, t, make(map[string]objectClassValidator)}, nil
}

type SearchContext struct {
	DepthLimit    int
    processor     *Processor
	LazyPath      string
	OmitOuters    bool
	DbTransaction transactions.DbTransaction
}

func IsBackLink(m *Meta, f *FieldDescription) bool {
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
			transaction, e := processor.transactionManager.BeginTransaction()
			if e != nil {
				return nil, e
			}

			ctx := SearchContext{DepthLimit: depth, processor: processor, LazyPath: "/custodian/data", DbTransaction: transaction, OmitOuters: omitOuters}

			//
			root := &Node{
				KeyField:       objectMeta.Key,
				Meta:           objectMeta,
				ChildNodes:     *NewChildNodes(),
				Depth:          1,
				OnlyLink:       false,
				Plural:         false,
				Parent:         nil,
				Type:           NodeTypeRegular,
				SelectFields:   *NewSelectFields(objectMeta.Key, objectMeta.TableFields()),
				RetrievePolicy: new(AggregatedRetrievePolicyFactory).Factory(includePaths, excludePaths),
			}

			err := root.RecursivelyFillChildNodes(ctx.DepthLimit, description.FieldModeRetrieve)
			if err != nil {
				
				transaction.Rollback()
				return nil, err
			}

			if recordData, e := root.Resolve(ctx, pk); e != nil {
				
				transaction.Rollback()
				return nil, e
			} else if recordData == nil {
				
				transaction.Rollback()
				return nil, nil
			} else {
				
				transaction.Commit()
				return recordData, nil
			}
		}
	}
}

func (processor *Processor) GetBulk(objectName string, filter string, includePaths []string, excludePaths []string, depth int, omitOuters bool) (int, []*Record, error) {
	if businessObject, ok, e := processor.metaStore.Get(objectName, true); e != nil {
		return 0, nil, e
	} else if !ok {
		return 0, nil, errors2.NewNotFoundError(
			errors.ErrObjectClassNotFound, fmt.Sprintf("Object class '%s' not found", objectName), nil,
		)
	} else {
		transaction, e := processor.transactionManager.BeginTransaction()
		if e != nil {
			return 0, nil, e
		}
		searchContext := SearchContext{DepthLimit: depth, processor: processor, LazyPath: "/custodian/data/bulk", DbTransaction: transaction, OmitOuters: omitOuters}

		//make and apply retrieves policy
		retrievePolicy := new(AggregatedRetrievePolicyFactory).Factory(includePaths, excludePaths)
		//
		root := &Node{
			KeyField:       businessObject.Key,
			Meta:           businessObject,
			ChildNodes:     *NewChildNodes(),
			Depth:          1,
			OnlyLink:       false,
			Plural:         false,
			Parent:         nil,
			Type:           NodeTypeRegular,
			SelectFields:   *NewSelectFields(businessObject.Key, businessObject.TableFields()),
			RetrievePolicy: retrievePolicy,
		}
		// TODO handle error
		// if err is handled test [It] Can exclude fileld of m2m object does not pass
		root.RecursivelyFillChildNodes(searchContext.DepthLimit, description.FieldModeRetrieve)
		// err := root.RecursivelyFillChildNodes(searchContext.DepthLimit, description.FieldModeRetrieve)
		// if err != nil {
		// 	processor.transactionManager.RollbackTransaction(transaction)
		// 	return 0, nil, errors2.NewValidationError(errors.ErrWrongRQL, err.Error(), nil)
		// }

		parser := rqlParser.NewParser()

		rqlNode, err := parser.Parse(filter)
		if err != nil {
			
			transaction.Rollback()
			return 0, nil, errors2.NewValidationError(errors.ErrWrongRQL, err.Error(), nil)
		}

		records, recordsCount, e := root.ResolveByRql(searchContext, rqlNode)

		if e != nil {
			
			transaction.Rollback()
			return recordsCount, nil, e
		}
		
		transaction.Commit()
		return recordsCount, records, nil
	}
}

type DNode struct {
	KeyField   *FieldDescription
	Meta       *Meta
	ChildNodes map[string]*DNode
	Plural     bool
}

func (dn *DNode) fillOuterChildNodes() {
	for _, f := range dn.Meta.Fields {
		if f.LinkType == description.LinkTypeOuter {
			dn.ChildNodes[f.Name] = &DNode{KeyField: f.OuterLinkField,
				Meta:       f.LinkMeta,
				ChildNodes: make(map[string]*DNode),
				Plural:     true}
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

func (processor *Processor) GetMeta(objectName string) (*Meta, error) {
	objectMeta, ok, err := processor.metaStore.Get(objectName, true)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors2.NewNotFoundError(
			errors.ErrObjectClassNotFound, fmt.Sprintf("Object class '%s' not found", objectName), nil,
		)
	}
	return objectMeta, nil
}

func SetRecordOwner(objectMeta *Meta, recordData map[string]interface{}, user auth.User) {
	for i, field := range objectMeta.Fields {
		if field.Def != nil {
			switch f := field.Def.(type) {
			case map[string]interface{}:
				owner, ownerInData := recordData[objectMeta.Fields[i].Name]
				if ownerInData {
					recordData[objectMeta.Fields[i].Name] = owner
				} else if !ownerInData && f["func"] == "owner" {
					if user.Id == 0 {
						recordData[objectMeta.Fields[i].Name] = nil
					} else {
						recordData[objectMeta.Fields[i].Name] = user.Id
					}

				}
			}
		}
	}
}

// CreateRecord create object record in database
func (processor *Processor) CreateRecord(objectName string, recordData map[string]interface{}, user auth.User) (*Record, error) {
	// get MetaDescription
	objectMeta, err := processor.GetMeta(objectName)
	if err != nil {
		return nil, err
	}

	// extract processing node
	recordProcessingNode, err := new(RecordProcessingTreeBuilder).Build(&Record{Meta: objectMeta, Data: recordData, processor: processor}, processor)
	if err != nil {
		return nil, err
	}

	// create notification pool
	recordSetNotificationPool := NewRecordSetNotificationPool()
	defer func() { recordSetNotificationPool.CompleteSend(err) }()

	//perform update
	rootRecordSet, recordSetsOperations := recordProcessingNode.RecordSetOperations(user)

	// create records

	for _, recordSetOperation := range recordSetsOperations {
		isRoot := recordSetOperation.RecordSet == rootRecordSet
		if recordSetOperation.Type == RecordOperationTypeCreate || isRoot {
			if _, err := processor.createRecordSet(
				recordSetOperation.RecordSet,
				isRoot,
				recordSetNotificationPool,
			); err != nil {
				return nil, err
			}
		} else if recordSetOperation.Type == RecordOperationTypeUpdate {
			if _, err := processor.updateRecordSet(
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
					record.Meta.Name,
					recordPkAsStr,
					user,
				); err != nil {
					return nil, err
				}
			}
		} else if recordSetOperation.Type == RecordOperationTypeRetrieve {
			for i, record := range recordSetOperation.RecordSet.Records {
				retrievedRecord, err := processor.Get(record.Meta.Name, record.PkAsString(), nil, nil, 1, true)
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
		recordSetNotificationPool.Push(user)
	}
	return NewRecord(objectMeta, recordData, processor), nil
}

func (processor *Processor) BulkCreateRecords(objectName string, recordData []map[string]interface{}, user auth.User) ([]*Record, error) {

	// get MetaDescription
	objectMeta, err := processor.GetMeta(objectName)
	if err != nil {
		return nil, err
	}

	// create notification pool
	recordSetNotificationPool := NewRecordSetNotificationPool()
	defer func() { recordSetNotificationPool.CompleteSend(err) }()

	//assemble RecordSetOperations
	var recordProcessingNode *RecordProcessingNode
	rootRecordSets := make([]interface{}, 0)
	for _, record := range recordData {
		SetRecordOwner(objectMeta, record, user)
		// extract processing node
		recordProcessingNode, err = new(RecordProcessingTreeBuilder).Build(
			&Record{Meta: objectMeta, Data: record, processor: processor}, processor,
		)
		if err != nil {
			return nil, err
		}
		rootRecordSet, recordSetOperations := recordProcessingNode.RecordSetOperations(user)

		for _, recordSetOperation := range recordSetOperations {
			isRoot := recordSetOperation.RecordSet == rootRecordSet
			// TODO: investigate this shit
			if recordSetOperation.Type == RecordOperationTypeCreate || isRoot {
				if _, err := processor.createRecordSet(
					recordSetOperation.RecordSet,
					isRoot,
					recordSetNotificationPool,
				); err != nil {
					return nil, err
				}
			} else if recordSetOperation.Type == RecordOperationTypeUpdate {
				if _, err := processor.updateRecordSet(
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
						record.Meta.Name,
						recordPkAsStr,
						user,
					); err != nil {
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
		return nil, err
	}

	// push notifications if needed
	if recordSetNotificationPool.ShouldBeProcessed() {
		recordSetNotificationPool.Push(user)
	}

	return result, nil
}

func (processor *Processor) UpdateRecord(objectName, key string, recordData map[string]interface{}, user auth.User) (updatedRecord *Record, err error) {
	// get MetaDescription
	objectMeta, err := processor.GetMeta(objectName)
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
	recordProcessingNode, err := new(RecordProcessingTreeBuilder).Build(&Record{Meta: objectMeta, Data: recordData, processor: processor}, processor)
	if err != nil {
		return nil, err
	}

	// create notification pool
	recordSetNotificationPool := NewRecordSetNotificationPool()
	defer func() { recordSetNotificationPool.CompleteSend(err) }()

	//perform update
	rootRecordSet, recordSetOperations := recordProcessingNode.RecordSetOperations(user)

	for _, recordSetOperation := range recordSetOperations {
		isRoot := recordSetOperation.RecordSet == rootRecordSet
		if recordSetOperation.Type == RecordOperationTypeUpdate {
			if _, err := processor.updateRecordSet(
				recordSetOperation.RecordSet,
				isRoot,
				recordSetNotificationPool,
			); err != nil {
				return nil, err
			}
		} else if recordSetOperation.Type == RecordOperationTypeCreate {
			if _, err := processor.createRecordSet(
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
					record.Meta.Name,
					recordPkAsStr,
					user,
				); err != nil {
					return nil, err
				}
			}

		} else if recordSetOperation.Type == RecordOperationTypeRetrieve {
			for i, record := range recordSetOperation.RecordSet.Records {
				retrievedRecord, err := processor.Get(record.Meta.Name, record.PkAsString(), nil, nil, 1, true)
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
		recordSetNotificationPool.Push(user)
	}

	return rootRecordSet.Records[0], nil
}

func (processor *Processor) BulkUpdateRecords(objectName string, next func() (map[string]interface{}, error), sink func(map[string]interface{}) error, user auth.User) (err error) {

	//get MetaDescription
	objectMeta, err := processor.GetMeta(objectName)
	if err != nil {
		return err
	}

	// create notification pool
	recordSetNotificationPool := NewRecordSetNotificationPool()
	defer func() { recordSetNotificationPool.CompleteSend(err) }()

	// collect records` data to update
	records, err := processor.consumeRecords(next, objectMeta, true)
	if err != nil {
		return err
	}

	//assemble RecordSetOperations
	var recordProcessingNode *RecordProcessingNode
	rootRecordSets := make([]*RecordSet, 0)
	for _, record := range records {
		// extract processing node
		recordProcessingNode, err = new(RecordProcessingTreeBuilder).Build(record, processor)
		if err != nil {
			return err
		}
		//perform update
		rootRecordSet, recordSetOperations := recordProcessingNode.RecordSetOperations(user)

		for _, recordSetOperation := range recordSetOperations {
			isRoot := recordSetOperation.RecordSet == rootRecordSet
			if recordSetOperation.Type == RecordOperationTypeUpdate {
				if _, err := processor.updateRecordSet(
					recordSetOperation.RecordSet,
					isRoot,
					recordSetNotificationPool,
				); err != nil {
					return err
				}
			} else if recordSetOperation.Type == RecordOperationTypeCreate {
				if _, err := processor.createRecordSet(
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
		recordSetNotificationPool.Push(user)
	}

	return nil

}

//TODO: Refactor this method similarly to UpdateRecord, so notifications could be tested properly, it should affect PrepareDeletes method
func (processor *Processor) RemoveRecord(objectName string, key string, user auth.User) (*Record, error) {
	var err error

	//get pk
	recordToRemove, err := processor.Get(objectName, key, nil, nil, 1, false)
	if err != nil {
		return nil, err
	}
	if recordToRemove == nil {
		return nil, errors2.NewNotFoundError("RecordNotFound", "Record not found", nil)
	}

	// create notification pool
	recordSetNotificationPool := NewRecordSetNotificationPool()
	defer func() { recordSetNotificationPool.CompleteSend(err) }()

	//fill node
	dbTransaction, err := processor.transactionManager.BeginTransaction()
	if err != nil {
		return nil, err
	}
	removalRootNode, err := new(RecordRemovalTreeBuilder).Extract(recordToRemove, processor, dbTransaction)
	if err != nil {
		
		dbTransaction.Rollback()
		return nil, err
	}

	err = processor.PerformRemove(removalRootNode, dbTransaction, recordSetNotificationPool)
	if err != nil {
		
		dbTransaction.Rollback()
		return nil, err
	}

	
	dbTransaction.Commit()
	// push notifications if needed
	if recordSetNotificationPool.ShouldBeProcessed() {
		//capture updated state of all records in the pool
		recordSetNotificationPool.Push(user)
	}
	return removalRootNode.Record, nil
}

//TODO: Refactor this method similarly to BulkUpdateRecords, so notifications could be tested properly, it should affect PrepareDeletes method
func (processor *Processor) BulkDeleteRecords(objectName string, next func() (map[string]interface{}, error), user auth.User) (err error) {
	// get MetaDescription
	objectMeta, err := processor.GetMeta(objectName)
	if err != nil {
		return err
	}
	//

	// create notification pool
	recordSetNotificationPool := NewRecordSetNotificationPool()
	defer func() { recordSetNotificationPool.CompleteSend(err) }()

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
			return errors2.NewNotFoundError("RecordNotFound", "Record not found", nil)
		}

		//fill node
		dbTransaction, err := processor.transactionManager.BeginTransaction()
		if err != nil {
			return err
		}
		removalRootNode, err := new(RecordRemovalTreeBuilder).Extract(recordToRemove, processor, dbTransaction)
		if err != nil {
			
			dbTransaction.Rollback()
			return err
		}

		err = processor.PerformRemove(removalRootNode, dbTransaction, recordSetNotificationPool)
		if err != nil {
			
			dbTransaction.Rollback()
			return err
		}

		
		dbTransaction.Commit()
		// push notifications if needed
		if recordSetNotificationPool.ShouldBeProcessed() {
			//capture updated state of all records in the pool
			recordSetNotificationPool.Push(user)
		}

	}

	// push notifications if needed
	if recordSetNotificationPool.ShouldBeProcessed() {
		recordSetNotificationPool.Push(user)
	}

	return nil
}

//consume all records from callback function
func (processor *Processor) consumeRecords(nextCallback func() (map[string]interface{}, error), objectMeta *Meta, strictPkCheck bool) ([]*Record, error) {
	var records = make([]*Record, 0)
	// collect records
	for recordData, err := nextCallback(); err != nil || (recordData != nil); recordData, err = nextCallback() {
		if err != nil {
			return nil, err
		}
		if strictPkCheck {
			keyValue, ok := recordData[objectMeta.Key.Name]
			if !ok || !objectMeta.Key.Type.TypeAsserter()(keyValue) {
				return nil, errors2.NewValidationError(
					errors.ErrKeyValueNotFound, "Key value not found or has a wrong type", nil,
				)
			}
		}

		records = append(records, NewRecord(objectMeta, recordData, processor))
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
func (processor *Processor) updateRecordSet(recordSet *RecordSet, isRoot bool, recordSetNotificationPool *RecordSetNotificationPool) (*RecordSet, error) {
	recordSet.PrepareData(RecordOperationTypeUpdate)
	// create notification, capture current recordData state and Add notification to notification pool
	recordSetNotification := NewRecordSetNotification(recordSet, isRoot, description.MethodUpdate)
	if recordSetNotification.ShouldBeProcessed() {
		previous, _ := processor.Get(recordSet.Meta.Name, recordSet.Records[0].PkAsString(), nil, nil, 1, true)
		recordSetNotification.CapturePreviousState([]*Record{previous})
	}

	var operations = make([]transactions.Operation, 0)

	if operation, e := processor.PrepareUpdateOperation(recordSet.Meta, recordSet.RawData()); e != nil {
		return nil, e
	} else {
		operations = append(operations, operation)
	}
	//
	dbTransaction, err := processor.transactionManager.BeginTransaction()
	if err != nil {
		return nil, err
	}
	if e := dbTransaction.Execute(operations); e != nil {
		
		dbTransaction.Rollback()
		return nil, e
	}
	
	dbTransaction.Commit()

	recordSet.MergeData()

	if recordSetNotification.ShouldBeProcessed() {
		recordSetNotification.CaptureCurrentState(recordSet.Records)
		recordSetNotificationPool.Add(recordSetNotification)
	}

	return recordSet, nil
}

// perform create and return list of records
func (processor *Processor) createRecordSet(recordSet *RecordSet, isRoot bool, recordSetNotificationPool *RecordSetNotificationPool) (*RecordSet, error) {
	recordSet.PrepareData(RecordOperationTypeCreate)
	// create notification, capture current recordData state and Add notification to notification pool
	recordSetNotification := NewRecordSetNotification(recordSet, isRoot, description.MethodCreate)
	if recordSetNotification.ShouldBeProcessed() {
		recordSetNotification.CapturePreviousState(make([]*Record, len(recordSet.Records)))
	}

	var operations = make([]transactions.Operation, 0)

	if operation, e := processor.PrepareCreateOperation(recordSet.Meta, recordSet.RawData()); e != nil {
		return nil, e
	} else {
		operations = append(operations, operation)
	}
	//
	dbTransaction, err := processor.transactionManager.BeginTransaction()
	if err != nil {
		return nil, err
	}
	if e := dbTransaction.Execute(operations); e != nil {
		
		dbTransaction.Rollback()
		return nil, e
	}
	
	dbTransaction.Commit()
	recordSet.MergeData()

	// create notification, capture current recordData state and Add notification to notification pool
	if recordSetNotification.ShouldBeProcessed() {
		recordSetNotification.CaptureCurrentState(recordSet.Records)
		recordSetNotificationPool.Add(recordSetNotification)
	}

	return recordSet, nil
}


func (processor *Processor) PrepareUpdateOperation(m *Meta, recordValues []map[string]interface{}) (transactions.Operation, error) {
	if len(recordValues) == 0 {
		return emptyOperation, nil
	}

	rFields := m.TableFields()
	updateInfo := &dml_info.UpdateInfo{GetTableName(m.Name), dml_info.EscapeColumns(getFieldsColumnsNames(rFields)), make([]string, 0), make([]string, 0)}
	updateFields := make([]string, 0, len(recordValues[0]))
	valueExtractors := make([]func(interface{}) interface{}, 0, len(recordValues[0]))
	currentColumnIndex := 0
	var b bytes.Buffer
	newBind := func(col string, columnIndex int) string {
		defer b.Reset()
		b.WriteString(fmt.Sprintf("\"%s\"", col))
		b.WriteString("=$")
		b.WriteString(strconv.Itoa(columnIndex))
		return b.String()
	}
	for fieldName, val := range recordValues[0] {
		//primary key column
		if m.Key.Name == fieldName {
			currentColumnIndex++
			updateFields = append(updateFields, fieldName)
			updateInfo.Filters = append(updateInfo.Filters, newBind(fieldName, currentColumnIndex))
			valueExtractors = append(valueExtractors, identityVal)
			//cas column
		} else if fieldName == "cas" {

			currentColumnIndex++
			updateFields = append(updateFields, fieldName)
			updateInfo.Filters = append(updateInfo.Filters, newBind(fieldName, currentColumnIndex))
			valueExtractors = append(valueExtractors, identityVal)

			currentColumnIndex++
			updateFields = append(updateFields, fieldName)
			updateInfo.Values = append(updateInfo.Values, newBind(fieldName, currentColumnIndex))
			valueExtractors = append(valueExtractors, increaseCasVal)

		} else {
			switch val.(type) {
			case LazyLink:
				currentColumnIndex++
				updateFields = append(updateFields, fieldName)
				updateInfo.Filters = append(updateInfo.Filters, newBind(fieldName, currentColumnIndex))
				valueExtractors = append(valueExtractors, alinkVal)
			case DLink:
				currentColumnIndex++
				updateFields = append(updateFields, fieldName)
				updateInfo.Values = append(updateInfo.Values, newBind(fieldName, currentColumnIndex))
				valueExtractors = append(valueExtractors, dlinkVal)
			case *GenericInnerLink:

				currentColumnIndex++
				updateInfo.Values = append(updateInfo.Values, newBind(GetGenericFieldTypeColumnName(fieldName), currentColumnIndex))

				currentColumnIndex++
				updateInfo.Values = append(updateInfo.Values, newBind(GetGenericFieldKeyColumnName(fieldName), currentColumnIndex))

				updateFields = append(updateFields, fieldName)

				valueExtractors = append(valueExtractors, genericInnerLinkValue)
			default:
				currentColumnIndex++
				updateFields = append(updateFields, fieldName)
				updateInfo.Values = append(updateInfo.Values, newBind(fieldName, currentColumnIndex))
				valueExtractors = append(valueExtractors, identityVal)
			}
		}
	}

	if len(updateInfo.Values) == 0 {
		return emptyOperation, nil
	}

	if err := parsedTemplUpdate.Execute(&b, updateInfo); err != nil {
		logger.Error("Prepare update SQL by template error: %s", err.Error())
		return nil, errors2.NewFatalError(ErrTemplateFailed, err.Error(), nil)
	}

	return func(dbTransaction transactions.DbTransaction) error {
		stmt, err := dbTransaction.(*PgTransaction).Prepare(b.String())
		if err != nil {
			return err
		}
		defer stmt.Close()

		for i := range recordValues {
			binds := make([]interface{}, 0)
			for j := range updateFields {
				if v, ok := recordValues[i][updateFields[j]]; !ok {
					return errors2.NewValidationError(ErrInvalidArgument, "Different set of fields. Object #%d. All objects must have the same set of fields.", i)
				} else {
					value := valueExtractors[j](v)
					if value == nil {
						binds = append(binds, value)
					} else {
						switch castValue := value.(type) {
						case []string:
							for _, value := range castValue {
								binds = append(binds, value)
							}
						case []interface{}:
							//case for inner generic nil value: [nil,nil]
							for _, value := range castValue {
								switch value.(type) {
								case nil:
									binds = append(binds, value)
								default:
									binds = append(binds, fmt.Sprintf("%v", value))
								}

							}
						case interface{}:
							switch castValue.(type) {
							case nil:
								binds = append(binds, castValue)
							default:
								binds = append(binds, fmt.Sprintf("%v", castValue))
							}
						}
					}
				}
			}
			if uo, err := stmt.ParsedSingleQuery(binds, rFields); err == nil {
				updateNodes(recordValues[i], uo)
			} else {
				return err
			}
		}

		return nil
	}, nil
}

func (processor *Processor) PrepareCreateOperation(m *Meta, recordsValues []map[string]interface{}) (transactions.Operation, error) {
	if len(recordsValues) == 0 {
		return emptyOperation, nil
	}

	//fix the columns by the first object
	fields := m.TableFields()

	insertFields, insertValuesPattern := utils.GetMapKeysValues(recordsValues[0])
	insertColumns := getColumnsToInsert(insertFields, insertValuesPattern)

	insertInfo := dml_info.NewInsertInfo(GetTableName(m.Name), insertColumns, getFieldsColumnsNames(fields), len(recordsValues))
	var insertDML bytes.Buffer
	if err := parsedTemplInsert.Execute(&insertDML, insertInfo); err != nil {
		return nil, errors2.NewFatalError(ErrTemplateFailed, err.Error(), nil)
	}

	var fixSeqDML bytes.Buffer
	for _, field := range insertFields {
		if f := m.FindField(field); f != nil {
			def := f.Default()
			if d, ok := def.(description.DefExpr); ok && d.Func == "nextval" && f.Type == description.FieldTypeNumber {
				if err := parsedTemplFixSequense.Execute(&fixSeqDML, map[string]interface{}{
					"Table": insertInfo.Table,
					"Field": field,
				}); err != nil {
					return nil, errors2.NewFatalError(ErrTemplateFailed, err.Error(), nil)
				}
			}
		}
	}

	return func(dbTransaction transactions.DbTransaction) error {
		//prepare binds only on executing step otherwise the foregin key may be absent (tx sequence)
		binds := make([]interface{}, 0, len(insertColumns)*len(recordsValues))
		for _, recordValues := range recordsValues {
			if values, err := getValuesToInsert(insertFields, recordValues, insertColumns); err != nil {
				return err
			} else {
				binds = append(binds, values...)
			}
		}
		stmt, err := NewStmt(dbTransaction.Transaction(), insertDML.String())
		if err != nil {
			return err
		}
		defer stmt.Close()
		dbObjs, err := stmt.ParsedQuery(binds, fields)
		if err != nil {
			if err, ok := err.(*errors2.ServerError); ok && err.Code == ErrValueDuplication {
				//dupTransaction, _ := dbTransaction.(*PgTransaction).Manager.BeginTransaction()
				duplicates, dup_error := processor.GetAll(m, m.TableFields(), err.Data.(map[string]interface{}), dbTransaction)
				//dupTransaction.Rollback()
				if dup_error != nil {
					logger.Error(dup_error.Error())
				}
				err.Data = duplicates
			}
			return err
		}
		if _, err := dbTransaction.(*PgTransaction).Exec(fixSeqDML.String()); err != nil {
			return err
		}

		for i := 0; i < len(recordsValues); i++ {
			updateNodes(recordsValues[i], dbObjs[i])
		}
		return nil
	}, nil
}

func (processor *Processor) GetSystem(m *Meta, fields []*FieldDescription, key string, val interface{}, dbTransaction transactions.DbTransaction) (map[string]interface{}, error) {
	objs, err := processor.GetAll(m, fields, map[string]interface{}{key: val}, dbTransaction)
	if err != nil {
		return nil, err
	}

	l := len(objs)
	if l > 1 {
		return nil, errors2.NewFatalError(ErrTooManyFound, "too many rows found", nil)
	}

	if l == 0 {
		return nil, nil
	}

	return objs[0], nil
}

func (processor *Processor) GetAll(m *Meta, fields []*FieldDescription, filters map[string]interface{}, dbTransction transactions.DbTransaction) ([]map[string]interface{}, error) {
	tx := dbTransction.Transaction()
	if fields == nil {
		fields = m.TableFields()
	}
	filterKeys, filterValues := GetMapKeysStrValues(filters)

	selectInfo := NewSelectInfo(m, fields, filterKeys)
	var q bytes.Buffer
	if err := selectInfo.sql(&q); err != nil {
		return nil, errors2.NewFatalError(ErrTemplateFailed, err.Error(), nil)
	}

	stmt, err := NewStmt(tx, q.String())
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.ParsedQuery(filterValues, fields)
}

func (processor *Processor) PerformRemove(recordNode *RecordRemovalNode, dbTransaction transactions.DbTransaction, notificationPool *RecordSetNotificationPool) error {
	var operation transactions.Operation
	var err error
	var onDeleteStrategy description.OnDeleteStrategy
	if recordNode.OnDeleteStrategy != nil {
		onDeleteStrategy = *recordNode.OnDeleteStrategy
	} else {
		onDeleteStrategy = description.OnDeleteCascade
	}

	//make operation
	var recordSetNotification *RecordSetNotification
	switch onDeleteStrategy {
	case description.OnDeleteSetNull:
		//make corresponding null value
		var nullValue interface{}
		if recordNode.LinkField.Type == description.FieldTypeGeneric {
			nullValue = new(GenericInnerLink)
		} else {
			nullValue = nil
		}
		//update record with this value
		operation, err = processor.PrepareUpdateOperation(
			recordNode.Record.Meta,
			[]map[string]interface{}{{recordNode.Record.Meta.Key.Name: recordNode.Record.Pk(), recordNode.LinkField.Name: nullValue}},
		)
		if err != nil {
			return err
		}
		recordSetNotification = NewRecordSetNotification(&RecordSet{Meta: recordNode.Record.Meta, Records: []*Record{recordNode.Record}}, false, description.MethodUpdate)
	default:
		operation, err = processor.PrepareRemoveOperation(recordNode.Record)
		if err != nil {
			return err
		}
		recordSetNotification = NewRecordSetNotification(&RecordSet{Meta: recordNode.Record.Meta, Records: []*Record{recordNode.Record}}, false, description.MethodRemove)
	}

	if recordSetNotification.ShouldBeProcessed() {
		records := []*Record{recordNode.Record}
		recordSetNotification.CapturePreviousState(records)
		notificationPool.Add(recordSetNotification)
	}

	//process child records
	for _, recordNodes := range recordNode.Children {
		for _, recordNode := range recordNodes {
			err := processor.PerformRemove(recordNode, dbTransaction, notificationPool)
			if err != nil {
				return err
			}
		}
	}
	//create and process notification
	if err := dbTransaction.Execute([]transactions.Operation{operation}); err != nil {
		return err
	} else {
		recordSetNotification.CaptureCurrentState(make([]*Record, 0))
		return nil
	}
}

func (processor *Processor) PrepareRemoveOperation(record *Record) (transactions.Operation, error) {
	var query bytes.Buffer
	deleteInfo := dml_info.NewDeleteInfo(GetTableName(record.Meta.Name), []string{dml_info.EscapeColumn(record.Meta.Key.Name) + " IN (" + dml_info.BindValues(1, 1) + ")"})
	operation := func(dbTransaction transactions.DbTransaction) error {
		stmt, err := dbTransaction.(*PgTransaction).Prepare(query.String())
		if err != nil {
			return err
		}
		defer stmt.Close()
		if _, err = stmt.Exec(record.Pk()); err != nil {
			return errors2.NewFatalError(ErrDMLFailed, err.Error(), nil)
		}
		return nil
	}
	if err := parsedTemplDelete.Execute(&query, deleteInfo); err != nil {
		return nil, errors2.NewFatalError(ErrTemplateFailed, err.Error(), nil)
	}
	return operation, nil

}

func (processor *Processor) GetRql(dataNode *Node, rqlRoot *rqlParser.RqlRootNode, fields []*FieldDescription, dbTransaction transactions.DbTransaction) ([]map[string]interface{}, int, error) {
	tx := dbTransaction.Transaction()
	tableAlias := string(dataNode.Meta.Name[0])
	translator := NewSqlTranslator(rqlRoot)
	sqlQuery, err := translator.query(tableAlias, dataNode)
	if err != nil {
		return nil, 0, err
	}

	selectInfo := &SelectInfo{
		From:   GetTableName(dataNode.Meta.Name) + " " + tableAlias,
		Cols:   fieldsToCols(fields, tableAlias),
		Where:  sqlQuery.Where,
		Order:  sqlQuery.Sort,
		Limit:  sqlQuery.Limit,
		Offset: sqlQuery.Offset,
	}

	countInfo := &SelectInfo{
		From:  GetTableName(dataNode.Meta.Name) + " " + tableAlias,
		Cols:  []string{"count(*)"},
		Where: sqlQuery.Where,
	}

	//records data
	var queryString bytes.Buffer
	if err := selectInfo.sql(&queryString); err != nil {
		return nil, 0, errors2.NewFatalError(ErrTemplateFailed, err.Error(), nil)
	}
	statement, err := NewStmt(tx, queryString.String())
	if err != nil {
		return nil, 0, err
	}
	defer statement.Close()
	//count data
	count := 0
	queryString.Reset()
	if err := countInfo.sql(&queryString); err != nil {
		return nil, 0, errors2.NewFatalError(ErrTemplateFailed, err.Error(), nil)
	}
	countStatement, err := NewStmt(tx, queryString.String())
	if err != nil {
		return nil, 0, err
	}
	defer countStatement.Close()

	recordsData, err := statement.ParsedQuery(sqlQuery.Binds, fields)
	err = countStatement.Scalar(&count, sqlQuery.Binds)
	if err != nil {
		return nil, 0, err
	}
	return recordsData, count, err
}