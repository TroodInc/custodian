package notifications

import (
	"server/data/record"
	"strconv"
	"utils"
	"server/auth"
	"server/data/types"
	"server/object/description"
	"server/transactions"
)

type RecordSetNotification struct {
	recordSet *record.RecordSet
	isRoot    bool
	getRecordsCallback func(transaction transactions.DbTransaction, objectName, filter string, depth int, omitOuters bool, sink func(map[string]interface{}) error) (int, error)
	getRecordCallback func(transaction transactions.DbTransaction, objectClass, key string, depth int, omitOuters bool) (*record.Record, error)
	dbTransaction     transactions.DbTransaction
	Actions           []*description.Action
	Method            description.Method
	PreviousState     map[int]*record.RecordSet
	CurrentState      map[int]*record.RecordSet
}

func NewRecordSetNotification(dbTransaction transactions.DbTransaction, recordSet *record.RecordSet, isRoot bool, method description.Method, getRecordsCallback func(transaction transactions.DbTransaction, objectName, filter string, depth int, omitOuters bool, sink func(map[string]interface{}) error) (int, error), getRecordCallback func(transaction transactions.DbTransaction, objectClass, key string, depth int, omitOuters bool) (*record.Record, error)) *RecordSetNotification {
	actions := recordSet.Meta.ActionSet.FilterByMethod(method)
	return &RecordSetNotification{
		recordSet:          recordSet,
		isRoot:             isRoot,
		Method:             method,
		Actions:            actions,
		PreviousState:      make(map[int]*record.RecordSet, len(actions)), //for both arrays index is an corresponding
		CurrentState:       make(map[int]*record.RecordSet, len(actions)), //action's index, states are action-specific due to actions's own fields configuration(IncludeValues)
		getRecordsCallback: getRecordsCallback,
		getRecordCallback:  getRecordCallback,
		dbTransaction:      dbTransaction,
	}
}

func (notification *RecordSetNotification) CapturePreviousState() {
	notification.captureState(notification.PreviousState)
}

func (notification *RecordSetNotification) CaptureCurrentState() {
	notification.captureState(notification.CurrentState)
}

func (notification *RecordSetNotification) ShouldBeProcessed() bool {
	return len(notification.recordSet.Meta.ActionSet.Notifiers[notification.Method]) > 0
}

//Build notification object for each record in recordSet for given action
func (notification *RecordSetNotification) BuildNotificationsData(previousState *record.RecordSet, currentState *record.RecordSet, user auth.User) []map[string]interface{} {
	notifications := make([]map[string]interface{}, 0)
	for i := range previousState.Records {
		var previousStateData map[string]interface{}
		var currentStateData map[string]interface{}
		if previousState.Records[i] != nil {
			previousStateData = previousState.Records[i].Data
		} else {
			previousStateData = map[string]interface{}{}
		}
		if currentState.Records[i] != nil {
			currentStateData = currentState.Records[i].Data
		} else {
			currentStateData = map[string]interface{}{}
		}
		notificationData := make(map[string]interface{})
		notificationData["action"] = notification.Method.AsString()
		notificationData["object"] = notification.recordSet.Meta.Name
		notificationData["previous"] = adaptRecordData(previousStateData)
		notificationData["current"] = adaptRecordData(currentStateData)
		notificationData["user"] = user
		notifications = append(notifications, notificationData)
	}
	return notifications
}

func (notification *RecordSetNotification) captureState(state map[int]*record.RecordSet) {
	//capture state if recordSet has PKs defined, set empty map otherwise, because records cannot be retrieved
	for _, action := range notification.Actions {

		state[action.Id()] = &record.RecordSet{Meta: notification.recordSet.Meta, Records: make([]*record.Record, 0)}

		if recordsFilter := notification.getRecordsFilter(); recordsFilter != "" {
			recordsSink := func(recordData map[string]interface{}) error {

				state[action.Id()].Records = append(
					state[action.Id()].Records,
					record.NewRecord(state[action.Id()].Meta, notification.buildRecordStateObject(recordData, action, notification.getRecordsCallback)),
				)
				return nil
			}
			//get data within current transaction
			notification.getRecordsCallback(notification.dbTransaction, notification.recordSet.Meta.Name, recordsFilter, 1, true, recordsSink)
		}
		//fill DataSet with empty values
		if len(state[action.Id()].Records) == 0 {
			state[action.Id()].Records = make([]*record.Record, len(notification.recordSet.Records))
		}
	}
}

func (notification *RecordSetNotification) getRecordsFilter() string {
	hasKeys := false
	filter := "in(" + notification.recordSet.Meta.Key.Name + ",("
	for i, record := range notification.recordSet.Records {
		if rawKeyValue, ok := record.Data[notification.recordSet.Meta.Key.Name]; ok {
			hasKeys = true
			var keyValue string
			switch value := rawKeyValue.(type) {
			case string:
				keyValue = value
			case int:
				keyValue = strconv.Itoa(value)
			case float64:
				keyValue = strconv.Itoa(int(value))
			}
			if i != 0 {
				filter += ","
			}
			filter += keyValue
		}
	}
	filter += "))"
	if hasKeys {
		return filter
	} else {
		return ""
	}
}

//Build object to use in notification
func (notification *RecordSetNotification) buildRecordStateObject(recordData map[string]interface{}, action *description.Action, getRecordsCallback func(transaction transactions.DbTransaction, objectName, filter string, depth int, omitOuters bool, sink func(map[string]interface{}) error) (int, error)) map[string]interface{} {

	stateObject := make(map[string]interface{}, 0)

	//	include values which are being updated/created
	if action.Method != description.MethodRemove {
		keys, _ := utils.GetMapKeysValues(notification.recordSet.Records[0].Data)
		for _, key := range keys {
			if value, ok := recordData[key]; ok {
				stateObject[key] = value
			}
		}
	} else {
		//copy entire record data if it is being removed
		stateObject = utils.CloneMap(recordData)
	}
	currentRecord := record.NewRecord(notification.recordSet.Meta, recordData)
	//include values listed in IncludeValues
	for alias, getterConfig := range action.IncludeValues {
		stateObject[alias] = currentRecord.GetValue(getterConfig, notification.dbTransaction, notification.getRecordCallback)
		//remove key if alias is not equal to actual getterConfig and stateObject already
		// contains value under the getterConfig key, that is getterConfig key should be replaced with alias

		//remove duplicated values
		if getterString, ok := getterConfig.(string); ok {
			if _, ok := stateObject[getterString]; ok && getterConfig != alias {
				delete(stateObject, getterString)
			}
		}
	}
	return stateObject
}

func adaptRecordData(recordData map[string]interface{}) map[string]interface{} {
	adaptedRecordData := map[string]interface{}{}
	for key, value := range recordData {
		switch castValue := value.(type) {
		case types.DLink:
			adaptedRecordData[key] = castValue.Id
		default:
			adaptedRecordData[key] = castValue
		}
	}
	return adaptedRecordData
}
