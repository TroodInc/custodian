package notifications

import (
	"server/meta"
	"server/data/record"
	"strconv"
	"utils"
	"server/auth"
	"strings"
	"fmt"
)

type RecordSetNotification struct {
	recordSet     *record.RecordSet
	isRoot        bool
	method        meta.Method
	PreviousState []*record.RecordSet
	CurrentState  []*record.RecordSet
	getRecordsCallback func(objectName, filter string, depth int, sink func(map[string]interface{}) error, handleTransaction bool) error
	getRecordCallback func(objectClass, key string, depth int, handleTransaction bool) (map[string]interface{}, error)
}

func NewRecordSetNotification(recordSet *record.RecordSet, isRoot bool, method meta.Method, getRecordsCallback func(objectName, filter string, depth int, sink func(map[string]interface{}) error, handleTransaction bool) error, getRecordCallback func(objectClass, key string, depth int, handleTransaction bool) (map[string]interface{}, error)) *RecordSetNotification {
	return &RecordSetNotification{
		recordSet:          recordSet,
		isRoot:             isRoot,
		method:             method,
		PreviousState:      make([]*record.RecordSet, len(recordSet.Meta.Actions.Original)), //for both arrays index is an corresponding
		CurrentState:       make([]*record.RecordSet, len(recordSet.Meta.Actions.Original)), //action's index, states are action-specific due to actions's own fields configuration(IncludeValues)
		getRecordsCallback: getRecordsCallback,
		getRecordCallback:  getRecordCallback,
	}
}

func (notification *RecordSetNotification) CapturePreviousState() {
	notification.captureState(notification.PreviousState)
}

func (notification *RecordSetNotification) CaptureCurrentState() {
	notification.captureState(notification.CurrentState)
}

func (notification *RecordSetNotification) ShouldBeProcessed() bool {
	return len(notification.recordSet.Meta.Actions.Notifiers[notification.method]) > 0
}

//Build notification object for each record in recordSet for given action
func (notification *RecordSetNotification) BuildNotificationsData(actionIndex int, user auth.User) []map[string]interface{} {
	notifications := make([]map[string]interface{}, 0)
	for i := range notification.PreviousState[actionIndex].DataSet {
		notificationData := make(map[string]interface{})
		notificationData["action"] = notification.method.AsString()
		notificationData["object"] = notification.recordSet.Meta.Name
		notificationData["previous"] = notification.PreviousState[actionIndex].DataSet[i]
		notificationData["current"] = notification.CurrentState[actionIndex].DataSet[i]
		notificationData["user"] = user
		notifications = append(notifications, notificationData)
	}
	return notifications
}

func (notification *RecordSetNotification) captureState(state []*record.RecordSet) {
	//capture state if recordSet has PKs defined, set empty map otherwise, because records cannot be retrieved
	for i, action := range notification.recordSet.Meta.Actions.Original {
		state[i] = &record.RecordSet{Meta: notification.recordSet.Meta, DataSet: make([]map[string]interface{}, 0)}

		if recordsFilter := notification.getRecordsFilter(); recordsFilter != "" {
			recordsSink := func(recordData map[string]interface{}) error {
				state[i].DataSet = append(state[i].DataSet, notification.buildRecordStateObject(recordData, &action, notification.getRecordsCallback))
				return nil
			}
			//get data within current transaction
			notification.getRecordsCallback(notification.recordSet.Meta.Name, recordsFilter, 1, recordsSink, false)
		}
		//fill DataSet with empty values
		if len(state[i].DataSet) == 0 {
			state[i].DataSet = make([]map[string]interface{}, len(notification.recordSet.DataSet))
		}
	}
}

func (notification *RecordSetNotification) getRecordsFilter() string {
	hasKeys := false
	filter := "in(" + notification.recordSet.Meta.Key.Name + ",("
	for i, recordData := range notification.recordSet.DataSet {
		if rawKeyValue, ok := recordData[notification.recordSet.Meta.Key.Name]; ok {
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
func (notification *RecordSetNotification) buildRecordStateObject(recordData map[string]interface{}, action *meta.Action, getRecordsCallback func(objectName, filter string, depth int, sink func(map[string]interface{}) error, handleTransaction bool) error) map[string]interface{} {

	stateObject := make(map[string]interface{}, 0)
	//	include values which are updated being updated/created
	keys, _ := utils.GetMapKeysValues(notification.recordSet.DataSet[0])
	for _, key := range keys {
		if value, ok := recordData[key]; ok {
			stateObject[key] = value
		}
	}

	//include values listed in IncludeValues
	for alias, getterConfig := range action.IncludeValues {
		stateObject[alias] = getValue(record.Record{Data: recordData, Meta: notification.recordSet.Meta}, getterConfig, notification.getRecordCallback)
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

func getValue(targetRecord record.Record, getterConfig interface{}, getRecordCallback func(objectClass, key string, depth int, handleTransaction bool) (map[string]interface{}, error)) interface{} {
	switch getterValue := getterConfig.(type) {
	case map[string]interface{}:
		fmt.Println("sdf")
	case string:
		return getSimpleValue(targetRecord, strings.Split(getterValue, "."), getRecordCallback)
	}
	return ""
}

//get key value traversing down if needed
func getSimpleValue(targetRecord record.Record, keyParts []string, getRecordCallback func(objectClass, key string, depth int, handleTransaction bool) (map[string]interface{}, error)) interface{} {
	if len(keyParts) == 1 {
		return targetRecord.Data[keyParts[0]]
	} else {
		keyPart := keyParts[0]
		nestedObjectMeta := targetRecord.Meta.FindField(keyPart).LinkMeta
		if targetRecord.Data[keyPart] != nil {
			keyValue, _ := nestedObjectMeta.Key.ValueAsString(targetRecord.Data[keyPart])
			nestedRecordData, _ := getRecordCallback(targetRecord.Meta.Name, keyValue, 1, false)
			return getSimpleValue(record.Record{Data: nestedRecordData, Meta: nestedObjectMeta}, keyParts[1:], getRecordCallback)
		} else {
			return nil
		}
	}
}
