package notifications

import (
	"server/meta"
	"server/data/record"
	"strconv"
	"utils"
	"server/auth"
)

type RecordSetNotification struct {
	recordSet     *record.RecordSet
	isRoot        bool
	method        meta.Method
	PreviousState []*record.RecordSet
	CurrentState  []*record.RecordSet
	getRecords func(objectName, filter string, depth int, sink func(map[string]interface{}) error, handleTransaction bool) error
}

func NewRecordSetNotification(recordSet *record.RecordSet, isRoot bool, method meta.Method, getRecords func(objectName, filter string, depth int, sink func(map[string]interface{}) error, handleTransaction bool) error) *RecordSetNotification {
	return &RecordSetNotification{
		recordSet:     recordSet,
		isRoot:        isRoot,
		method:        method,
		PreviousState: make([]*record.RecordSet, len(recordSet.Meta.Actions.Original)), //for both arrays index is an corresponding
		CurrentState:  make([]*record.RecordSet, len(recordSet.Meta.Actions.Original)), //action's index, states are action-specific due to actions's own fields configuration(IncludeValues)
		getRecords:    getRecords,
	}
}

func (notification *RecordSetNotification) CapturePreviousState() {
	notification.captureState(notification.PreviousState)
}

func (notification *RecordSetNotification) CaptureCurrentState() {
	notification.captureState(notification.CurrentState)
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

func (notification *RecordSetNotification) captureState(state []*record.RecordSet) {
	//capture state if recordSet has PKs defined, set empty map otherwise, because records cannot be retrieved
	for i, action := range notification.recordSet.Meta.Actions.Original {
		if stateRecordSet := state[i]; stateRecordSet == nil {
			state[i] = &record.RecordSet{Meta: notification.recordSet.Meta, DataSet: make([]map[string]interface{}, 0)}
		}
		if recordsFilter := notification.getRecordsFilter(); recordsFilter != "" {
			recordsSink := func(recordData map[string]interface{}) error {
				state[i].DataSet = append(state[i].DataSet, notification.buildRecordStateObject(recordData, &action))
				return nil
			}
			//get data within current transaction
			notification.getRecords(notification.recordSet.Meta.Name, recordsFilter, 1, recordsSink, false)
		} else {
			//fill with array of empty maps
			state[i].DataSet = make([]map[string]interface{}, len(notification.recordSet.DataSet))
		}
	}
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

//Build object to use in notification
func (notification *RecordSetNotification) buildRecordStateObject(recordData map[string]interface{}, action *meta.Action) map[string]interface{} {
	stateObject := make(map[string]interface{}, 0)
	//	include values which are updated being updated/created
	keys, _ := utils.GetMapKeysValues(notification.recordSet.DataSet[0])
	for _, key := range keys {
		if value, ok := recordData[key]; ok {
			stateObject[key] = value
		}
	}

	//include values listed in IncludeValues
	for fieldPath, alias := range action.IncludeValues {
		if value, ok := recordData[fieldPath]; ok {
			stateObject[alias] = value
		}
		//remove key if alias is not equal to actual fieldPath and stateObject already
		// contains value under the fieldPath key, that is fieldPath key should be replaced with alias
		if _, ok := stateObject[fieldPath]; ok && fieldPath != alias {
			delete(stateObject, fieldPath)
		}
	}
	return stateObject
}
