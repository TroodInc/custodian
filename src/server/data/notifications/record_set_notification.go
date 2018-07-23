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
	recordsFilter string
	isRoot        bool
	method        meta.Method
	previousState []*record.RecordSet
	currentState  []*record.RecordSet
	getRecords func(objectName, filter string, depth int, sink func(map[string]interface{}) error) error
}

func NewRecordSetStateNotification(recordSet *record.RecordSet, isRoot bool, method meta.Method, getRecords func(objectName, filter string, depth int, sink func(map[string]interface{}) error) error) *RecordSetNotification {
	recordKeys := make([]string, len(recordSet.DataSet))
	filter := "in(" + recordSet.Meta.Key.Name + ",("
	for i, recordData := range recordSet.DataSet {
		rawKeyValue := recordData[recordSet.Meta.Key.Name]
		var keyValue string
		switch value := rawKeyValue.(type) {
		case string:
			keyValue = value
		case int:
			keyValue = strconv.Itoa(value)
		}
		recordKeys[i] = keyValue
		if i != 0 {
			filter += ","
		}
		filter += keyValue
	}
	filter += "))"
	return &RecordSetNotification{
		recordSet:     recordSet,
		recordsFilter: filter,
		isRoot:        isRoot,
		method:        method,
		previousState: make([]*record.RecordSet, len(recordSet.Meta.Actions.Original)),
		currentState:  make([]*record.RecordSet, len(recordSet.Meta.Actions.Original)),
		getRecords:    getRecords,
	}
}

func (notification *RecordSetNotification) CapturePreviousState() {
	notification.captureState(notification.previousState)
}

func (notification *RecordSetNotification) CaptureCurrentState() {
	notification.captureState(notification.currentState)
}

func (notification *RecordSetNotification) captureState(state []*record.RecordSet) {
	//todo: use action
	for i, action := range notification.recordSet.Meta.Actions.Original {
		if notification.method == meta.MethodUpdate || notification.method == meta.MethodRemove {
			recordsSink := func(recordData map[string]interface{}) error {
				state[i].DataSet = append(state[i].DataSet, notification.buildRecordStateObject(recordData, &action))
				return nil
			}
			notification.getRecords(notification.recordSet.Meta.Name, notification.recordsFilter, 1, recordsSink)
		} else {
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
	for i := range notification.previousState[actionIndex].DataSet {
		notificationData := make(map[string]interface{})
		notificationData["action"] = notification.method.AsString()
		notificationData["object"] = notification.recordSet.Meta.Name
		notificationData["previous"] = notification.previousState[actionIndex].DataSet[i]
		notificationData["current"] = notification.currentState[actionIndex].DataSet[i]
		notificationData["user"] = user
		notifications = append(notifications, notificationData)
	}
	return notifications
}

//Build object to use in notification
func (notification *RecordSetNotification) buildRecordStateObject(recordData map[string]interface{}, action *meta.Action) map[string]interface{} {
	stateObject := make(map[string]interface{}, 0)
	//object includes values listed in IncludeValues
	for fieldPath, alias := range action.IncludeValues {
		if value, ok := recordData[fieldPath]; ok {
			stateObject[alias] = value
		}
	}
	//	also include values which are updated being updated/created
	keys, _ := utils.GetMapKeysValues(notification.recordSet.DataSet[0])
	for _, key := range keys {
		if value, ok := recordData[key]; ok {
			stateObject[key] = value
		}
	}
	return stateObject
}
