package object

import (
	"custodian/server/auth"
	"custodian/server/object/description"
	"custodian/utils"
)

type RecordSetNotification struct {
	recordSet     *RecordSet
	isRoot        bool
	Actions       []*description.Action
	Method        description.Method
	PreviousState map[int]*RecordSet
	CurrentState  map[int]*RecordSet
}

func NewRecordSetNotification(recordSet *RecordSet, isRoot bool, method description.Method) *RecordSetNotification {
	actions := recordSet.Meta.Actions
	return &RecordSetNotification{
		recordSet:     recordSet,
		isRoot:        isRoot,
		Method:        method,
		Actions:       actions,
		PreviousState: make(map[int]*RecordSet, len(actions)), //for both arrays index is an corresponding
		CurrentState:  make(map[int]*RecordSet, len(actions)), //action's index, states are action-specific due to actions's own fields configuration(IncludeValues)
	}
}

func (notification *RecordSetNotification) CapturePreviousState(objects []*Record) {
	notification.captureState(notification.PreviousState, objects)
}

func (notification *RecordSetNotification) CaptureCurrentState(objects []*Record) {
	notification.captureState(notification.CurrentState, objects)
}

func (notification *RecordSetNotification) ShouldBeProcessed() bool {

	for _, a := range notification.recordSet.Meta.Actions {
		if a.Method.AsString() == notification.Method.AsString() {
			return true
		}
	}

	return false
}

//Build notification object for each record in recordSet for given action
func (notification *RecordSetNotification) BuildNotificationsData(previousState *RecordSet, currentState *RecordSet, user auth.User) []map[string]interface{} {
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

func (notification *RecordSetNotification) captureState(state map[int]*RecordSet, objects []*Record) {
	//capture state if recordSet has PKs defined, set empty map otherwise, because records cannot be retrieved
	for _, action := range notification.Actions {

		state[action.Id()] = &RecordSet{Meta: notification.recordSet.Meta, Records: make([]*Record, 0)}

		for _, obj := range objects {
			if obj != nil {
				state[action.Id()].Records = append(
					state[action.Id()].Records,
					NewRecord(state[action.Id()].Meta, notification.buildRecordStateObject(obj, action), obj.processor),
				)
			}
		}

		//fill DataSet with empty values
		if len(state[action.Id()].Records) == 0 {
			state[action.Id()].Records = make([]*Record, len(notification.recordSet.Records))
		}
	}
}

//Build object to use in notification
func (notification *RecordSetNotification) buildRecordStateObject(recordData *Record, action *description.Action) map[string]interface{} {

	stateObject := make(map[string]interface{}, 0)

	if recordData != nil {
		//	include values which are being updated/created
		if action.Method != description.MethodRemove {
			keys, _ := utils.GetMapKeysValues(notification.recordSet.Records[0].Data)
			for _, key := range keys {
				if value, ok := recordData.Data[key]; ok {
					stateObject[key] = value
				}
			}
		} else {
			//copy entire record data if it is being removed
			stateObject = utils.CloneMap(recordData.Data)
		}

		//include values listed in IncludeValues
		for alias, getterConfig := range action.IncludeValues {
			stateObject[alias] = recordData.GetValue(getterConfig)
			//remove key if alias is not equal to actual getterConfig and stateObject already
			// contains value under the getterConfig key, that is getterConfig key should be replaced with alias

			//remove duplicated values
			if getterString, ok := getterConfig.(string); ok {
				if _, ok := stateObject[getterString]; ok && getterConfig != alias {
					delete(stateObject, getterString)
				}
			}
		}
	}
	return stateObject
}

func adaptRecordData(recordData map[string]interface{}) map[string]interface{} {
	adaptedRecordData := map[string]interface{}{}
	for key, value := range recordData {
		switch castValue := value.(type) {
		case DLink:
			adaptedRecordData[key] = castValue.Id
		default:
			adaptedRecordData[key] = castValue
		}
	}
	return adaptedRecordData
}
