package notifications

import (
	"server/meta"
	"server/data/record"
	"strconv"
)

type RecordSetNotification struct {
	meta *meta.Meta
	//recordKeys    []string
	recordsFilter string
	isRoot        bool
	method        meta.Method
	previousState *record.RecordSet
	currentState  *record.RecordSet
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
		//recordKeys:    recordKeys,
		meta:          recordSet.Meta,
		recordsFilter: filter,
		isRoot:        isRoot,
		method:        method,
		previousState: &record.RecordSet{},
		currentState:  &record.RecordSet{},
		getRecords:    getRecords,
	}
}

func (notification *RecordSetNotification) CapturePreviousState() {
	notification.captureState(notification.previousState)
}

func (notification *RecordSetNotification) CaptureCurrentState() {
	notification.captureState(notification.currentState)
}

func (notification *RecordSetNotification) captureState(state *record.RecordSet) {
	recordsSink := func(recordData map[string]interface{}) error {
		state.DataSet = append(state.DataSet, recordData)
		return nil
	}
	notification.getRecords(notification.meta.Name, notification.recordsFilter, 1, recordsSink)
}

func (notification *RecordSetNotification) ShouldBeProcessed() bool {
	return len(notification.meta.Actions.Notifiers[notification.method]) > 0
}
