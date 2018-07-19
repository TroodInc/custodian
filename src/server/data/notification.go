package data

import (
	"server/meta"
	"strconv"
)

type RecordStateNotification struct {
	objMeta       *meta.Meta
	recordKey     string
	isRoot        bool
	method        meta.Method
	previousState map[string]interface{}
	currentState  map[string]interface{}
}

func NewRecordStateNotification(objMeta *meta.Meta, key interface{}, isRoot bool, method meta.Method) *RecordStateNotification {
	var keyValue string
	switch value := key.(type) {
	case string:
		keyValue = value
	case int:
		keyValue = strconv.Itoa(value)
	}

	return &RecordStateNotification{
		objMeta,
		keyValue,
		isRoot,
		method,
		make(map[string]interface{}),
		make(map[string]interface{}),
	}
}

func (notification *RecordStateNotification) CapturePreviousState(processor *Processor) {
	if record, err := processor.Get(notification.objMeta.Name, notification.recordKey, 2); err != nil {

	} else {
		notification.previousState = record
	}
}

func (notification *RecordStateNotification) CaptureCurrentState(processor *Processor) {
	if record, err := processor.Get(notification.objMeta.Name, notification.recordKey, 2); err != nil {
	} else {
		notification.currentState = record
	}
}

//check if notification action exists for given action
func (notification *RecordStateNotification) ShouldBeProcessed() bool {
	return len(notification.objMeta.Actions.Notifiers[notification.method]) > 0
}
