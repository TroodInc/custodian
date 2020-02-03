package notifications

import (
	"server/auth"
)

type RecordNotification struct {
	Object 				string
	Method             	Method
	PreviousState		map[string]interface{}
	CurrentState		map[string]interface{}
	isRoot				bool
}

func NewRecordNotification (object string, method Method, previous, current map[string]interface{}, isRoot bool) *RecordNotification{
	return &RecordNotification{object, method,  previous, current, isRoot}
}

//Build notification object for each record in recordSet for given action
func (notification *RecordNotification) BuildNotificationsData(user auth.User) map[string]interface{} {
	notificationData := map[string]interface{}{
		"action": notification.Method.AsString(),
		"object": notification.Object,
		"previous": notification.PreviousState,
		"current": notification.CurrentState,
		"user": user,
	}

	return notificationData
}