package object

import (
	"custodian/server/auth"
	"custodian/server/object/description"
)

type RecordSetNotificationPool struct {
	notifications      []*RecordSetNotification
	notificationSender *notificationSender
}

func (notificationPool *RecordSetNotificationPool) Add(notification *RecordSetNotification) {
	notificationPool.notifications = append(notificationPool.notifications, notification)
}

func (notificationPool *RecordSetNotificationPool) CompleteSend(err error) {
	notificationPool.notificationSender.complete(err)
}

func (notificationPool *RecordSetNotificationPool) Push(method description.Method, user auth.User) {

	for _, notification := range notificationPool.notifications {
		if method.AsString() == notification.Method.AsString() {
			notificationPool.notificationSender.push(notification, user)
		}
	}
}

func (notificationPool *RecordSetNotificationPool) Notifications() []*RecordSetNotification {
	return notificationPool.notifications
}

func NewRecordSetNotificationPool() *RecordSetNotificationPool {
	return &RecordSetNotificationPool{
		notifications:      make([]*RecordSetNotification, 0),
		notificationSender: newNotificationSender(),
	}
}
