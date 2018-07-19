package data

import "server/auth"

type RecordStateNotificationPool struct {
	processor          *Processor
	notifications      []*RecordStateNotification
	notificationSender *notificationSender
}

func (notificationPool *RecordStateNotificationPool) Add(notification *RecordStateNotification) {
	notificationPool.notifications = append(notificationPool.notifications, notification)
}

func (notificationPool *RecordStateNotificationPool) CompleteSend(err error) {
	notificationPool.notificationSender.complete(err)
}

func (notificationPool *RecordStateNotificationPool) CaptureCurrentState() {
	for _, notification := range notificationPool.notifications {
		notification.CaptureCurrentState(notificationPool.processor)
	}
}

func (notificationPool *RecordStateNotificationPool) Push(user auth.User) {
	for _, notification := range notificationPool.notifications {
		notificationPool.notificationSender.push(notification, user)
	}
}

func NewRecordStateNotificationPool(processor *Processor) *RecordStateNotificationPool {
	return &RecordStateNotificationPool{
		notifications:      make([]*RecordStateNotification, 0),
		processor:          processor,
		notificationSender: newNotificationSender(),
	}
}
