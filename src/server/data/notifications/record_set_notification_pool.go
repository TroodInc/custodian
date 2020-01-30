package notifications

import "server/auth"

type RecordSetNotificationPool struct {
	notifications      []*RecordSetNotification
	notificationSender *notificationSender
}

func (notificationPool *RecordSetNotificationPool) Add(notification *RecordSetNotification) {
	notificationPool.notifications = append(notificationPool.notifications, notification)
}

func (notificationPool *RecordSetNotificationPool) CompleteSend(err error) {
	if err == nil {
		notificationPool.notificationSender.close()
	} else {
		notificationPool.notificationSender.failed(err)
	}
}

func (notificationPool *RecordSetNotificationPool) CaptureCurrentState() {
	for _, notification := range notificationPool.notifications {
		notification.CaptureCurrentState()
	}
}

func (notificationPool *RecordSetNotificationPool) Push(user auth.User) {
	for _, notification := range notificationPool.notifications {
		notificationPool.notificationSender.push(notification, user)
	}
}

func (notificationPool *RecordSetNotificationPool) ShouldBeProcessed() bool {
	shouldBeProcessed := false
	for _, notification := range notificationPool.notifications {
		if notification.ShouldBeProcessed() {
			shouldBeProcessed = true
		}
	}
	return shouldBeProcessed
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
