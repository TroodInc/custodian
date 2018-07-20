package notifications

import (
	"server/meta"
	"server/noti"
	"server/auth"
)

type notificationSender struct {
	notificationChannels map[string]chan *noti.Event
}

func newNotificationSender() *notificationSender {
	return &notificationSender{notificationChannels: make(map[string]chan *noti.Event)}
}

func (notificationSender *notificationSender) push(recordStateNotification *RecordSetNotification, user auth.User) {
	notificationChannel := notificationSender.getNotificationChannel(recordStateNotification.meta, recordStateNotification.method)
	for i := range recordStateNotification.previousState.DataSet {
		notificationData := make(map[string]interface{})
		notificationData["action"] = recordStateNotification.method.AsString()
		notificationData["object"] = recordStateNotification.meta.Name
		notificationData["previous"] = recordStateNotification.previousState.DataSet[i]
		notificationData["current"] = recordStateNotification.currentState.DataSet[i]
		notificationData["user"] = user

		notificationChannel <- noti.NewObjectEvent(notificationData, recordStateNotification.isRoot)
	}
}

func (notificationSender *notificationSender) getNotificationChannel(meta *meta.Meta, method meta.Method) chan *noti.Event {
	notificationChannel, ok := notificationSender.notificationChannels[meta.Name]
	if !ok {
		notificationChannel = meta.Actions.StartNotification(method)
		notificationSender.notificationChannels[meta.Name] = notificationChannel
	}
	return notificationChannel
}

func (notificationSender *notificationSender) complete(err error) {
	if err == nil {
		notificationSender.close()
	} else {
		notificationSender.failed(err)
	}
}

func (notificationSender *notificationSender) close() {
	for _, c := range notificationSender.notificationChannels {
		close(c)
	}
}

func (notificationSender *notificationSender) failed(err error) {
	for _, c := range notificationSender.notificationChannels {
		c <- noti.NewErrorEvent(err)
		close(c)
	}
}