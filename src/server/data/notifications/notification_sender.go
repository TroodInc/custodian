package notifications

import (
	"server/object/meta"
	"server/noti"
	"server/auth"
	"server/object/description"
)

type notificationSender struct {
	notificationChannels map[string]chan *noti.Event
}

func newNotificationSender() *notificationSender {
	return &notificationSender{notificationChannels: make(map[string]chan *noti.Event)}
}

func (notificationSender *notificationSender) push(recordSetNotification *RecordSetNotification, user auth.User) {
	for actionIndex, action := range recordSetNotification.recordSet.Meta.Actions.Original {
		if recordSetNotification.Method == action.Method {
			notificationChannel := notificationSender.getNotificationChannel(recordSetNotification.recordSet.Meta, recordSetNotification.Method, &action)
			for _, notificationObject := range recordSetNotification.BuildNotificationsData(actionIndex, user) {
				notificationChannel <- noti.NewObjectEvent(notificationObject, recordSetNotification.isRoot)
			}
		}
	}
}

func (notificationSender *notificationSender) getNotificationChannel(meta *meta.Meta, method description.Method, action *description.Action) chan *noti.Event {
	key := meta.Name + method.AsString()
	notificationChannel, ok := notificationSender.notificationChannels[key]
	if !ok {
		notificationChannel = meta.Actions.NewNotificationChannel(method, action)
		notificationSender.notificationChannels[key] = notificationChannel
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
