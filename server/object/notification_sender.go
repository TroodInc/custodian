package object

import (
	"custodian/server/auth"
	"custodian/server/noti"
	"custodian/server/object/description"
	"strconv"
)

type notificationSender struct {
	notificationChannels map[string]chan *noti.Event
}

func newNotificationSender() *notificationSender {
	return &notificationSender{notificationChannels: make(map[string]chan *noti.Event)}
}

func (notificationSender *notificationSender) push(recordSetNotification *RecordSetNotification, user auth.User) {
	for _, action := range recordSetNotification.Actions {
		if action.Method.AsString() == recordSetNotification.Method.AsString() {

			notificationChannel := action.NewNotificationChannel()
			for _, notificationObject := range recordSetNotification.BuildNotificationsData(
				recordSetNotification.PreviousState[action.Id()],
				recordSetNotification.CurrentState[action.Id()],
				user,
			) {
				notificationChannel <- noti.NewObjectEvent(notificationObject, recordSetNotification.isRoot)
			}
		}
	}
}

func (notificationSender *notificationSender) getNotificationChannel(meta *Meta, method description.Method, action *description.Action) chan *noti.Event {
	key := meta.Name + method.AsString() + strconv.Itoa(action.Id())
	notificationChannel, ok := notificationSender.notificationChannels[key]
	if !ok {
		notificationChannel = action.NewNotificationChannel()
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
