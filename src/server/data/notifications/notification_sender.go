package notifications

import (
	"server/object/meta"
	"server/noti"
	"server/auth"
	"server/object/description"
	"strconv"
)

type notificationSender struct {
	notificationChannels map[string]chan *noti.Event
}

// returns a new notification sender
func newNotificationSender() *notificationSender {
	return &notificationSender{notificationChannels: make(map[string]chan *noti.Event)}
}

// pushes a notification to the record set notification
func (notificationSender *notificationSender) push(recordSetNotification *RecordSetNotification, user auth.User) {
	for _, action := range recordSetNotification.Actions {
		notificationChannel := notificationSender.getNotificationChannel(recordSetNotification.recordSet.Meta, recordSetNotification.Method, action)
		for _, notificationObject := range recordSetNotification.BuildNotificationsData(
			recordSetNotification.PreviousState[action.Id()],
			recordSetNotification.CurrentState[action.Id()],
			user,
		) {
			notificationChannel <- noti.NewObjectEvent(notificationObject, recordSetNotification.isRoot)
		}
	}
}

// returns the notificationChannel
func (notificationSender *notificationSender) getNotificationChannel(meta *meta.Meta, method description.Method, action *description.Action) chan *noti.Event {
	key := meta.Name + method.AsString() + strconv.Itoa(action.Id())
	notificationChannel, ok := notificationSender.notificationChannels[key]
	if !ok {
		notificationChannel = meta.ActionSet.NewNotificationChannel(method, action)
		notificationSender.notificationChannels[key] = notificationChannel
	}
	return notificationChannel
}

// will close the chanel if there was no error
func (notificationSender *notificationSender) complete(err error) {
	if err == nil {
		notificationSender.close()
	} else {
		notificationSender.failed(err)
	}
}

// hepler function to close the chanel
func (notificationSender *notificationSender) close() {
	for _, c := range notificationSender.notificationChannels {
		close(c)
	}
}

// returns a new error event and close the chanel in case of failure for that 
func (notificationSender *notificationSender) failed(err error) {
	for _, c := range notificationSender.notificationChannels {
		c <- noti.NewErrorEvent(err)
		close(c)
	}
}
