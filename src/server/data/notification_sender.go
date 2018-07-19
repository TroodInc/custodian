package data

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

func (n *notificationSender) push(recordStateNotification *RecordStateNotification, user auth.User) {

	notificationData := make(map[string]interface{})
	notificationData["action"] = recordStateNotification.method.AsString()
	notificationData["object"] = recordStateNotification.objMeta.Name
	notificationData["previous"] = recordStateNotification.previousState
	notificationData["current"] = recordStateNotification.currentState
	notificationData["user"] = user

	n.getNotificationChannel(recordStateNotification.objMeta, recordStateNotification.method) <- noti.NewObjectEvent(notificationData, recordStateNotification.isRoot)
}

func (n *notificationSender) getNotificationChannel(meta *meta.Meta, method meta.Method) chan *noti.Event {
	notificationChannel, ok := n.notificationChannels[meta.Name]
	if !ok {
		notificationChannel = meta.Actions.StartNotification(method)
		n.notificationChannels[meta.Name] = notificationChannel
	}
	return notificationChannel
}

func (n *notificationSender) complete(err error) {
	if err == nil {
		n.close()
	} else {
		n.failed(err)
	}
}

func (n *notificationSender) close() {
	for _, c := range n.notificationChannels {
		close(c)
	}
}

func (n *notificationSender) failed(err error) {
	for _, c := range n.notificationChannels {
		c <- noti.NewErrorEvent(err)
		close(c)
	}
}
