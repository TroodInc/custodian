package data

import (
	"server/meta"
	"server/noti"
	"server/auth"
)

const NOTIFICATION_CREATE = "create"
const NOTIFICATION_UPDATE = "update"
const NOTIFICATION_DELETE = "delete"

type notificationSender struct {
	method meta.Method
	notifs map[string]chan *noti.Event
}

func newNotificationSender(m meta.Method) *notificationSender {
	return &notificationSender{method: m, notifs: make(map[string]chan *noti.Event)}
}

func (n *notificationSender) push(action string, meta *meta.Meta, recordData map[string]interface{}, user auth.User, isRoot bool) {

	notificationData := make(map[string]interface{})
	notificationData["action"] = action
	notificationData["object"] = meta.MetaDescription.Name
	notificationData["data"] = recordData
	notificationData["user"] = user

	notifchan, ok := n.notifs[meta.Name]
	if !ok {
		notifchan = meta.Actions.StartNotification(n.method)
		n.notifs[meta.Name] = notifchan
	}
	notifchan <- noti.NewObjectEvent(notificationData, isRoot)
}

func (n *notificationSender) complete(err error) {
	if err == nil {
		n.close()
	} else {
		n.failed(err)
	}
}

func (n *notificationSender) close() {
	for _, c := range n.notifs {
		close(c)
	}
}

func (n *notificationSender) failed(err error) {
	for _, c := range n.notifs {
		c <- noti.NewErrorEvent(err)
		close(c)
	}
}
