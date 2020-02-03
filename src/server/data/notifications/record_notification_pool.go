package notifications

import (
	"server/auth"
	"server/noti"
)


type RecordSetNotificationPool struct {
	notifications        []*RecordNotification
	notificationChannels map[string][]chan *noti.Event
	Notifiers            map[string]map[Method][] *noti.Notifier
	actions              map[string][]Action
}

func NewRecordSetNotificationPool(actions map[string][]Action) *RecordSetNotificationPool {
	pool := &RecordSetNotificationPool{
		notifications: make([]*RecordNotification, 0),
		Notifiers:     make(map[string]map[Method][] *noti.Notifier, 0),
		actions:       actions,
	}

	for metaName, items := range actions {
		pool.Notifiers[metaName] = make(map[Method][] *noti.Notifier, 0)

		for _, action := range items {
			notifierFactory, ok := noti.NotifierFactories[action.Protocol]
			if ok {
				notifier, err := notifierFactory(action.Args, action.ActiveIfNotRoot)
				if err == nil {
					pool.Notifiers[metaName][action.Method] = append(pool.Notifiers[metaName][action.Method], &notifier)
				}
			}
		}
	}
	return pool
}

func (pool *RecordSetNotificationPool) Add(notification *RecordNotification) {
	pool.notifications = append(pool.notifications, notification)
}

func (pool *RecordSetNotificationPool) CompleteSend(err error) {
	for _, items := range pool.notificationChannels {
		for _, c := range items {
			if err != nil {
				c <- noti.NewErrorEvent(err)
			}

			close(c)
		}
	}
}

func (pool *RecordSetNotificationPool) Push(user auth.User) {
	for _, notification := range pool.notifications {
		for _, chanel := range pool.getChannels(notification.Object, notification.Method) {
			chanel <- noti.NewObjectEvent(notification.BuildNotificationsData(user), notification.isRoot)
		}
	}
}

func (pool *RecordSetNotificationPool) ShouldBeProcessed() bool {
	return len(pool.notifications) > 0
}

func (pool *RecordSetNotificationPool) Notifications() []*RecordNotification {
	return pool.notifications
}

func (pool *RecordSetNotificationPool) getChannels(metaName string, method Method) []chan *noti.Event {
	key := metaName + method.AsString()
	channels, ok := pool.notificationChannels[key]
	if !ok {
		for _, notifier := range pool.Notifiers[metaName][method] {
			channels = append(pool.notificationChannels[key], noti.Broadcast(*notifier))
		}
	}
	return channels
}