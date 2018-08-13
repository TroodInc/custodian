package meta

import (
	"server/noti"
	. "server/object/description"
)

type ActionSet struct {
	Original  []Action
	Notifiers map[Method]map[string]noti.Notifier
}

func newActions(array []Action) (*ActionSet, error) {
	notifiers := make(map[Method]map[string]noti.Notifier, 0)
	for i, _ := range array {
		factory, ok := notifierFactories[array[i].Protocol]
		if !ok {
			ps, _ := array[i].Protocol.String()
			return nil, NewMetaDescriptionError("", "create_actions", ErrInternal, "Notifier factory not found for protocol: %s", ps)
		}

		notifier, err := factory(array[i].Args, array[i].ActiveIfNotRoot)
		if err != nil {
			return nil, err
		}
		if _, ok := notifiers[array[i].Method]; !ok {
			notifiers[array[i].Method] = make(map[string]noti.Notifier, 0)
		}
		notifiers[array[i].Method][array[i].GetUid()] = notifier
	}
	return &ActionSet{Original: array, Notifiers: notifiers}, nil
}

func (a *ActionSet) NewNotificationChannel(method Method, action *Action) chan *noti.Event {
	return noti.Broadcast(a.Notifiers[method][action.GetUid()])
}
