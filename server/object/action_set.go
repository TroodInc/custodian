package object

import (
	"custodian/server/noti"
	. "custodian/server/object/description"
)

type ActionSet struct {
	Original  []Action
	Notifiers map[Method]map[int]noti.Notifier
}

func newActionSet(array []Action) (*ActionSet, error) {
	notifiers := make(map[Method]map[int]noti.Notifier, 0)
	for i, _ := range array {
		//set action local id
		array[i].SetId(i)
		notifierFactory, ok := notifierFactories[array[i].Protocol]
		if !ok {
			ps, _ := array[i].Protocol.String()
			return nil, NewMetaDescriptionError("", "create_actions", ErrInternal, "Notifier notifierFactory not found for protocol: %s", ps)
		}

		notifier, err := notifierFactory(array[i].Args, array[i].ActiveIfNotRoot)
		if err != nil {
			return nil, err
		}
		if _, ok := notifiers[array[i].Method]; !ok {
			notifiers[array[i].Method] = make(map[int]noti.Notifier, 0)
		}
		notifiers[array[i].Method][array[i].Id()] = notifier
	}
	return &ActionSet{Original: array, Notifiers: notifiers}, nil
}

func (a *ActionSet) NewNotificationChannel(method Method, action *Action) chan *noti.Event {
	return noti.Broadcast(a.Notifiers[method][action.Id()])
}

func (a *ActionSet) FilterByMethod(method Method) []*Action {
	actions := make([]*Action, 0)
	for i := range a.Original {
		if a.Original[i].Method == method {
			actions = append(actions, &a.Original[i])
		}
	}
	return actions
}
