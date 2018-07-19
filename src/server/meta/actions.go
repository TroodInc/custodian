package meta

import "server/noti"

type actions struct {
	Original  []action
	Notifiers map[Method][]noti.Notifier
}

func newActions(array []action) (*actions, error) {
	notifiers := make(map[Method][]noti.Notifier)
	for i, _ := range array {
		factory, ok := notifierFactories[array[i].Protocol]
		if !ok {
			ps, _ := array[i].Protocol.String()
			return nil, NewMetaError("", "create_actions", ErrInternal, "Notifier factory not found for protocol: %s", ps)
		}

		notifier, err := factory(array[i].Args, array[i].ActiveIfNotRoot)
		if err != nil {
			return nil, err
		}
		m := array[i].Method
		notifiers[m] = append(notifiers[m], notifier)
	}
	return &actions{Original: array, Notifiers: notifiers}, nil
}

func (a *actions) StartNotification(method Method) chan *noti.Event {
	return noti.Broadcast(a.Notifiers[method])
}


type action struct {
	Method          Method            `json:"method"`
	Protocol        Protocol          `json:"protocol"`
	Args            []string          `json:"args,omitempty"`
	ActiveIfNotRoot bool              `json:"activeIfNotRoot"`
	IncludeValues   map[string]string `json:"includeValues"`
}
