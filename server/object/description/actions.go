package description

import (
	"custodian/server/errors"
	"custodian/server/noti"
)

type Action struct {
	Method          Method                 `json:"method"`
	Protocol        noti.Protocol               `json:"protocol"`
	Args            []string               `json:"args,omitempty"`
	ActiveIfNotRoot bool                   `json:"activeIfNotRoot"`
	IncludeValues   map[string]interface{} `json:"includeValues"`
	Name            string                 `json:"name"`
	id              int
	Notifier        noti.Notifier
}

func InitAction(a *Action) (*Action, error) {
	f, ok := noti.NotifierFactories[a.Protocol]
	if !ok {
		return nil, errors.NewValidationError("ErrInternal", "Notifier notifierFactory not found for protocol: %s", a.Protocol)
	}

	n, err := f(a.Args, a.ActiveIfNotRoot)
	if err != nil {
		return nil, err
	}

	a.Notifier = n
	return  a, nil
}

func (a *Action) NewNotificationChannel() chan *noti.Event {
	//return noti.Broadcast(a.Notifier)
	return a.Notifier.NewNotification()
}

func (a *Action) Clone() *Action {
	return &Action{
		Method:          a.Method,
		Protocol:        a.Protocol,
		Args:            a.Args,
		ActiveIfNotRoot: a.ActiveIfNotRoot,
		IncludeValues:   a.IncludeValues,
		Name:            a.Name,
	}
}

func (a *Action) SetId(id int) {
	a.id = id
}

func (a *Action) Id() int {
	return a.id
}