package meta

import (
	"server/noti"
	"encoding/json"
	"crypto/md5"
)

type actions struct {
	Original  []Action
	Notifiers map[Method]map[string]noti.Notifier
}

func newActions(array []Action) (*actions, error) {
	notifiers := make(map[Method]map[string]noti.Notifier, 0)
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
		if _, ok := notifiers[array[i].Method]; !ok {
			notifiers[array[i].Method] = make(map[string]noti.Notifier, 0)
		}
		notifiers[array[i].Method][array[i].GetUid()] = notifier
	}
	return &actions{Original: array, Notifiers: notifiers}, nil
}

func (a *actions) NewNotificationChannel(method Method, action *Action) chan *noti.Event {
	return noti.Broadcast(a.Notifiers[method][action.GetUid()])
}

type Action struct {
	Method          Method            `json:"method"`
	Protocol        Protocol          `json:"protocol"`
	Args            []string          `json:"args,omitempty"`
	ActiveIfNotRoot bool              `json:"activeIfNotRoot"`
	IncludeValues   map[string]string `json:"includeValues"`
}

func (action *Action) GetUid() string {
	arrBytes := []byte{}
	jsonBytes, _ := json.Marshal(action)
	arrBytes = append(arrBytes, jsonBytes...)
	bytesResult := md5.Sum(arrBytes)
	return string(bytesResult[:])
}
