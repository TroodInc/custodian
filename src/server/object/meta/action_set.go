package meta

//import (
//	"server/data/notifications"
//	"server/noti"
//	. "server/object/description"
//)
//
//type ActionSet struct {
//	Original  []notifications.Action
//	Notifiers map[notifications.Method]map[int]noti.Notifier
//}
//
//func newActionSet(array []notifications.Action) (*ActionSet, error) {
//	notifiers := make(map[notifications.Method]map[int]noti.Notifier, 0)
//	for i, _ := range array {
//		//set action local id
//		array[i].SetId(i)
//		notifierFactory, ok := notifierFactories[array[i].Protocol]
//		if !ok {
//			ps, _ := array[i].Protocol.String()
//			return nil, NewMetaDescriptionError("", "create_actions", ErrInternal, "Notifier notifierFactory not found for protocol: %s", ps)
//		}
//
//		notifier, err := notifierFactory(array[i].Args, array[i].ActiveIfNotRoot)
//		if err != nil {
//			return nil, err
//		}
//		if _, ok := notifiers[array[i].Method]; !ok {
//			notifiers[array[i].Method] = make(map[int]noti.Notifier, 0)
//		}
//		notifiers[array[i].Method][array[i].Id()] = notifier
//	}
//	return &ActionSet{Original: array, Notifiers: notifiers}, nil
//}
//
//func (a *ActionSet) FilterByMethod(method notifications.Method) []*notifications.Action {
//	actions := make([]*notifications.Action, 0)
//	for i := range a.Original {
//		if a.Original[i].Method == method {
//			actions = append(actions, &a.Original[i])
//		}
//	}
//	return actions
//}
