package noti

type TestNotifier struct {
	url             string
	activeIfNotRoot bool
	Events          chan *Event
}

func NewTestNotifier(args []string, activeIfNotRoot bool) (Notifier, error) {
	events := make(chan *Event, 100)
	return &TestNotifier{url: args[0], activeIfNotRoot: activeIfNotRoot, Events: events}, nil
}

func (rn *TestNotifier) NewNotification() chan *Event {
	return rn.Events
}