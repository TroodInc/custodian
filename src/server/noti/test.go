package noti

type TestNotifier struct {
	Buff	[]map[string]interface{}
}

func NewTestNotifier(args []string, _ bool) (Notifier, error) {
	return &TestNotifier{Buff: make([]map[string]interface{}, 0)}, nil
}

func (rn *TestNotifier) NewNotification() chan *Event {
	in := make(chan *Event, 100)
	go rn.start(in)
	return in
}

func (rn *TestNotifier) start(in chan *Event) {
	for notification := range in {
		rn.Buff = append(rn.Buff, notification.obj)
	}
}


