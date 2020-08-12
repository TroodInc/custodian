package noti

import (
	"net/url"
)

type testNotifier struct {
	url             string
	activeIfNotRoot bool
}

func NewTestNotifier(args []string, activeIfNotRoot bool) (Notifier, error) {
	if len(args) < 1 {
		return nil, NewNotiError(ErrRESTNoURLFound, "Build a rest notifier failed. No URL found in arguments")
	}

	if _, err := url.Parse(args[0]); err != nil {
		return nil, NewNotiError(ErrRESTFailedURL, "Build a rest notifier failed. Specified URL '%s' is bad: %s", args[0], err.Error())
	}
	return &testNotifier{url: args[0], activeIfNotRoot: activeIfNotRoot}, nil
}

func (rn *testNotifier) NewNotification() chan *Event {
	in := make(chan *Event, 100)
	return in
}
