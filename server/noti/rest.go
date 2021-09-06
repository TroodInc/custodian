package noti

import (
	"bytes"
	"custodian/logger"
	"custodian/server/auth"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"time"
)

var REST_REDELIVERY_PAUSE_IN_SEC time.Duration = 10
var REST_MAX_REDILIVERY_ATTEMPTS byte = 3

const (
	ErrRESTNoURLFound = "rest_no_url_found"
	ErrRESTFailedURL  = "rest_failed_url"
)

var restClient = &http.Client{
	Timeout: time.Second * 10,
}

type restNotifier struct {
	url             string
	activeIfNotRoot bool
}

func NewRestNotifier(args []string, activeIfNotRoot bool) (Notifier, error) {
	if len(args) < 1 {
		return nil, NewNotiError(ErrRESTNoURLFound, "Build a rest notifier failed. No URL found in arguments")
	}

	if _, err := url.Parse(args[0]); err != nil {
		return nil, NewNotiError(ErrRESTFailedURL, "Build a rest notifier failed. Specified URL '%s' is bad: %s", args[0], err.Error())
	}
	return &restNotifier{url: args[0], activeIfNotRoot: activeIfNotRoot}, nil
}

func (rn *restNotifier) redelivery(body []byte, attempt byte) {
	timer := time.NewTimer(time.Second * REST_REDELIVERY_PAUSE_IN_SEC)
	logger.Info("Scheduled '%d' attempt of re-delivery notification for '%s' URL in '%d' seconds", attempt, rn.url, REST_REDELIVERY_PAUSE_IN_SEC)
	<-timer.C
	logger.Info("Setup '%d' attempt of rre-delivery notification for '%s' URL", attempt, rn.url)

	tryAgain := func() {
		if attempt < REST_MAX_REDILIVERY_ATTEMPTS {
			go rn.redelivery(body, attempt+1)
		} else {
			logger.Error("Can't schedule re-delivery for '%s' URL. Achived max re-delivery attempts '%d'", rn.url, REST_MAX_REDILIVERY_ATTEMPTS)
		}
	}
	resp, err := postCallbackData(rn.url, bytes.NewReader(body))
	if err != nil {
		logger.Error("Error sending notification: %s", err.Error())
		tryAgain()
		return
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		logger.Error("Received an invalid response code '%d' from the '%s' notified server", resp.StatusCode, rn.url)
		tryAgain()
	}
}

func (rn *restNotifier) start(in chan *Event) {
	for event := range in {
		body, _ := json.Marshal(event.Obj())
		resp, err := postCallbackData(rn.url, bytes.NewReader(body))

		failed := false
		if err != nil {
			logger.Error("Error sending notification: %s", err.Error())
			failed = true
		} else {
			resp.Body.Close()
			if resp.StatusCode != 200 {
				logger.Error("Received an invalid response code '%d' from the '%s' notified server", resp.StatusCode, rn.url)
				failed = true
			}
		}

		//waiting for the end of cunsumption
		if failed {
			go rn.redelivery(body, 1)
		}
	}
}

func (rn *restNotifier) NewNotification() chan *Event {
	// Use buffer to reduce the effect of network latency on process
	in := make(chan *Event, 100)
	go rn.start(in)
	return in
}

func postCallbackData(url string, body io.Reader) (*http.Response, error){
	callbackRequest, _ := http.NewRequest("POST", url, body)
	callbackRequest.Header.Add("Content-Type", "application/json")

	serviceToken, err := auth.GetServiceToken()

	if err == nil {
		callbackRequest.Header.Add("Authorization", "Service " +serviceToken)
	}

	return restClient.Do(callbackRequest)
}