package main

import (
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/docker/distribution/notifications"

	"github.com/uber/kraken/utils/log"
)

const (
	baseManifestQuery = "http://%s/v2/%s/manifests/%s"
	baseLayerQuery    = "http://%s/v2/%s/blobs/%s"
	transferTimeout   = 120 * time.Second
	localSource       = "localhost:5051"
	tempDir           = "/tmp/kraken/tmp/puller/"
)

// HealthHandler tells haproxy we'fre still alive
func HealthHandler(w http.ResponseWriter, request *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("OK"))
}

// NotificationHandler receives docker push notification
type NotificationHandler struct {
	queue     chan uint8
	useDocker bool
}

// NewNotificationHandler creates a new Notifaction
func NewNotificationHandler(maxChanSize int, useDocker bool) (*NotificationHandler, error) {
	if err := os.MkdirAll(tempDir, 0775); err != nil {
		return nil, err
	}
	return &NotificationHandler{
		queue:     make(chan byte, maxChanSize),
		useDocker: useDocker,
	}, nil
}

// Handler handles messages defined in http://godoc.org/github.com/docker/distribution/notifications.
func (n *NotificationHandler) Handler(w http.ResponseWriter, r *http.Request) {
	log.Debugf("notification received")
	decoder := json.NewDecoder(r.Body)
	var envelope notifications.Envelope

	if err := decoder.Decode(&envelope); err != nil {
		log.With("err", err).Error("cannot decode envelope")
		return
	}
	for _, event := range envelope.Events {
		if event.Action == notifications.EventActionPush {
			url := event.Target.URL
			repo := event.Target.Repository
			tag := event.Target.Tag
			digest := guessDigest(url, repo)
			if len(digest) == 0 {
				log.Debugf("non tag push action: target.URL: '%s', target.Repository: '%s'", url, repo)
			} else {
				select {
				case n.queue <- 'c':
					time.Sleep(2 * time.Second)
					go func() {
						PullImage(localSource, repo, tag, n.useDocker)
						<-n.queue
					}()
				default:
					// drop if queue full
					log.Infof("queue full. drop %s:%s", repo, tag)
				}
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	resString := "OK"
	w.Write([]byte(resString))
}
