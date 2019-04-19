package server

import "time"

// Notification holds all events.
type Notification struct {
	Events []Event
}

// Event holds the details of a event.
type Event struct {
	ID        string `json:"Id"`
	TimeStamp time.Time
	Action    string
	Target    *Target
	Request   *Request
	Actor     *Actor
}

// Target holds information about the target of a event.
type Target struct {
	MediaType  string
	Digest     string
	Repository string
	URL        string `json:"Url"`
	Tag        string
}

// Actor holds information about actor.
type Actor struct {
	Name string
}

// Request holds information about a request.
type Request struct {
	ID        string `json:"Id"`
	Method    string
	UserAgent string
}
