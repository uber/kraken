package networkevent

import (
	"sort"
	"time"

	"code.uber.internal/infra/kraken/utils/stringset"
)

type byTime []*Event

func (s byTime) Len() int           { return len(s) }
func (s byTime) Less(i, j int) bool { return s[i].Time.Before(s[j].Time) }
func (s byTime) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// Sort sorts events in place by timestamp.
func Sort(events []*Event) {
	sort.Sort(byTime(events))
}

// Filter filters events by name.
func Filter(events []*Event, names ...Name) []*Event {
	s := make(stringset.Set)
	for _, name := range names {
		s.Add(string(name))
	}
	var f []*Event
	for _, e := range events {
		if s.Has(string(e.Name)) {
			f = append(f, e)
		}
	}
	return f
}

// StripTimestamps overwrites timestamps in events as empty, allowing clients
// to check equality of events.
//
// Mutates events in place and returns events for chaining purposes.
func StripTimestamps(events []*Event) []*Event {
	for _, e := range events {
		e.Time = time.Time{}
	}
	return events
}
