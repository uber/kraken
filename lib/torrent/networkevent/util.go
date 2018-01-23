package networkevent

import (
	"sort"

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
