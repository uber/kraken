// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package networkevent

import (
	"sort"
	"time"

	"github.com/uber/kraken/utils/stringset"
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
