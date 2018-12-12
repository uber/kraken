package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"

	"github.com/uber/kraken/lib/torrent/networkevent"
)

type server struct {
	events []*networkevent.Event
}

func newServer(eventFile *os.File) *server {
	var events []*networkevent.Event

	scanner := bufio.NewScanner(eventFile)
	for scanner.Scan() {
		line := scanner.Bytes()
		var event networkevent.Event
		if err := json.Unmarshal(line, &event); err != nil {
			log.Printf("Error unmarshalling event: %s\n", err)
			continue
		}
		events = append(events, &event)
	}
	events = networkevent.Filter(
		events,
		networkevent.AddTorrent,
		networkevent.AddActiveConn,
		networkevent.DropActiveConn,
		networkevent.BlacklistConn,
		networkevent.ReceivePiece,
		networkevent.TorrentComplete,
		networkevent.TorrentCancelled)
	networkevent.Sort(events)

	return &server{events}
}

func (s *server) handler() http.Handler {
	r := mux.NewRouter()

	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/static/html/app.html", http.StatusSeeOther)
	})

	fs := http.FileServer(http.Dir("./tools/bin/simulation/static/"))
	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/", fs))

	r.HandleFunc("/events", s.getEvents)

	return r
}

func (s *server) getEvents(w http.ResponseWriter, r *http.Request) {
	if err := json.NewEncoder(w).Encode(s.events); err != nil {
		log.Printf("Error encoding events: %s\n", err)
		http.Error(w, fmt.Sprintf("encode events: %s", err), 500)
		return
	}
}

type byTime []networkevent.Event

func (s byTime) Len() int           { return len(s) }
func (s byTime) Less(i, j int) bool { return s[i].Time.Before(s[j].Time) }
func (s byTime) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
