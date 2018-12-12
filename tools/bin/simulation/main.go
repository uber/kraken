package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/alecthomas/kingpin"
)

func main() {
	eventFile := kingpin.Arg("events", "Network event file").Required().File()
	port := kingpin.Flag("port", "listening port").Default("3000").Int()
	kingpin.Parse()

	s := newServer(*eventFile)
	addr := fmt.Sprintf("localhost:%d", *port)
	log.Printf("Listening on %s ...", addr)
	log.Fatal(http.ListenAndServe(addr, s.handler()))
}
