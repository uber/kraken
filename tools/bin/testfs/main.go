package main

import (
	"flag"
	"fmt"
	"net/http"

	"github.com/uber/kraken/lib/backend/testfs"
	"github.com/uber/kraken/utils/log"
)

func main() {
	port := flag.Int("port", 0, "port which testfs server listens on")
	flag.Parse()

	if *port == 0 {
		log.Fatal("-port required")
	}

	server := testfs.NewServer()
	defer server.Cleanup()

	addr := fmt.Sprintf(":%d", *port)
	log.Infof("Starting testfs server on %s", addr)
	log.Fatal(http.ListenAndServe(addr, server.Handler()))
}
