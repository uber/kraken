package main

import (
	"fmt"
	"log"
)

type profile struct {
	proxy   string
	origin  string
	tracker string
}

func getProfile(cluster string) *profile {
	var proxy, origin, tracker string
	switch cluster {

	default:
		log.Fatalf("No profile configured for cluster %s", cluster)
	}
	return &profile{
		proxy:   fmt.Sprintf("%s:%d", proxy, 5367),
		origin:  fmt.Sprintf("%s:%d", origin, 9003),
		tracker: fmt.Sprintf("%s:%d", tracker, 8351),
	}
}
