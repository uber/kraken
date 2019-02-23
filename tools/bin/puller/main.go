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
package main

import (
	"flag"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/uber/kraken/utils/log"
)

const listenAddr = "0.0.0.0:5055"

// mini service that receives docker push notifications and pull from local registry
func main() {
	var docker bool
	var source string
	var image string
	flag.StringVar(&source, "source", "", "source registry")
	flag.StringVar(&image, "image", "", "<repo>:<tag>")
	flag.BoolVar(&docker, "docker", false, "if to use docker")
	flag.Parse()

	// If source and image are specified, puller exits after pulling one image.
	if source != "" && image != "" {
		log.Infof("pulling image from %s/%s", source, image)
		str := strings.Split(image, ":")
		if len(str) != 2 {
			log.Fatalf("invalid image: %s", image)
		}
		if err := PullImage(source, str[0], str[1], docker); err != nil {
			log.Fatal(err)
		}
		return
	}

	// NotificationHandler pulls image once it receives notification from docker registry.
	// It is a long running process.
	notification, err := NewNotificationHandler(200, docker)
	if err != nil {
		log.Fatal(err)
	}

	router := mux.NewRouter()
	router.HandleFunc("/", HealthHandler).
		Methods("GET")
	router.HandleFunc("/notifications", notification.Handler).
		Methods("POST")
	router.HandleFunc("/notifications", HealthHandler).
		Methods("GET")

	log.Infof("listening on %s", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, router))
}
