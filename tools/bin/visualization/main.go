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
