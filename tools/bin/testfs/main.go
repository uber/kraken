// Copyright (c) 2019 Uber Technologies, Inc.
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
