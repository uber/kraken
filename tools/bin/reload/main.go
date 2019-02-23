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
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/uber/kraken/lib/torrent/scheduler"
	"github.com/uber/kraken/utils/configutil"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/osutil"
)

type appConfig struct {
	Scheduler scheduler.Config `yaml:"scheduler"`
}

func reload(addr string, config scheduler.Config) error {
	b, err := json.Marshal(config)
	if err != nil {
		return err
	}
	_, err = httputil.Patch(
		fmt.Sprintf("http://%s/x/config/scheduler", addr),
		httputil.SendBody(bytes.NewBuffer(b)),
		httputil.SendTimeout(5*time.Second))
	return err
}

func main() {
	configFile := flag.String("config", "", "config file")
	hostFile := flag.String("f", "", "host file")
	hostStr := flag.String("hosts", "", "comma-separated hosts")
	port := flag.Int("port", 7602, "server port (different for agent / origin)")
	flag.Parse()

	if *configFile == "" {
		panic("-config required")
	}
	if (*hostFile != "" && *hostStr != "") || (*hostFile == "" && *hostStr == "") {
		panic("must set either -f or -hosts")
	}
	if *port == 0 {
		panic("-port must be non-zero")
	}

	var hosts []string
	if *hostFile != "" {
		f, err := os.Open(*hostFile)
		if err != nil {
			panic(err)
		}
		hosts, err = osutil.ReadLines(f)
		if err != nil {
			panic(err)
		}
	} else if *hostStr != "" {
		hosts = strings.Split(*hostStr, ",")
	}

	var config appConfig
	if err := configutil.Load(*configFile, &config); err != nil {
		panic(err)
	}

	errs := make(chan error)
	var wg sync.WaitGroup
	for _, host := range hosts {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			addr := fmt.Sprintf("%s:%d", host, *port)
			if err := reload(addr, config.Scheduler); err != nil {
				errs <- err
			}
		}(host)
	}
	go func() {
		for err := range errs {
			fmt.Println(err)
		}
	}()
	wg.Wait()
	close(errs)
}
