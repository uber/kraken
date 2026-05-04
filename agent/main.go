// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/uber/kraken/agent/cmd"
	"github.com/uber/kraken/lib/dockerregistry"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := cmd.Run(ctx, cmd.ParseFlags(), cmd.WithEffect(func() {
		dockerregistry.RegisterKrakenStorageDriver()
	})); err != nil {
		log.Fatal(err)
	}
	if err := ctx.Err(); err != nil {
		log.Fatal(err)
	}
}
