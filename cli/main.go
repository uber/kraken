package main

import (
	"os"

	xconfig "code.uber.internal/go-common.git/x/config"

	"code.uber.internal/infra/kraken/origin/blobserver"
)

func main() {
	var config blobserver.Config
	if err := xconfig.Load(&config); err != nil {
		panic(err)
	}
	RunMain(os.Args, config, os.Stdout)
}
