package main

import (
	"flag"
	"os"

	"code.uber.internal/infra/kraken/origin/blobserver"
	"code.uber.internal/infra/kraken/utils/configutil"
)

func main() {
	configFile := flag.String("config", "", "Configuration file that has to be loaded from one of UBER_CONFIG_DIR locations")
	flag.Parse()

	var config blobserver.Config
	if err := configutil.Load(*configFile, &config); err != nil {
		panic(err)
	}

	RunMain(os.Args, config, os.Stdout)
}
