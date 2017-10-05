package main

import (
	"os"

	"code.uber.internal/infra/kraken/config/origin"
)

func main() {
	cfg := config.Initialize()
	RunMain(os.Args, cfg, os.Stdout)
}
