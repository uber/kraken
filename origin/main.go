package main

import (

	// Import all backend client packages to register them with backend manager.
	_ "github.com/uber/kraken/lib/backend/hdfsbackend"
	_ "github.com/uber/kraken/lib/backend/httpbackend"
	_ "github.com/uber/kraken/lib/backend/registrybackend"
	_ "github.com/uber/kraken/lib/backend/s3backend"
	_ "github.com/uber/kraken/lib/backend/testfs"
	"github.com/uber/kraken/origin/cmd"
)

func main() {
	cmd.Execute()
}
