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
package dockerregistry

import (
	"context"
	"fmt"
	"os"

	"github.com/distribution/distribution/v3/configuration"
	"github.com/distribution/distribution/v3/registry"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/lib/dockerregistry/transfer"
	"github.com/uber/kraken/lib/store"
	"go.opentelemetry.io/otel"
)

const (
	_rw = "rw"
	_ro = "ro"

	// Read by the OpenTelemetry auto-exporter that registry.NewRegistry
	// installs.
	_otelTracesExporterEnv = "OTEL_TRACES_EXPORTER"
)

// Config defines registry configuration.
type Config struct {
	// Docker is the Docker Distribution registry configuration.
	Docker configuration.Configuration `yaml:"docker"`

	// EnforceSignatureVerification rejects manifest downloads when signature
	// verification returns deny or an error. Allow and skip still succeed.
	//
	// DefaultVerificationFunc always skips, so this has no effect unless a
	// custom verification function is registered via
	// RegisterKrakenStorageDriverWithImageVerification.
	EnforceSignatureVerification bool `yaml:"enforce_signature_verification"`
}

// ReadWriteParameters builds parameters for a read-write driver.
func (c Config) ReadWriteParameters(
	transferer transfer.ImageTransferer,
	cas *store.CAStore,
	metrics tally.Scope) configuration.Parameters {

	return configuration.Parameters{
		"constructor": _rw,
		"config":      c,
		"transferer":  transferer,
		"castore":     cas,
		"metrics":     metrics,
	}
}

// ReadOnlyParameters builds parameters for a read-only driver.
func (c Config) ReadOnlyParameters(
	transferer transfer.ImageTransferer,
	bs BlobStore,
	metrics tally.Scope) configuration.Parameters {

	return configuration.Parameters{
		"constructor": _ro,
		"config":      c,
		"transferer":  transferer,
		"blobstore":   bs,
		"metrics":     metrics,
	}
}

// Build builds a new docker registry.
func (c Config) Build(parameters configuration.Parameters) (*registry.Registry, error) {
	c.Docker.Storage = configuration.Storage{
		Name: parameters,
		// Redirect is enabled by default in docker registry.
		// We implement redirect on proxy level so we do not need this in storage driver for now.
		"redirect": configuration.Parameters{
			"disable": true,
		},
	}
	// registry.NewRegistry initializes OpenTelemetry for the whole process: it
	// replaces the global tracer provider, propagator and error handler, and
	// its span exporter defaults to OTLP on localhost. Disable the exporter
	// unless the operator opted in, and restore the globals afterwards so
	// kraken's own tracing setup is left untouched.
	if os.Getenv(_otelTracesExporterEnv) == "" {
		if err := os.Setenv(_otelTracesExporterEnv, "none"); err != nil {
			return nil, fmt.Errorf("set %s: %s", _otelTracesExporterEnv, err)
		}
	}
	tp := otel.GetTracerProvider()
	prop := otel.GetTextMapPropagator()
	eh := otel.GetErrorHandler()
	r, err := registry.NewRegistry(context.Background(), &c.Docker)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(prop)
	otel.SetErrorHandler(eh)
	return r, err
}
