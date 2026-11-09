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
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace/noop"
)

// The driver factory can only be registered once per process.
var registerDriverOnce sync.Once

func TestBuildPreservesOTelGlobals(t *testing.T) {
	require := require.New(t)

	d, cleanup := newTestDriver()
	defer cleanup()
	registerDriverOnce.Do(RegisterKrakenStorageDriver)

	t.Setenv(_otelTracesExporterEnv, "")

	tp := noop.NewTracerProvider()
	prop := otel.GetTextMapPropagator()
	eh := otel.GetErrorHandler()
	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prevTP)

	config := Config{}
	r, err := config.Build(config.ReadWriteParameters(d.transferer, d.cas, tally.NoopScope))
	require.NoError(err)
	require.NotNil(r)

	require.Equal(tp, otel.GetTracerProvider())
	require.Equal(prop, otel.GetTextMapPropagator())
	require.Equal(eh, otel.GetErrorHandler())
	require.Equal("none", os.Getenv(_otelTracesExporterEnv))
}
