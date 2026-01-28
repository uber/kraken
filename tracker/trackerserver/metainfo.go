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
package trackerserver

import (
	"fmt"
	"net/http"

	"github.com/uber/kraken/utils/handler"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func (s *Server) getMetaInfoHandler(w http.ResponseWriter, r *http.Request) error {
	ctx, span := otel.Tracer("kraken-tracker").Start(r.Context(), "tracker.get_metainfo",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("component", "tracker"),
			attribute.String("operation", "get_metainfo"),
		),
	)
	defer span.End()

	namespace, err := httputil.ParseParam(r, "namespace")
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "parse namespace failed")
		return err
	}
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "parse digest failed")
		return handler.Errorf("parse digest: %s", err).Status(http.StatusBadRequest)
	}

	span.SetAttributes(
		attribute.String("namespace", namespace),
		attribute.String("blob.digest", d.Hex()),
	)
	log.WithTraceContext(ctx).With("namespace", namespace, "digest", d.Hex()).Debug("Getting metainfo from origin")

	timer := s.stats.Timer("get_metainfo").Start()
	mi, err := s.originCluster.GetMetaInfo(ctx, namespace, d)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "get metainfo from origin failed")
		if serr, ok := err.(httputil.StatusError); ok {
			// Propagate errors received from origin.
			return handler.Errorf("origin: %s", serr.ResponseDump).Status(serr.Status)
		}
		return err
	}
	timer.Stop()

	b, err := mi.Serialize()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "serialize metainfo failed")
		return fmt.Errorf("serialize metainfo: %s", err)
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(b); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "write response failed")
		return fmt.Errorf("write response: %s", err)
	}
	span.SetStatus(codes.Ok, "metainfo retrieved")
	return nil
}
