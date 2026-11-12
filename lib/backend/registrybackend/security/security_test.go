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
package security

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/docker/distribution/registry/client/auth/challenge"
	"github.com/stretchr/testify/require"
)

type closeTrackingBody struct {
	io.Reader
	closed bool
}

func (b *closeTrackingBody) Close() error {
	b.closed = true
	return nil
}

type stubRoundTripper struct {
	status  int
	version string
	bodies  []*closeTrackingBody
}

func (rt *stubRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	body := &closeTrackingBody{Reader: strings.NewReader("{}")}
	rt.bodies = append(rt.bodies, body)
	header := http.Header{}
	if rt.version != "" {
		header.Set(registryVersionHeader, rt.version)
	}
	return &http.Response{
		StatusCode: rt.status,
		Header:     header,
		Body:       body,
		Request:    req,
	}, nil
}

func TestUpdateChallengeClosesResponseBody(t *testing.T) {
	tests := []struct {
		desc    string
		status  int
		version string
		wantErr bool
	}{
		{"ok v2", http.StatusOK, "registry/2.0", false},
		{"unauthorized v2", http.StatusUnauthorized, "registry/2.0", false},
		{"not v2", http.StatusOK, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			require := require.New(t)

			rt := &stubRoundTripper{status: tt.status, version: tt.version}
			a := &authenticator{
				address:          "localhost:5000",
				roundTripper:     rt,
				challengeManager: challenge.NewSimpleManager(),
			}

			for i := 0; i < 3; i++ {
				err := a.updateChallenge()
				if tt.wantErr {
					require.Error(err)
				} else {
					require.NoError(err)
				}
			}

			require.Len(rt.bodies, 3)
			for _, b := range rt.bodies {
				require.True(b.closed, "response body not closed")
			}
		})
	}
}
