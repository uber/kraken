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
package backend

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/utils/bandwidth"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

// These tests live in package backend, not backend_test, because throttle and
// the bandwidth field of ThrottledClient are both unexported. That rules out
// mocks/lib/backend, which imports backend and would make an import cycle, so
// the fake below stands in for gomock.

const (
	// _throttleBitsPerSec is the limit both directions are configured with.
	// Paired with a token size of one bit the bucket holds 1000 tokens, so the
	// limiter can reserve a blob of up to 125 bytes and refuses anything larger.
	_throttleBitsPerSec = 1000

	// _reservableBlob needs 800 tokens. The bucket starts full, so reserving it
	// succeeds with no delay and the test does not depend on the clock.
	_reservableBlob = 100

	// _unreservableBlob needs 8000 tokens, which is more than the whole bucket.
	// ReserveN cannot ever satisfy it, so it fails immediately rather than
	// sleeping.
	_unreservableBlob = 1000
)

// fakeBackendClient counts the calls ThrottledClient forwards to the backend
// underneath it. Only the three methods throttling touches are implemented --
// the embedded nil Client panics on anything else, so the fake cannot quietly
// absorb a call it was never meant to answer.
type fakeBackendClient struct {
	Client

	size      int64
	statErr   error
	uploads   int
	downloads int
}

func (c *fakeBackendClient) Stat(namespace, name string) (*core.BlobInfo, error) {
	if c.statErr != nil {
		return nil, c.statErr
	}
	return core.NewBlobInfo(c.size), nil
}

func (c *fakeBackendClient) Upload(namespace, name string, src io.Reader) error {
	c.uploads++
	return nil
}

func (c *fakeBackendClient) Download(namespace, name string, dst io.Writer) error {
	c.downloads++
	return nil
}

// throttledClientFixture builds a ThrottledClient over a fake backend whose
// blobs are size bytes, and returns the stats scope the limiter reports to.
func throttledClientFixture(t *testing.T, size int64) (*fakeBackendClient, *ThrottledClient, tally.TestScope) {
	t.Helper()

	stats := tally.NewTestScope("", nil)
	l, err := bandwidth.NewLimiter(bandwidth.Config{
		EgressBitsPerSec:  _throttleBitsPerSec,
		IngressBitsPerSec: _throttleBitsPerSec,
		TokenSize:         1,
		Enable:            true,
	}, stats)
	require.NoError(t, err)

	c := &fakeBackendClient{size: size}
	return c, throttle(c, l), stats
}

// reserved reports whether the limiter completed a reservation in direction.
// The limiter records its sleep duration only after ReserveN succeeds, so the
// presence of a sample tells us throttling actually ran without asserting on
// any elapsed time.
func reserved(stats tally.TestScope, direction string) bool {
	key := "throttle_sleep_duration+direction=" + direction + ",module=bandwidth"
	_, ok := stats.Snapshot().Histograms()[key]
	return ok
}

// TestThrottledClientReservesBandwidth is the non-vacuity guard for the three
// tests below it. Those assert that a transfer survives a throttling failure,
// which stays true if throttling is removed altogether -- deleting the
// ReserveEgress call would leave all three green. This one fails instead.
func TestThrottledClientReservesBandwidth(t *testing.T) {
	require := require.New(t)

	c, tc, stats := throttledClientFixture(t, _reservableBlob)

	src := store.NewBufferFileReader(make([]byte, _reservableBlob))
	require.NoError(tc.Upload("namespace", "name", src))
	require.NoError(tc.Download("namespace", "name", io.Discard))

	require.True(reserved(stats, "egress"), "upload did not reserve egress bandwidth")
	require.True(reserved(stats, "ingress"), "download did not reserve ingress bandwidth")
	require.Equal(1, c.uploads)
	require.Equal(1, c.downloads)
}

// TestThrottledClientUploadProceedsWhenReservationFails pins the contract that
// throttling is best effort. A blob which needs more tokens than the bucket
// holds can never be reserved, and the limiter reports that as an error. The
// upload must still go through: propagating the error instead would reject
// every blob larger than one second of configured bandwidth, which on a real
// origin is most image layers.
func TestThrottledClientUploadProceedsWhenReservationFails(t *testing.T) {
	require := require.New(t)

	c, tc, stats := throttledClientFixture(t, _reservableBlob)

	src := store.NewBufferFileReader(make([]byte, _unreservableBlob))
	require.NoError(tc.Upload("namespace", "name", src))

	require.Equal(1, c.uploads)
	require.False(reserved(stats, "egress"), "oversized blob must not reserve egress bandwidth")
}

// TestThrottledClientUploadProceedsWhenSizeUnknown covers the branch added by
// #669. A src which cannot report its size gives the limiter nothing to
// reserve against, so the upload runs unthrottled. That is a real gap in
// origin's self-throttling, but it must stay a logged gap rather than a failed
// upload.
func TestThrottledClientUploadProceedsWhenSizeUnknown(t *testing.T) {
	require := require.New(t)

	c, tc, stats := throttledClientFixture(t, _reservableBlob)

	// bytes.Buffer has no Size method, unlike store.FileReader.
	src := bytes.NewBuffer(make([]byte, _reservableBlob))
	require.NoError(tc.Upload("namespace", "name", src))

	require.Equal(1, c.uploads)
	require.False(reserved(stats, "egress"), "a src of unknown size must not reserve egress bandwidth")
}

// TestThrottledClientDownloadProceedsWhenReservationFails is the ingress half
// of the same contract.
func TestThrottledClientDownloadProceedsWhenReservationFails(t *testing.T) {
	require := require.New(t)

	c, tc, stats := throttledClientFixture(t, _unreservableBlob)

	require.NoError(tc.Download("namespace", "name", io.Discard))

	require.Equal(1, c.downloads)
	require.False(reserved(stats, "ingress"), "oversized blob must not reserve ingress bandwidth")
}

// TestThrottledClientDownloadFailsWhenStatFails is the other half of the
// contract. Download has to ask the backend how large the blob is before it
// can reserve ingress for it, so unlike a reservation failure a Stat failure
// is fatal -- carrying on would download the blob with a size of zero and so
// with no throttling at all.
func TestThrottledClientDownloadFailsWhenStatFails(t *testing.T) {
	require := require.New(t)

	c, tc, stats := throttledClientFixture(t, _reservableBlob)
	c.statErr = errors.New("some stat error")

	require.Error(tc.Download("namespace", "name", io.Discard))

	require.Equal(0, c.downloads)
	require.False(reserved(stats, "ingress"))
}
