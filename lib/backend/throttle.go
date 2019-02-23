// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package backend

import (
	"io"

	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/utils/bandwidth"
	"github.com/uber/kraken/utils/log"
	"github.com/uber/kraken/utils/stringset"
)

// ThrottledClient is a backend client with speed limit.
type ThrottledClient struct {
	Client
	bandwidth *bandwidth.Limiter
}

// throttle wraps client with bandwidth limits.
func throttle(client Client, bandwidth *bandwidth.Limiter) *ThrottledClient {
	return &ThrottledClient{client, bandwidth}
}

type sizer interface {
	Size() int64
}

// Ensure that we can get size from file store readers.
var _ sizer = (store.FileReader)(nil)

// Upload uploads src into name.
func (c *ThrottledClient) Upload(namespace, name string, src io.Reader) error {
	if s, ok := src.(sizer); ok {
		// Only throttle if the src implements a Size method.
		if err := c.bandwidth.ReserveEgress(s.Size()); err != nil {
			log.With("name", name).Errorf("Error reserving egress: %s", err)
			// Ignore error.
		}
	}
	return c.Client.Upload(namespace, name, src)
}

// Download downloads name into dst.
func (c *ThrottledClient) Download(namespace, name string, dst io.Writer) error {
	info, err := c.Client.Stat(namespace, name)
	if err != nil {
		return err
	}
	if err := c.bandwidth.ReserveIngress(info.Size); err != nil {
		log.With("name", name).Errorf("Error reserving ingress: %s", err)
		// Ignore error.
	}
	return c.Client.Download(namespace, name, dst)
}

func (c *ThrottledClient) adjustBandwidth(denominator int) error {
	return c.bandwidth.Adjust(denominator)
}

// EgressLimit returns egress limit.
func (c *ThrottledClient) EgressLimit() int64 {
	return c.bandwidth.EgressLimit()
}

// IngressLimit returns ingress limit.
func (c *ThrottledClient) IngressLimit() int64 {
	return c.bandwidth.IngressLimit()
}

// BandwidthWatcher is a hashring.Watcher which adjusts bandwidth on throttled
// backends when hashring membership changes.
type BandwidthWatcher struct {
	manager *Manager
}

// NewBandwidthWatcher creates a new BandwidthWatcher for manager.
func NewBandwidthWatcher(manager *Manager) *BandwidthWatcher {
	return &BandwidthWatcher{manager}
}

// Notify splits bandwidth across the size of latest.
func (w *BandwidthWatcher) Notify(latest stringset.Set) {
	if err := w.manager.AdjustBandwidth(len(latest)); err != nil {
		log.With("latest", latest.ToSlice()).Errorf("Error adjusting bandwidth: %s", err)
	}
}
