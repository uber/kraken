package backend

import (
	"io"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/utils/bandwidth"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/stringset"
)

type throttledClient struct {
	Client
	bandwidth *bandwidth.Limiter
}

// throttle wraps client with bandwidth limits.
func throttle(client Client, bandwidth *bandwidth.Limiter) *throttledClient {
	return &throttledClient{client, bandwidth}
}

type sizer interface {
	Size() int64
}

// Ensure that we can get size from file store readers.
var _ sizer = (store.FileReader)(nil)

func (c *throttledClient) Upload(name string, src io.Reader) error {
	if s, ok := src.(sizer); ok {
		// Only throttle if the src implements a Size method.
		if err := c.bandwidth.ReserveEgress(s.Size()); err != nil {
			log.With("name", name).Errorf("Error reserving egress: %s", err)
			// Ignore error.
		}
	}
	return c.Client.Upload(name, src)
}

func (c *throttledClient) Download(name string, dst io.Writer) error {
	info, err := c.Client.Stat(name)
	if err != nil {
		return err
	}
	if err := c.bandwidth.ReserveIngress(info.Size); err != nil {
		log.With("name", name).Errorf("Error reserving ingress: %s", err)
		// Ignore error.
	}
	return c.Client.Download(name, dst)
}

func (c *throttledClient) adjustBandwidth(denominator int) error {
	return c.bandwidth.Adjust(denominator)
}

func (c *throttledClient) egressLimit() int64 {
	return c.bandwidth.EgressLimit()
}

func (c *throttledClient) ingressLimit() int64 {
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
