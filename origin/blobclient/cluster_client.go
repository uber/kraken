package blobclient

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/utils/backoff"
	"code.uber.internal/infra/kraken/utils/errutil"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/log"
)

// ClientResolver resolves digests into Clients of origins.
type ClientResolver interface {
	// Resolve must return an ordered, stable list of Clients for origins owning d.
	Resolve(d core.Digest) ([]Client, error)
}

type clientResolver struct {
	provider Provider
	servers  serverset.Set
}

// NewClientResolver returns a new client resolver.
func NewClientResolver(p Provider, servers serverset.Set) ClientResolver {
	return &clientResolver{p, servers}
}

func (r *clientResolver) Resolve(d core.Digest) ([]Client, error) {
	it := r.servers.Iter()
	for it.Next() {
		locs, err := r.provider.Provide(it.Addr()).Locations(d)
		if err != nil {
			if _, ok := err.(httputil.NetworkError); ok {
				log.Errorf("Error resolving locations from %s: %s", it.Addr(), err)
				continue
			}
			return nil, fmt.Errorf("get locations: %s", err)
		}
		if len(locs) == 0 {
			return nil, errors.New("no locations found")
		}
		var clients []Client
		for _, loc := range locs {
			clients = append(clients, r.provider.Provide(loc))
		}
		return clients, nil
	}
	return nil, it.Err()
}

// ClusterClient defines a top-level origin cluster client which handles blob
// location resolution and retries.
type ClusterClient interface {
	UploadBlob(namespace string, d core.Digest, blob io.Reader, through bool) error
	DownloadBlob(namespace string, d core.Digest, dst io.Writer) error
	GetMetaInfo(namespace string, d core.Digest) (*core.MetaInfo, error)
	OverwriteMetaInfo(d core.Digest, pieceLength int64) error
	Owners(d core.Digest) ([]core.PeerContext, error)
}

type clusterClient struct {
	resolver            ClientResolver
	pollMetaInfoBackoff *backoff.Backoff
	pollDownloadBackoff *backoff.Backoff
}

// NewClusterClient returns a new ClusterClient.
func NewClusterClient(r ClientResolver) ClusterClient {
	return &clusterClient{
		resolver:            r,
		pollMetaInfoBackoff: backoff.New(backoff.Config{}),
		pollDownloadBackoff: backoff.New(backoff.Config{}),
	}
}

// UploadBlob uploads blob to origin cluster. See Client.UploadBlob for more details.
func (c *clusterClient) UploadBlob(
	namespace string, d core.Digest, blob io.Reader, through bool) (err error) {

	clients, err := c.resolver.Resolve(d)
	if err != nil {
		return fmt.Errorf("resolve clients: %s", err)
	}
	// Shuffle clients to balance load.
	shuffle(clients)
	for _, client := range clients {
		err = client.UploadBlob(namespace, d, blob, through)
		if _, ok := err.(httputil.NetworkError); ok {
			continue
		}
		break
	}
	return err
}

// GetMetaInfo returns the metainfo for d.
func (c *clusterClient) GetMetaInfo(namespace string, d core.Digest) (mi *core.MetaInfo, err error) {
	err = Poll(c.resolver, c.pollMetaInfoBackoff, d, func(client Client) error {
		mi, err = client.GetMetaInfo(namespace, d)
		return err
	})
	return mi, err
}

// OverwriteMetaInfo overwrites existing metainfo for d with new metainfo configured
// with pieceLength on every origin server. Returns error if any origin was unable
// to overwrite metainfo. Primarly intended for benchmarking purposes.
func (c *clusterClient) OverwriteMetaInfo(d core.Digest, pieceLength int64) error {
	clients, err := c.resolver.Resolve(d)
	if err != nil {
		return fmt.Errorf("resolve clients: %s", err)
	}
	var errs []error
	for _, client := range clients {
		if err := client.OverwriteMetaInfo(d, pieceLength); err != nil {
			errs = append(errs, fmt.Errorf("origin %s: %s", client.Addr(), err))
		}
	}
	return errutil.Join(errs)
}

// DownloadBlob pulls a blob from the origin cluster.
func (c *clusterClient) DownloadBlob(namespace string, d core.Digest, dst io.Writer) error {
	return Poll(c.resolver, c.pollDownloadBackoff, d, func(client Client) error {
		return client.DownloadBlob(namespace, d, dst)
	})
}

// Owners returns the origin peers which own d.
func (c *clusterClient) Owners(d core.Digest) ([]core.PeerContext, error) {
	clients, err := c.resolver.Resolve(d)
	if err != nil {
		return nil, fmt.Errorf("resolve clients: %s", err)
	}

	var mu sync.Mutex
	var peers []core.PeerContext
	var errs []error

	var wg sync.WaitGroup
	for _, client := range clients {
		wg.Add(1)
		go func(client Client) {
			defer wg.Done()
			pctx, err := client.GetPeerContext()
			mu.Lock()
			if err != nil {
				errs = append(errs, err)
			} else {
				peers = append(peers, pctx)
			}
			mu.Unlock()
		}(client)
	}
	wg.Wait()

	err = errutil.Join(errs)

	if len(peers) == 0 {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("no origin peers found")
	}

	if err != nil {
		log.With("blob", d.Hex()).Errorf("Error getting all origin peers: %s", err)
	}
	return peers, nil
}

func shuffle(cs []Client) {
	for i := range cs {
		j := rand.Intn(i + 1)
		cs[i], cs[j] = cs[j], cs[i]
	}
}

// Poll wraps requests for endpoints which require polling, due to a blob
// being asynchronously fetched from remote storage in the origin cluster.
func Poll(
	r ClientResolver, b *backoff.Backoff, d core.Digest, makeRequest func(Client) error) error {

	// By looping over clients in order, we will always prefer the same origin
	// for making requests to loosely guarantee that only one origin needs to
	// fetch the file from remote backend.
	clients, err := r.Resolve(d)
	if err != nil {
		return fmt.Errorf("resolve clients: %s", err)
	}
	var errs []error
ORIGINS:
	for _, client := range clients {
		a := b.Attempts()
	POLL:
		for a.WaitForNext() {
			if err := makeRequest(client); err != nil {
				if httputil.IsNetworkError(err) {
					errs = append(errs, fmt.Errorf("origin %s: %s", client.Addr(), err))
					continue ORIGINS
				}
				if httputil.IsAccepted(err) {
					continue POLL
				}
				return err
			}
			return nil // Success!
		}
		errs = append(errs, fmt.Errorf("origin %s: %s", client.Addr(), err))
	}
	return fmt.Errorf("all origins unavailable: %s", errutil.Join(errs))
}
