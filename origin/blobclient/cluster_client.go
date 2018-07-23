package blobclient

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"

	"code.uber.internal/infra/kraken/core"
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
	addr     string
}

// NewClientResolver returns a new client resolver.
func NewClientResolver(p Provider, addr string) (ClientResolver, error) {
	if addr == "" {
		return nil, errors.New("addr is empty")
	}
	return &clientResolver{p, addr}, nil
}

func (r *clientResolver) Resolve(d core.Digest) ([]Client, error) {
	locs, err := r.provider.Provide(r.addr).Locations(d)
	if err != nil {
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

// ClusterClient defines a top-level origin cluster client which handles blob
// location resolution and retries.
type ClusterClient interface {
	UploadBlob(namespace string, d core.Digest, blob io.Reader) error
	DownloadBlob(namespace string, d core.Digest, dst io.Writer) error
	GetMetaInfo(namespace string, d core.Digest) (*core.MetaInfo, error)
	Stat(namespace string, d core.Digest) (*core.BlobInfo, error)
	OverwriteMetaInfo(d core.Digest, pieceLength int64) error
	Owners(d core.Digest) ([]core.PeerContext, error)
	ReplicateToRemote(namespace string, d core.Digest, remoteDNS string) error
}

type clusterClient struct {
	resolver            ClientResolver
	pollDownloadBackoff *backoff.Backoff
}

// NewClusterClient returns a new ClusterClient.
func NewClusterClient(r ClientResolver) ClusterClient {
	return &clusterClient{
		resolver:            r,
		pollDownloadBackoff: backoff.New(backoff.Config{}),
	}
}

// UploadBlob uploads blob to origin cluster. See Client.UploadBlob for more details.
func (c *clusterClient) UploadBlob(namespace string, d core.Digest, blob io.Reader) (err error) {
	clients, err := c.resolver.Resolve(d)
	if err != nil {
		return fmt.Errorf("resolve clients: %s", err)
	}

	// We prefer the origin with highest hashing score so the first origin will handle
	// replication to origins with lower score. This is because we want to reduce upload
	// conflicts between local replicas.
	for _, client := range clients {
		err = client.UploadBlob(namespace, d, blob)
		if httputil.IsNetworkError(err) {
			continue
		}
		break
	}
	return err
}

// GetMetaInfo returns the metainfo for d. Does not handle polling.
func (c *clusterClient) GetMetaInfo(namespace string, d core.Digest) (mi *core.MetaInfo, err error) {
	clients, err := c.resolver.Resolve(d)
	if err != nil {
		return nil, fmt.Errorf("resolve clients: %s", err)
	}
	for _, client := range clients {
		mi, err = client.GetMetaInfo(namespace, d)
		if err != nil {
			continue
		}
		break
	}
	return mi, err
}

// Stat checks availability of a blob in the cluster.
func (c *clusterClient) Stat(namespace string, d core.Digest) (bi *core.BlobInfo, err error) {
	clients, err := c.resolver.Resolve(d)
	if err != nil {
		return nil, fmt.Errorf("resolve clients: %s", err)
	}

	shuffle(clients)
	for _, client := range clients {
		bi, err = client.Stat(namespace, d)
		if err != nil {
			continue
		}
		break
	}

	return bi, err
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
	err := Poll(c.resolver, c.pollDownloadBackoff, d, func(client Client) error {
		return client.DownloadBlob(namespace, d, dst)
	})
	if httputil.IsNotFound(err) {
		err = ErrBlobNotFound
	}
	return err
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

// ReplicateToRemote replicates d to a remote origin cluster.
func (c *clusterClient) ReplicateToRemote(namespace string, d core.Digest, remoteDNS string) error {
	// Re-use download backoff since replicate may download blobs.
	return Poll(c.resolver, c.pollDownloadBackoff, d, func(client Client) error {
		return client.ReplicateToRemote(namespace, d, remoteDNS)
	})
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
				if serr, ok := err.(httputil.StatusError); ok {
					if serr.Status == http.StatusAccepted {
						continue POLL
					}
					if serr.Status < 500 {
						return err
					}
				}
				errs = append(errs, fmt.Errorf("origin %s: %s", client.Addr(), err))
				continue ORIGINS
			}
			return nil // Success!
		}
		errs = append(errs, fmt.Errorf("origin %s: 202 backoff: %s", client.Addr(), a.Err()))
	}
	return fmt.Errorf("all origins unavailable: %s", errutil.Join(errs))
}
