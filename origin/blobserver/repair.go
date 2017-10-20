package blobserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/hrw"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/netutil"
	"code.uber.internal/infra/kraken/utils/stringset"
)

type count32 int32

func (c *count32) increment() int32 {
	return atomic.AddInt32((*int32)(c), 1)
}

const (
	replicateShardOp  = "replicate_shard"
	replicateDigestOp = "replicate_digest"
	deleteDigestOp    = "delete_digest"
)

type repairMessage struct {
	Operation string `json:"operation"`
	Object    string `json:"object"`
	Success   bool   `json:"success"`
	Error     string `json:"error"`
}

func replicateShardErrorf(shardID string, format string, args ...interface{}) repairMessage {
	return repairMessage{
		Operation: replicateShardOp,
		Object:    shardID,
		Success:   false,
		Error:     fmt.Sprintf(format, args...),
	}
}

// Note: there is no replicateShardSuccess. Clients can infer shard failure from digest failure.

func replicateDigestErrorf(d image.Digest, format string, args ...interface{}) repairMessage {
	return repairMessage{
		Operation: replicateDigestOp,
		Object:    d.String(),
		Success:   false,
		Error:     fmt.Sprintf(format, args...),
	}
}

func replicateDigestSuccess(d image.Digest) repairMessage {
	return repairMessage{
		Operation: replicateDigestOp,
		Object:    d.String(),
		Success:   true,
	}
}

func deleteDigestErrorf(d image.Digest, format string, args ...interface{}) repairMessage {
	return repairMessage{
		Operation: deleteDigestOp,
		Object:    d.String(),
		Success:   false,
		Error:     fmt.Sprintf(format, args...),
	}
}

func deleteDigestSuccess(d image.Digest) repairMessage {
	return repairMessage{
		Operation: deleteDigestOp,
		Object:    d.String(),
		Success:   true,
	}
}

// repairer initializes repair context with number of concurrent workers,
// number of retries on error, default connection timeout and target host
// that should handle all blob transfers.
type repairer struct {
	config          Config
	hostname        string
	labelToHostname map[string]string
	hashState       *hrw.RendezvousHash
	fileStore       store.FileStore
	clientProvider  blobclient.Provider

	ctx      context.Context
	messages chan repairMessage
}

func newRepairer(
	ctx context.Context,
	config Config,
	hostname string,
	labelToHostname map[string]string,
	hashState *hrw.RendezvousHash,
	fileStore store.FileStore,
	clientProvider blobclient.Provider) *repairer {

	return &repairer{
		config:          config,
		hostname:        hostname,
		labelToHostname: labelToHostname,
		hashState:       hashState,
		fileStore:       fileStore,
		clientProvider:  clientProvider,
		ctx:             ctx,
		messages:        make(chan repairMessage),
	}
}

func (r *repairer) WriteMessages(w http.ResponseWriter) {
	// FIXME(codyg): Repair should stream messages to the client as they occur.
	// Our currently implementation buffers messages and sends them once the
	// repairer is closed because the first call to w.Write sends a response.
	buf := new(bytes.Buffer)
	for msg := range r.messages {
		if err := json.NewEncoder(buf).Encode(msg); err != nil {
			log.WithFields(log.Fields{
				"message": msg,
			}).Errorf("Failed to encode digest repair message: %s", err)
			continue
		}
	}
	if _, err := io.Copy(w, buf); err != nil {
		log.Errorf("Failed to write message buffer: %s", err)
	}
}

func (r *repairer) Close() {
	close(r.messages)
}

func (r *repairer) RepairShard(shardID string) error {
	hosts, purge, err := r.destination(shardID)
	if err != nil {
		return err
	}
	r.replicateShard(shardID, hosts, purge)
	return nil
}

func (r *repairer) RepairDigest(d image.Digest) error {
	hosts, purge, err := r.destination(d.ShardID())
	if err != nil {
		return err
	}
	r.replicateDigests([]image.Digest{d}, hosts, purge)
	return nil
}

// destination returns the destination hosts for the shardID and whether the
// shard should be purged after replicated.
func (r *repairer) destination(shardID string) (hosts stringset.Set, purge bool, err error) {
	nodes, err := r.hashState.GetOrderedNodes(shardID, r.config.NumReplica)
	if err != nil {
		return nil, false, fmt.Errorf("failed to compute nodes of shard %q: %s", shardID, err)
	}
	hosts = make(stringset.Set)
	for _, node := range nodes {
		host, ok := r.labelToHostname[node.Label]
		if !ok {
			return nil, false, fmt.Errorf("cannot find server for label %q", node.Label)
		}
		hosts.Add(host)
	}
	if hosts.Has(r.hostname) {
		hosts.Remove(r.hostname)
		purge = false
	} else {
		purge = true
	}
	return hosts, purge, nil
}

func (r *repairer) replicateShard(shardID string, hosts stringset.Set, purge bool) {
	names, err := r.fileStore.ListCacheFilesByShardID(shardID)
	if err != nil {
		r.messages <- replicateShardErrorf(shardID, "failed to retrieve digests: %s", err)
		return
	}

	var digests []image.Digest
	for _, name := range names {
		d, err := image.NewDigestFromString("sha256:" + name)
		if err != nil {
			r.messages <- replicateShardErrorf(shardID, "failed to parse digest %q: %s", name, err)
			continue
		}
		digests = append(digests, d)
	}

	r.replicateDigests(digests, hosts, purge)
}

func (r *repairer) replicateDigests(digests []image.Digest, hosts stringset.Set, purge bool) {
	var wg sync.WaitGroup
	var cursor count32 = -1

	for i := 0; i < r.config.Repair.NumWorkers && i < len(digests); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-r.ctx.Done():
					return
				default:
					next := int(cursor.increment())
					if next >= len(digests) {
						return
					}
					d := digests[next]

					replicated := r.replicateDigest(d, hosts)

					if purge {
						if replicated {
							r.deleteDigest(d)
						} else {
							r.messages <- deleteDigestErrorf(d, "cannot delete digest: replication failed")
						}
					}
				}
			}
		}()
	}
	wg.Wait()
}

func (r *repairer) replicateDigest(d image.Digest, hosts stringset.Set) (replicated bool) {
	f, err := r.fileStore.GetCacheFileReader(d.Hex())
	if err != nil {
		r.messages <- replicateDigestErrorf(
			d, "failed to replicate to hosts %v: cannot get blob reader: %s", hosts, err)
		return false
	}
	info, err := r.fileStore.GetCacheFileStat(d.Hex())
	if err != nil {
		r.messages <- replicateDigestErrorf(
			d, "failed to replicate to hosts %v: cannot get blob stat: %s", hosts, err)
		return false
	}
	size := info.Size()

	replicated = true

	for h := range hosts {
		client := r.clientProvider.Provide(h)
		err = netutil.WithRetry(
			r.config.Repair.MaxRetries, r.config.Repair.RetryDelay, r.config.Repair.MaxRetryDelay,
			func() error {

				err := client.PushBlob(d, f, size)
				// Reset f for next push.
				f.Seek(0, io.SeekStart)

				if err == blobclient.ErrBlobExist {
					// Host already has blob -- move along.
					return nil
				}
				return err
			})
		if err != nil {
			replicated = false
			r.messages <- replicateDigestErrorf(d, "failed to replicate to host %q: %s", h, err)
		}
		r.messages <- replicateDigestSuccess(d)
	}

	return replicated
}

func (r *repairer) deleteDigest(d image.Digest) {
	if err := r.fileStore.MoveCacheFileToTrash(d.Hex()); err != nil {
		r.messages <- deleteDigestErrorf(d, "failed to move blob to trash: %s", err)
		return
	}
	r.messages <- deleteDigestSuccess(d)
}
