package blobserver

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/hrw"
	"code.uber.internal/infra/kraken/origin/client"
	hashcfg "code.uber.internal/infra/kraken/origin/config"
)

const (
	uploadChunkSize int64 = 16 * 1024 * 1024 // 16MB
)

func downloadBlobHandler(ctx context.Context, writer http.ResponseWriter) (context.Context, *ServerResponse) {
	digest, ok := ctx.Value(ctxKeyDigest).(*image.Digest)
	if !ok {
		return nil, NewServerResponseWithError(http.StatusInternalServerError, "Digest not set")
	}
	localStore, ok := ctx.Value(ctxKeyLocalStore).(*store.LocalStore)
	if !ok {
		return nil, NewServerResponseWithError(http.StatusInternalServerError, "LocalStore not set")
	}

	// Get reader.
	blobReader, err := localStore.GetCacheFileReader(digest.Hex())
	if os.IsNotExist(err) {
		return nil, NewServerResponseWithError(http.StatusNotFound, "Cannot find blob data for digest: %s, error: %s", digest, err)
	} else if err != nil {
		return nil, NewServerResponseWithError(http.StatusInternalServerError, "Cannot read blob data for digest: %s, error: %s", digest, err)
	}
	defer blobReader.Close()

	// Read data.
	for {
		_, err := io.CopyN(writer, blobReader, uploadChunkSize)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, NewServerResponseWithError(http.StatusInternalServerError, "Cannot read digest: %s, error: %s", digest, err)
		}
	}

	return ctx, nil
}

func okHandler(ctx context.Context, writer http.ResponseWriter) (context.Context, *ServerResponse) {
	resp := NewServerResponse(http.StatusOK)

	return ctx, resp
}

func okOctetStreamHandler(ctx context.Context, writer http.ResponseWriter) (context.Context, *ServerResponse) {
	resp := NewServerResponse(http.StatusOK)
	writer.Header().Set("Content-Type", "application/octet-stream-v1")

	return ctx, resp
}

func acceptedHandler(ctx context.Context, writer http.ResponseWriter) (context.Context, *ServerResponse) {
	resp := NewServerResponse(http.StatusAccepted)
	writer.Header().Set("Content-Length", "0")

	return ctx, resp
}

func createdHandler(ctx context.Context, writer http.ResponseWriter) (context.Context, *ServerResponse) {
	resp := NewServerResponse(http.StatusCreated)
	writer.Header().Set("Content-Length", "0")

	return ctx, resp
}

func returnUploadLocationHandler(ctx context.Context, writer http.ResponseWriter) (context.Context, *ServerResponse) {
	uploadUUID, ok := ctx.Value(ctxKeyUploadUUID).(string)
	if !ok {
		return nil, NewServerResponseWithError(http.StatusInternalServerError, "Digest not set")
	}

	resp := NewServerResponse(http.StatusAccepted)
	writer.Header().Set("Location", fmt.Sprintf("/blobs/uploads/%s", uploadUUID))
	writer.Header().Set("Content-Length", "0")

	return ctx, resp
}

func repairBlobStreamHandler(ctx context.Context, writer http.ResponseWriter) (context.Context, *ServerResponse) {
	shardID, okSh := ctx.Value(ctxKeyShardID).(string)
	digest, okDi := ctx.Value(ctxKeyDigest).(*image.Digest)

	if !okDi && !okSh {
		return nil, NewServerResponseWithError(http.StatusInternalServerError,
			"neither shard id nor digest set")
	}
	hashConfig, ok := ctx.Value(ctxKeyHashConfig).(hashcfg.HashConfig)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError, "label is not set")
	}

	hashState, ok := ctx.Value(ctxKeyHashState).(*hrw.RendezvousHash)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError, "hashState is not set")
	}

	localStore, ok := ctx.Value(ctxKeyLocalStore).(*store.LocalStore)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError, "LocalStore is not set")
	}
	blobTransferFactory, ok := ctx.Value(ctxBlobTransferFactory).(client.BlobTransferFactory)
	if !ok {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError, "blobTransferFactory is not set")
	}
	// TODO(igor): Need to read num_replicas from tracker's metadata
	nodes, err := hashState.GetOrderedNodes(shardID, hashConfig.NumReplica)
	if err != nil || len(nodes) == 0 {
		return nil, NewServerResponseWithError(
			http.StatusInternalServerError,
			"failed to compute hash for shard %s, error: %s", shardID, err)
	}

	var digests []*image.Digest
	if okSh { //shard id
		if digests, err = localStore.ListDigests(shardID); err != nil {
			return nil, NewServerResponseWithError(
				http.StatusInternalServerError,
				"failed to list digests for shard %s, error: %s", shardID, err)
		}

	} else { //digest item
		digests = append(digests, digest)
	}

	writer.Header().Set("Content-Type", "application/json")
	for _, node := range nodes {

		// skip repair for the current node
		if node.Label == hashConfig.Label {
			continue
		}

		host, ok := hashConfig.LabelToHostname[node.Label]

		if !ok {
			return nil, NewServerResponseWithError(
				http.StatusInternalServerError,
				"cannot find a server by its label: invalid hash configuration, server label: %s",
				node.Label,
			)
		}

		br := &BlobRepairer{
			context:     ctx,
			hostname:    host,
			blobAPI:     blobTransferFactory(host, localStore),
			numWorkers:  hashConfig.Repair.NumWorkers,
			numRetries:  hashConfig.Repair.NumRetries,
			retryDelay:  hashConfig.Repair.RetryDelayMs,
			connTimeout: hashConfig.Repair.ConnTimeout,
		}

		// Batch repairer launches a number of background go-routines
		// and reports the result back into response writer asynchronously
		br.BatchRepair(digests, writer)

	}
	resp := NewServerResponse(http.StatusOK)
	return ctx, resp
}
