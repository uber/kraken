**Table of Contents**

- [Push And Pull Docker Images](#push-and-pull-docker-images)
  - [Pushing Docker Images To Kraken Proxy](#pushing-docker-images-to-kraken-proxy)
  - [Pulling Docker Images From Kraken Agent](#pulling-docker-images-from-kraken-agent)
- [Upload and Download Generic Content Addressable Blobs](#upload-and-download-generic-content-addressable-blobs)
  - [Uploading Blobs To Kraken Origin](#uploading-blobs-to-kraken-origin)
  - [Downloading Blobs From Kraken Agent](#downloading-blobs-from-kraken-agent)

# Push And Pull Docker Images

Kraken proxy implements all [docker registry V2 endpoints](https://docs.docker.com/registry/spec/api/).
Kraken agent only implements the GET and HEAD endpoints of docker registry.

## Pushing Docker Images To Kraken Proxy

You can push images to proxy using normal docker push command:
```
docker push {proxy_host}:{proxy_port}/{repo}:{tag}
```
Proxy will then upload the blobs and tag to origin and build-index.

Yon can also pull from proxy directly without going through the p2p network:
```
docker pull {proxy_host}:{proxy_port}/{repo}:{tag}
```
Note: if you didn't configure Kraken with TLS, you might need to update your docker daemon config to whitelist kraken-proxy as an [insecure registry](https://docs.docker.com/registry/insecure/#deploy-a-plain-http-registry) first.

## Pulling Docker Images From Kraken Agent

To pull docker images from local kraken agent, run:
```
docker pull localhost:{agent_registry_port}/{repo}:{tag}
```
Note: kraken agent use different ports for docker registry endpoints and generic content addressable blobs. Please make sure you are using the port configured via `agent_registry_port`.

# Upload and Download Generic Content Addressable Blobs

Kraken's usecase is not limited to docker images.
It exposes a separate set of endpoints for uploading and downloading generic [content addressable](https://en.wikipedia.org/wiki/Content-addressable_storage) blobs that's identified by SHA256 hash of the content.
Kraken proxy and build-index are not used in this usecase.
Uploads will be handled by Kraken origin, and downloads still go through Kraken agent.

## Uploading Blobs To Kraken Origin

Blobs can be uploaded through the Kraken origin cluster and into your storage backend. All blobs
must be uploaded in chunks, using the following API:

```
POST /namespace/<namespace>/blobs/<digest>/uploads
```

Starts the upload. Returns an upload id in the "Location" response header, which is used for
uploading chunks of the blob.

```
PATCH /namespace/<namespace>/blobs/<digest>/uploads/<uid>
```

Uploads a chunk of the blob, using the previously returned upload id. Supply the chunk bytes in the
request body. The "Content-Range" header in the request must specify the range of the chunk
being uploaded, for example:

```
Content-Range: 128,256
```

This would upload the request body to bytes ``[128, 256)`` of the blob.

```
PUT /namespace/<namespace>/blobs/<digest>/uploads/<uid>?through=<through>
```

Commits the upload. If ``through`` is set to ``true``, the blob will be uploaded through the origin
cluster and into the storage backend configured for ``namespace``.

## Downloading Blobs From Kraken Agent

```
GET /namespace/<namespace>/blobs/<digest>
```

Once Kraken has been configured with your storage information and namespace, you can download your
blobs from any host with a Kraken agent running on it. The download endpoint takes your namespace
and the content digest of the blob you want to download, and blocks until agent has downloaded the
blob to its on-disk cache. Once the blob is downloaded locally, status 200 is returned and the
blob content is streamed over the response body.

Error codes:

- 404: Blob was not found in your storage backend.
- 5xx: Something went wrong. Check the response body for an error message, or reach out to the
  Kraken team.
