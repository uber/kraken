## 1. Build

`$ make devcluster`

This command creates a `kraken-agent` image containing an agent binary and a `kraken-herd` image containing build-index, origin, proxy, and tracker binaries.

It starts 2 agent docker containers and 1 herd container. Docker-for-Mac is required for devcluster to work, because the development config files use host.docker.internal for address of all components.

## 2. Pulling from Docker Hub Library

A simple registry storage backend is provided for read-only access to Docker registry. A library image can be pulled from agent.

`$ docker pull localhost:16000/library/golang:1.14`

Note, this backend is used for all `library/.*` repositories. `library` is the default namespace for Docker Hub's standard public repositories.

For all the other repositories, a testfs storage backend is included in the `kraken-herd` image, which is a simple http server that supports file uploading and downloading via port `14000`. Testfs simply stores blobs and tags on filesystem.

## 3. Pushing a Test Image

A test image can be pushed to the herd instance

`$ docker push localhost:15000/<repo>:<tag>`

## 4. Pull the Test Image

Pull the test image from agent:

`$ docker pull localhost:16000/<repo>:<tag>`
