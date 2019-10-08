## 1. Run

`$ helm install --name=kraken-demo ./helm`

This command starts 3 trackers, origins and build-index pods, 1 proxy pod and an agent daemonset.

Once deployed, every node will have a docker registry API exposed on port 30081. The port number is
configurable.

## 2. Pulling from Docker Hub Library

A simple registry storage backend is provided for read-only access to Docker registry. A library 
image can be pulled from Kraken agent by specifying `127.0.0.1:30081` in the image name in pod
spec. For example spec, see [example](demo.json).

Note, this backend is used only for all `library/.*` repositories. `library` is the default
namespace for Docker Hub's standard public repositories. To use your own registry as the backend,
please update origin and build-index configs accordingly.
