## 1. Prerequisites

Install these tools before you start.

- `kubectl`. See the [kubectl install guide](https://kubernetes.io/docs/tasks/tools/#kubectl).
- Helm 3.x. See the [Helm install guide](https://helm.sh/docs/intro/install/).
- A local Kubernetes cluster. Use `kind` or `minikube`. See step 2.

## 2. Start a local cluster

Pick one tool. Both work with the steps below.

### Option A: kind

Install `kind` from the [kind quick start guide](https://kind.sigs.k8s.io/docs/user/quick-start/#installation).

Create a cluster:

```
$ kind create cluster
```

Expected output:

```
Creating cluster "kind" ...
 ✓ Ensuring node image (kindest/node:v1.31.0) 🖼
 ✓ Preparing nodes 📦
 ✓ Writing configuration 📜
 ✓ Starting control-plane 🕹️
 ✓ Installing CNI 🔌
 ✓ Installing StorageClass 💾
Set kubectl context to "kind-kind"
```

### Option B: minikube

Install `minikube` from the [minikube start guide](https://minikube.sigs.k8s.io/docs/start/).

Create a cluster:

```
$ minikube start --driver=docker
```

Expected output ends with a line like this:

```
Done! kubectl is now configured to use "minikube" cluster and "default" namespace by default
```

## 3. Get the Kraken images

Kraken publishes component images to `ghcr.io/uber`. The Helm chart uses this registry by
default. If `ghcr.io/uber` does not allow anonymous pulls yet, build the images yourself with
the steps below.

### Option A: use the published images

The chart's default values already point at `ghcr.io/uber`. You do not need extra steps. Go to
step 4.

### Option B: build the images yourself

Run this from the root of the repository:

```
$ make images
```

This command builds all 7 component images. It tags each image as `kraken-<component>:dev` on
your machine.

The Helm chart needs each image name in the form `<repository>/kraken-<component>:<tag>`. Add a
local repository prefix to each image:

```
$ docker tag kraken-agent:dev local/kraken-agent:dev
$ docker tag kraken-build-index:dev local/kraken-build-index:dev
$ docker tag kraken-origin:dev local/kraken-origin:dev
$ docker tag kraken-proxy:dev local/kraken-proxy:dev
$ docker tag kraken-testfs:dev local/kraken-testfs:dev
$ docker tag kraken-tracker:dev local/kraken-tracker:dev
$ docker tag kraken-herd:dev local/kraken-herd:dev
```

Load the tagged images into your cluster.

For `kind`:

```
$ kind load docker-image local/kraken-agent:dev
$ kind load docker-image local/kraken-build-index:dev
$ kind load docker-image local/kraken-origin:dev
$ kind load docker-image local/kraken-proxy:dev
$ kind load docker-image local/kraken-testfs:dev
$ kind load docker-image local/kraken-tracker:dev
$ kind load docker-image local/kraken-herd:dev
```

For `minikube`:

```
$ minikube image load local/kraken-agent:dev
$ minikube image load local/kraken-build-index:dev
$ minikube image load local/kraken-origin:dev
$ minikube image load local/kraken-proxy:dev
$ minikube image load local/kraken-testfs:dev
$ minikube image load local/kraken-tracker:dev
$ minikube image load local/kraken-herd:dev
```

## 4. Install Kraken with Helm

### If you use the published images (step 3, Option A)

```
$ helm install kraken-demo ./helm
```

### If you built the images yourself (step 3, Option B)

```
$ helm install kraken-demo ./helm \
    --set kraken.repository=local \
    --set kraken.tag=dev \
    --set kraken.imagePullPolicy=Never
```

Expected output:

```
NAME: kraken-demo
LAST DEPLOYED: Mon Jan  1 00:00:00 2026
NAMESPACE: default
STATUS: deployed
REVISION: 1
TEST SUITE: None
```

This command starts 3 tracker pods, 3 origin pods, 3 build-index pods, 1 proxy pod, and an
agent daemonset.

## 5. Check the pods

Run this command:

```
$ kubectl get pods
```

Wait until every pod shows `Running` in the `STATUS` column:

```
NAME                                  READY   STATUS    RESTARTS   AGE
kraken-agent-xxxxx                    1/1     Running   2          74s
kraken-build-index-xxxxxxxxxx-xxxxx   1/1     Running   0          74s
kraken-build-index-xxxxxxxxxx-xxxxx   1/1     Running   1          74s
kraken-build-index-xxxxxxxxxx-xxxxx   1/1     Running   0          74s
kraken-origin-xxxxxxxxxx-xxxxx        1/1     Running   1          74s
kraken-origin-xxxxxxxxxx-xxxxx        1/1     Running   0          74s
kraken-origin-xxxxxxxxxx-xxxxx        1/1     Running   0          74s
kraken-proxy-xxxxxxxxxx-xxxxx         1/1     Running   1          74s
kraken-testfs-xxxxxxxxxx-xxxxx        1/1     Running   0          74s
kraken-tracker-xxxxxxxxxx-xxxxx       2/2     Running   0          74s
kraken-tracker-xxxxxxxxxx-xxxxx       2/2     Running   0          74s
kraken-tracker-xxxxxxxxxx-xxxxx       2/2     Running   0          74s
```

The agent and proxy pods may restart once or twice at startup. This is normal. They wait for
the tracker and origin pods to start first. See the Troubleshooting Guide if a pod does not
reach `Running` after 2 minutes.

## 6. Push and pull a test image

This step proves Kraken works end to end on your cluster.

Forward the proxy port and the agent port to your machine. Run these commands in two separate
terminals, or run each command with a trailing `&` to run it in the background:

```
$ kubectl port-forward svc/kraken-proxy 80:80
$ kubectl port-forward svc/kraken-agent 30081:80
```

Port forwarding avoids a common problem. The agent's default network rule only allows
connections from `127.0.0.1` and `172.17.0.1`. A direct connection to the `30081` NodePort from
your host machine may not match this rule and may fail with a `403` error. A port-forwarded
connection always matches the rule.

Install `crane`, a small tool for pushing and pulling container images. See the
[crane install guide](https://github.com/google/go-containerregistry/blob/main/cmd/crane/README.md).
`crane` needs no Docker daemon configuration change. If you prefer to use the `docker` command
instead, see the
[Docker insecure registry guide](https://docs.docker.com/registry/insecure/) to allow `docker
push` and `docker pull` against a local HTTP registry.

Save any local image as a tarball, then push it through the Kraken proxy:

```
$ docker save <your-image>:<tag> -o /tmp/test-image.tar
$ crane push --insecure /tmp/test-image.tar 127.0.0.1/test/hello:v1
```

Expected output ends with a line like this:

```
127.0.0.1/test/hello@sha256:<digest>
```

Pull the same image back through the Kraken agent:

```
$ crane pull --insecure 127.0.0.1:30081/test/hello:v1 /tmp/pulled-image.tar
```

If this command exits with no error, and `/tmp/pulled-image.tar` exists, Kraken stored the
image on an origin pod and served it back through the agent's P2P path.

## 7. Pull a real image from Docker Hub (optional)

Kraken can proxy `library/*` image names to Docker Hub. This needs extra configuration. The
default `helm install` command does not set this up.

To enable it, set the `extraBackends` value for the `origin` and `build_index` components in
`helm/values.yaml`. See the commented example in that file for the exact fields. Point
`registry_tag` and `registry_blob` at your own registry, or at a Docker Hub mirror you control.

After you set this value, redeploy with `helm upgrade`. A pod that names an image like
`127.0.0.1:30081/library/<image>` can then pull that image through Kraken. See
[demo.json](demo.json) for an example pod spec.

## Troubleshooting Guide

### A pod stays in `ImagePullBackOff`

The image name or tag is wrong, or the image is private.

- If you used step 3, Option A, check that `ghcr.io/uber` allows anonymous pulls. Run
  `docker pull ghcr.io/uber/kraken-agent:latest` on your machine. If this command fails, use
  step 3, Option B instead.
- If you used step 3, Option B, check that you loaded every image into your cluster. Run
  `kind load docker-image local/kraken-agent:dev` again for any missing image. Check that
  `--set kraken.imagePullPolicy=Never` is in your `helm install` command. Without this flag,
  Kubernetes tries to pull the image from a remote registry instead of using the loaded image.

### A pod restarts a few times, then reaches `Running`

This is normal. The agent and proxy components wait for the tracker and origin components to
start. Wait 1 to 2 minutes. If a pod still does not reach `Running`, run `kubectl logs
<pod-name>` to see the error.

### `docker pull 127.0.0.1:30081/...` returns a `403` error

You connected to the `30081` NodePort directly from your host machine. The agent's default
network rule only allows connections from `127.0.0.1` and `172.17.0.1`. Your cluster's network
may use a different address for this connection.

Use `kubectl port-forward svc/kraken-agent 30081:80` instead, as shown in step 6. A
port-forwarded connection always matches the network rule.

### `crane push` fails with a connection error on a port other than 80

Forward the proxy port to port `80` on your machine, exactly as shown in step 6:
`kubectl port-forward svc/kraken-proxy 80:80`. The proxy's upload response does not include a
port number. A client that connects on a different port retries on the default port, `80`, and
fails if nothing listens there.

### `Error: INSTALLATION FAILED: cannot re-use a name that is still in use`

A Helm release named `kraken-demo` already exists in this cluster. Run `helm list` to check.
Run `helm uninstall kraken-demo` to remove the old release, then run `helm install` again.

### `minikube image load` reports "the image was not found"

`minikube` could not reach your local Docker daemon. Check your shell for these environment
variables: `DOCKER_HOST`, `DOCKER_TLS_VERIFY`, `DOCKER_CERT_PATH`. If any of these point at a
different Docker daemon, unset them, then run `minikube image load` again:

```
$ unset DOCKER_HOST DOCKER_TLS_VERIFY DOCKER_CERT_PATH
```

### `MANIFEST_UNKNOWN` when you pull a `library/*` image

Kraken does not proxy Docker Hub images by default. See step 7 for the extra configuration this
feature needs.
