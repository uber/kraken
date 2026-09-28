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

### Option B: minikube

Install `minikube` from the [minikube start guide](https://minikube.sigs.k8s.io/docs/start/).

Create a cluster:

```
$ minikube start --driver=docker
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

This command builds all 7 component images. Load them into your cluster.

For `kind`:

```
$ kind load docker-image gcr.io/uber-container-tools/kraken-agent:<tag>
$ kind load docker-image gcr.io/uber-container-tools/kraken-build-index:<tag>
$ kind load docker-image gcr.io/uber-container-tools/kraken-origin:<tag>
$ kind load docker-image gcr.io/uber-container-tools/kraken-proxy:<tag>
$ kind load docker-image gcr.io/uber-container-tools/kraken-testfs:<tag>
$ kind load docker-image gcr.io/uber-container-tools/kraken-tracker:<tag>
$ kind load docker-image gcr.io/uber-container-tools/kraken-herd:<tag>
```

For `minikube`:

```
$ minikube image load gcr.io/uber-container-tools/kraken-agent:<tag>
$ minikube image load gcr.io/uber-container-tools/kraken-build-index:<tag>
$ minikube image load gcr.io/uber-container-tools/kraken-origin:<tag>
$ minikube image load gcr.io/uber-container-tools/kraken-proxy:<tag>
$ minikube image load gcr.io/uber-container-tools/kraken-testfs:<tag>
$ minikube image load gcr.io/uber-container-tools/kraken-tracker:<tag>
$ minikube image load gcr.io/uber-container-tools/kraken-herd:<tag>
```

Find `<tag>` with this command:

```
$ git describe --always --tags
```

## 4. Install Kraken with Helm

### If you use the published images (step 3, Option A)

```
$ helm install kraken-demo ./helm
```

### If you built the images yourself (step 3, Option B)

```
$ helm install kraken-demo ./helm \
    --set kraken.repository=gcr.io/uber-container-tools \
    --set kraken.tag=<tag> \
    --set kraken.imagePullPolicy=Never
```

Use the same `<tag>` value from step 3.

This command starts 3 tracker pods, 3 origin pods, 3 build-index pods, 1 proxy pod, and an
agent daemonset.

## 5. Check the pods

Run this command:

```
$ kubectl get pods
```

Wait until every pod shows `Running` in the `STATUS` column. The agent and proxy pods may
restart once or twice at startup. This is normal. They wait for the tracker and origin pods to
start first.

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

Save any local image as a tarball, then push it through the Kraken proxy:

```
$ docker save <your-image>:<tag> -o /tmp/test-image.tar
$ crane push --insecure /tmp/test-image.tar 127.0.0.1/test/hello:v1
```

Pull the same image back through the Kraken agent:

```
$ crane pull --insecure 127.0.0.1:30081/test/hello:v1 /tmp/pulled-image.tar
```

If this command succeeds, Kraken stored the image on an origin pod and served it back through
the agent's P2P path.

## 7. Pull a real image from Docker Hub (optional)

Kraken can proxy `library/*` image names to Docker Hub. This needs extra configuration. The
default `helm install` command does not set this up.

To enable it, set the `extraBackends` value for the `origin` and `build_index` components in
`helm/values.yaml`. See the commented example in that file for the exact fields. Point
`registry_tag` and `registry_blob` at your own registry, or at a Docker Hub mirror you control.

After you set this value, redeploy with `helm upgrade`. A pod that names an image like
`127.0.0.1:30081/library/<image>` can then pull that image through Kraken. See
[demo.json](demo.json) for an example pod spec.
