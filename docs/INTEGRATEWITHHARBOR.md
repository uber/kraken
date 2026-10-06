## Concept
Harbor is the only container repository project in CNCF. And it supports image management, replication, CVE scan and so on. It could be used in production.

Kraken is also a container repository with P2P distribution. It supports S3, HDFS, docker registry as backend storage.

I will introduce how to integrate Harbor and Kraken here. And using the image management function of Harbor and the P2P distribution function of Kraken both.

## Architecture
![image](https://gitlab.com/pmm123/pics/raw/master/work/p2p/%E9%95%9C%E5%83%8F%E4%BB%93%E5%BA%93P2P_3_.png)

So you can see that Harbor and Kraken share the same docker registry as backend storage. It will decouple Harbor and Kraken if they just share the storage but not higher level sharing. So we can push images into Harbor and pull them from Harbor or kraken.

## Deployment
Here we use [helm chart](https://github.com/goharbor/harbor-helm) from Harbor community to deploy them. and also [helm chart](https://github.com/uber/kraken/tree/master/helm) from Kraken community to deploy them.

We should modify two config file additionally,
1. modify the backend of kraken-origin and kraken-buildindex and make it refer to harbor-registry.
2. modify config file of harbor-registry. add another notification hook for kraken.

As following,

harbor-registry configmap
```yaml
## add the second endpoint in notifications
    notifications:
      endpoints:
        - name: harbor
          disabled: false
          url: http://harbor-core.repo.svc.cluster.local/service/notifications
          timeout: 3000ms
          threshold: 5
          backoff: 1s
        - name: kraken
          disabled: false
          url: http://kraken-proxy.p2p.svc.cluster.local:10050/registry/notifications
          timeout: 3000ms
          threshold: 5
          backoff: 1s
```

kraken-origin,kraken-buildindex,

```yaml
    backends:
      - namespace: .*
        backend:
          registry_blob:
            address: harbor-registry.repo.svc.cluster.local:5000
            security:
              basic:
                username: "admin"
                password: "XXXXX"
```

You should add more configuration about TLS if the domain name of your Harbor use a self-signed SSL certificate. It will throws a X509 error if not.

```yaml
    backends:
      - namespace: .*
        backend:
          registry_blob:
            address: harbor-registry.repo.svc.cluster.local:5000
            security:
              basic:
                username: "admin"
                password: "XXXXX"
              tls:
                client:
                  cert:
                    path: /etc/certs/XXX.crt
                  key:
                    path: /etc/certs/XXX.key
                cas:
                  - path: /etc/certs/ca.crt
                  
```

You can pull images using a `localhost` domain by P2P distribution after you deploy kraken-agent in your k8s nodes using daemonSet.

For example, there is an image called  `hub.harbor.com/library/debian:latest`, then you can pull it using `localhost:13000/library/debian:latest`.

## Work flow
I describe the work flow of pushing and pulling images briefly here,
1. User push an image named `docker push hub.harbor.com/library/debian:latest` to Harbor.
2. Harbor-registry will trigger a notification of pushManifest event to kraken-proxy.
3. Kraken-proxy will fetch the manifest and notify kraken-origin to cache related blobs after receiving the pushManifest event.
4. User will try to pull image by `docker pull localhost:13000/library/debian:latest`
5. Then the P2P distribution work flow. but kraken-origin has cached these blobs before P2P distribution starts. it will save the time of fetching blobs from harbor-registry.
6. Pulling image is completed.

## Use Case
It has been a long time that this solution is used in Qingzhou Platform in NetEase Cloud.

Now we use Harbor for managing images and Kraken for distributing images, and it resolves the problem of distribution of huge number of images. there are thousands of images distributed everyday in our Qingzhou Platform.

## Notes
It is important to limit the resource quota of kraken-agent in k8s for it is deployed with containers of business. It will affect the containers of business if kraken-agent takes too much resource.

There are two aspects for resource quota. One is the resource limit of k8s pod, the other one is the bandwidth.
