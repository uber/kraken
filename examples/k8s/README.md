# Running Kraken in Kubernetes

This folder contains example k8s configuration files to set up kraken in your k8s cluster. This
is the most minimal setup to guide users and give a quick overview of what minimal sets of
configs are needed.

Kraken will come up with 1 origin, 1 tracker, 1 proxy and 1 build-index, but those should all
be > 1 in production. Each node will get a kraken agent and have `localhost:30081` as a pullable
registry location.

The deployment `my-pod.json` is an example of an application that got its image pulled from Kraken.

## Running the simple example

Simply run `make kubecluster` from the base of the repo.

## Expected output

`kubectl get pods` should output a healthy set of pods:
```
NAME                                  READY   STATUS    RESTARTS   AGE
kraken-agent-g589x                    1/1     Running   0          15h
kraken-build-index-6cc4556868-q7j27   1/1     Running   0          15h
kraken-origin-0-7d888656cd-5z8dl      1/1     Running   0          15h
kraken-proxy-6695ddfc9-t7n2l          1/1     Running   0          15h
kraken-testfs-79db8688dc-sgpjz        1/1     Running   0          15h
kraken-tracker-0-64c96bb4fd-brkk6     2/2     Running   0          15h
my-pod-5c4bc89745-vsfrh               1/1     Running   0          11m
```
