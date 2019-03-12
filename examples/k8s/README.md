# Running Kraken in Kubernetes

This folder contains example k8s configuration files to set up kraken in your k8s cluster. This
is the most minimal setup to guide users and give a quick overview of what minimal sets of
configs are needed.

Kraken will come up with 1 origin, 1 tracker, 1 proxy and 1 build-index, but those should all
be > 1 in production. Each node will get a kraken agent and have `localhost:30081` as a pullable
registry location.

The deployment `my-pod.json` is an example of an application that got its image pulled from Kraken.
