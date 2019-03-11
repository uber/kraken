#! /bin/bash

kubectl create configmap kraken --from-file=configs

for file in *.json; do
    kubectl apply -f $file
done
