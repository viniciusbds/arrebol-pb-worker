#!/bin/bash

VCPU=$1
RAM=$2
WORKER_ID=$3

echo "{\"vcpu\":${VCPU}, \"ram\":${RAM}, \"id\":\"${WORKER_ID}\"}" > ./worker-conf.json
