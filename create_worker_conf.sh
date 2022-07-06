#!/bin/bash

VCPU=$1
RAM=$2
WORKER_ID=$3
QUEUE_ID=$4

echo "{\"vcpu\":${VCPU}, \"ram\":${RAM}, \"id\":\"${WORKER_ID}\", \"queue_id\":${QUEUE_ID}}" > ./worker-conf.json
