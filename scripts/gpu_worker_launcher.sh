#!/bin/bash

# This will be called by each Parsl worker

# Choose GPU based on slot/index in the host
WORKER_ID=$1
export CUDA_VISIBLE_DEVICES=$WORKER_ID

# Print for debugging
echo "Launching worker $WORKER_ID on GPU $CUDA_VISIBLE_DEVICES"

# Shift args and exec actual worker
shift
exec "$@"
