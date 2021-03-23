#!/bin/bash

WORKERS=8
CONDA_ENV="rapids"
GPU_BDB_HOME="$HOME/romeo/git/gpu-bdb"

# start conda env
# conda activate "$CONDA_ENV"

# stard dask - change CUDA_VISIBLE_DEVICES according to the parallelism 
# CUDA_VISIBLE_DEVICES=0 DASK_JIT_UNSPILL=True CLUSTER_MODE=NVLINK bash cluster-startup.sh SCHEDULER

# stop dask 
# pkill -f dask 

start_dask(){
    echo "starting dask. workers $WORKERS"
    
    DEVICES="0"
    for d in $(seq 1 $(($1 - 1)) ); do 
        DEVICES+=",${d}"
    done
    echo "CUDA_VISIBLE_DEVICES=$DEVICES"
    
    CUDA_VISIBLE_DEVICES="$DEVICES" DASK_JIT_UNSPILL=True CLUSTER_MODE=NVLINK bash "$GPU_BDB_HOME"/gpu_bdb/cluster_configuration/cluster-startup.sh SCHEDULER
    
    sleep $(($1*2))
}

stop_dask(){
    echo "stopping dask"
    pkill -f dask 
}


stop_dask 


echo "load query starting"
start_dask $WORKERS

cd "$GPU_BDB_HOME"/gpu_bdb/queries/load_test || exit 
python gpu_bdb_load_test.py --config_file=../../benchmark_runner/benchmark_config.yaml

stop_dask

echo "query load done"
echo "============="
