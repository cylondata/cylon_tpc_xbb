#!/bin/bash

WORKERS=(1 2 4 8)
QUERIES=(06 07 09 14 22 23)

CONDA_ENV="rapids"
GPU_BDB_HOME="$HOME/romeo/git/gpu-bdb"

# start conda env
# conda activate "$CONDA_ENV"

start_dask() {
  echo "starting dask. workers $W"

  DEVICES="0"
  for d in $(seq 1 $(($1 - 1))); do
    DEVICES+=",${d}"
  done
  echo "CUDA_VISIBLE_DEVICES=$DEVICES"

  CUDA_VISIBLE_DEVICES="$DEVICES" DASK_JIT_UNSPILL=True CLUSTER_MODE=NVLINK bash "$GPU_BDB_HOME"/gpu_bdb/cluster_configuration/cluster-startup.sh SCHEDULER

  sleep $(($1 * 2))
}

stop_dask() {
  echo "stopping dask"
  pkill -f dask
}

replace_bech_config_yaml() {
  sed "s/WWW/$1/g" cudf_benchmark_config.yaml.temp >"$GPU_BDB_HOME"/gpu_bdb/benchmark_runner/benchmark_config.yaml

  echo "------benchmark_config.yaml-----"
  cat "$GPU_BDB_HOME"/gpu_bdb/benchmark_runner/benchmark_config.yaml
  echo "-----------"
}

stop_dask

for W in ${WORKERS[0]}; do

  for Q in ${QUERIES[0]}; do
    echo "query $Q starting"

    replace_bech_config_yaml $W

    start_dask $W

    cd "$GPU_BDB_HOME"/gpu_bdb/queries/q"$Q" || exit
    python gpu_bdb_query_${Q}.py --config_file=../../benchmark_runner/benchmark_config.yaml

    stop_dask

    echo "query $Q done"
    echo "============="

    cd -
  done

done
