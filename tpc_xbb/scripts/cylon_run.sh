#!/bin/bash

WORKERS=(1 2 4 8)
QUERIES=(06 07 09 14 22 23)
CYLON_TPC_HOME="$HOME/romeo/git/cylon_tpc_xbb"

python_bin=$(which python)

#copy bencmark_config
CONFIG_PATH="$CYLON_TPC_HOME"/tpc_xbb/config/benchmark_config.yaml
# if [ ! -f "$CONFIG_PATH" ]; then
#   echo "copying config"
#   cp "$CYLON_TPC_HOME"/tpc_xbb/config/benchmark_config.yaml.template $CONFIG_PATH
# fi


replace_bech_config_yaml() {
  sed "s/WWW/$1/g" cylon_benchmark_config.yaml.temp >"$CONFIG_PATH"

  echo "------benchmark_config.yaml-----"
  cat "$CONFIG_PATH"
  echo "-----------"
}

for W in ${WORKERS[0]}; do
  replace_bech_config_yaml $W
  
  for Q in ${QUERIES[0]}; do
    echo "query $Q starting"

    mpirun -np "$W" "$python_bin" "$CYLON_TPC_HOME"/tpc_xbb/queries/q"$Q"/tpc_xbb_query_${Q}.py --config_file="$CONFIG_PATH"

    echo "query $Q done"
    echo "============="
  done
done
