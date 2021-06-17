#!/bin/bash

for i in {1..5}; do 
  echo "Running $i"
  gscripts/run.sh

  echo "Finished Running. Sleeping 15 sec ..."
  sleep 15
done

