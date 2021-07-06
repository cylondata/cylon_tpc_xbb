#!/bin/bash

nworkers=8
datasize=1GB

# mpirun -n $nworkers --hostfile hostfile \
#	--mca btl_openib_want_cuda_gdr 1 \
#	--mca btl_smcuda_cuda_ipc_verbose 100 \
#	-x UCX_TLS=rc,sm,cuda_copy,cuda_ipc \

mpirun -n $nworkers \
	--mca btl_openib_allow_ib true \
	--mca pml ucx \
	python queries/join/gc_shuffle.py \
	$datasize
sleep 1

python gscripts/compute_result.py $nworkers
